package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.FotaBridgeInbox;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.kafka.FotaDlqPublisher;
import net.hwyz.iov.cloud.edd.vagw.kafka.FotaKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

/**
 * FOTA 桥接应用服务，对齐 EDD-VAGW-DSN-CR-005 §5/§6/§8。
 * <p>
 * 只编排接入桥接（MQTT↔Kafka），不依赖 OTA 领域模型、不持有 OTA 领域状态、不承担升级编排。
 * VAGW 只产生技术投递结果，不因 Kafka produce / MQTT publish 成功产生 OTA 业务回执。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FotaBridgeAppService {

    private static final int UPLINK_PRODUCE_MAX_RETRIES = 3;

    private final BindingService bindingService;
    private final SessionService sessionService;
    private final MqttClientManager mqttClientManager;
    private final FotaEnvelopeValidator envelopeValidator;
    private final FotaDeliveryResultService deliveryResultService;
    private final FotaKafkaProducer fotaKafkaProducer;
    private final FotaDlqPublisher dlqPublisher;
    private final FotaBridgeInbox fotaBridgeInbox;

    // ------------------------------------------------------------------
    // 上行：MQTT → Kafka iov.vagw.up.fota（Key=VIN）
    // ------------------------------------------------------------------

    /**
     * 处理 FOTA 上行：校验身份/Envelope → 补全 VIN → 幂等去重 → produce（Key=VIN）。
     * 生产失败有限重试，超限写上行 DLQ 并告警。
     */
    public FotaUplinkResult processFotaUplink(EnvelopeProto.Envelope envelope, String connectionDeviceSn) {
        FotaEnvelopeValidator.FotaValidationFailure vf =
                envelopeValidator.validateUplink(envelope, connectionDeviceSn);
        if (vf != null) {
            ErrorCode code = vf.reason() == DeliveryReason.DEVICE_MISMATCH
                    ? ErrorCode.IDENTITY_MISMATCH : ErrorCode.INVALID_ENVELOPE;
            log.warn("FOTA uplink rejected: {}: {}", vf.reason(), vf.message());
            return FotaUplinkResult.fail(code, "FOTA uplink invalid: " + vf.reason());
        }

        // VIN 富化（北向）；未绑定则拒绝并审计
        Optional<String> vinOpt = bindingService.resolveVin(envelope.getDeviceSn());
        if (vinOpt.isEmpty()) {
            log.warn("FOTA uplink rejected: VIN unbound, deviceSn={}", LogMask.mask(envelope.getDeviceSn()));
            return FotaUplinkResult.fail(ErrorCode.VIN_UNAUTHORIZED, "VIN not bound");
        }
        String vin = vinOpt.get();

        // 上行物理重复去重（按 messageId）
        String digest = FotaBridgeInbox.digestOf(envelope.getPayload().toByteArray());
        Optional<FotaBridgeInbox.InboxRecord> existing =
                fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.UPLINK, envelope.getMsgId());
        if (existing.isPresent()) {
            log.info("FOTA uplink duplicate skipped: messageId={}, digestMatch={}",
                    envelope.getMsgId(), digest.equals(existing.get().getPayloadDigest()));
            return FotaUplinkResult.success();
        }

        // produce iov.vagw.up.fota, key=VIN（有限重试）
        FotaKafkaProducer.SendResult result = produceWithRetry(envelope, vin);
        if (result == null) {
            // 超限：先记录 DLQED（防止重投重复生产），再尽力写 DLQ
            fotaBridgeInbox.recordFinal(FotaBridgeInbox.InboxRecord.builder()
                    .direction(FotaBridgeInbox.Direction.UPLINK.name())
                    .messageId(envelope.getMsgId())
                    .payloadDigest(digest)
                    .kafkaTopic(FotaRouteConfig.KAFKA_UP_TOPIC)
                    .status(FotaBridgeInbox.Status.DLQED.name())
                    .reason("produce_retries_exceeded")
                    .processedAt(Instant.now())
                    .build());
            try {
                dlqPublisher.publish(FotaRouteConfig.UP_DLQ_TOPIC, vin, envelope.toByteArray(),
                        "produce_retries_exceeded");
            } catch (Exception e) {
                log.error("FOTA uplink DLQ publish failed: vin={}, messageId={}", LogMask.mask(vin), envelope.getMsgId(), e);
            }
            return FotaUplinkResult.fail(ErrorCode.ROUTE_UNAVAILABLE, "Kafka produce failed after retries");
        }

        fotaBridgeInbox.recordFinal(FotaBridgeInbox.InboxRecord.builder()
                .direction(FotaBridgeInbox.Direction.UPLINK.name())
                .messageId(envelope.getMsgId())
                .payloadDigest(digest)
                .kafkaTopic(FotaRouteConfig.KAFKA_UP_TOPIC)
                .partition(result.partition())
                .offset(result.offset())
                .status(FotaBridgeInbox.Status.ACCEPTED.name())
                .processedAt(Instant.now())
                .build());

        log.info("FOTA uplink bridged: deviceSn={}, vin={}, messageId={}",
                LogMask.mask(envelope.getDeviceSn()), LogMask.mask(vin), envelope.getMsgId());
        return FotaUplinkResult.success();
    }

    private FotaKafkaProducer.SendResult produceWithRetry(EnvelopeProto.Envelope envelope, String vin) {
        Exception last = null;
        for (int i = 0; i < UPLINK_PRODUCE_MAX_RETRIES; i++) {
            try {
                return fotaKafkaProducer.sendFotaUplink(
                        FotaRouteConfig.KAFKA_UP_TOPIC, vin, envelope,
                        Instant.now().toString(), null);
            } catch (Exception e) {
                last = e;
                log.warn("FOTA uplink Kafka produce attempt {}/{} failed: messageId={}, err={}",
                        i + 1, UPLINK_PRODUCE_MAX_RETRIES, envelope.getMsgId(), e.getMessage());
            }
        }
        log.error("FOTA uplink Kafka produce exhausted retries: messageId={}", envelope.getMsgId(), last);
        return null;
    }

    // ------------------------------------------------------------------
    // 下行：Kafka iov.vagw.down.fota → MQTT vehicle/{device_sn}/down/fota
    // ------------------------------------------------------------------

    /**
     * 处理 FOTA 下行 Kafka 消息。
     *
     * @return true 表示已可靠处理（可提交 offset）；false 表示未可靠处理（不提交，Kafka 重投）
     */
    public boolean processFotaDownlink(ConsumerRecord<String, byte[]> record) {
        String kafkaKeyVin = record.key();
        byte[] value = record.value();

        // 解析 Envelope；不可恢复契约错误 → 下行 DLQ
        EnvelopeProto.Envelope envelope;
        try {
            envelope = EnvelopeProto.Envelope.parseFrom(value);
        } catch (Exception e) {
            log.warn("FOTA downlink contract invalid (parse): partition={}, offset={}, err={}",
                    record.partition(), record.offset(), e.getMessage());
            return moveToDlq(record, kafkaKeyVin, value, "contract_invalid: envelope parse failed");
        }

        FotaEnvelopeValidator.FotaValidationFailure vf =
                envelopeValidator.validateDownlink(envelope, kafkaKeyVin);
        if (vf != null) {
            if (vf.reason() == DeliveryReason.CONTRACT_INVALID) {
                log.warn("FOTA downlink contract invalid: {}", vf.message());
                return moveToDlq(record, kafkaKeyVin, value, "contract_invalid: " + vf.message());
            }
            // 可表达为技术投递结果的拒绝（如过期、VIN 不一致）
            return handleNonDeliverable(record, envelope, kafkaKeyVin,
                    envelope.getDeviceSn(), vf.reason());
        }

        // 下行幂等：同 messageId + payloadDigest 只发布一次 MQTT
        String digest = FotaBridgeInbox.digestOf(envelope.getPayload().toByteArray());
        Optional<FotaBridgeInbox.InboxRecord> existing =
                fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, envelope.getMsgId());
        if (existing.isPresent()) {
            if (digest.equals(existing.get().getPayloadDigest())) {
                log.info("FOTA downlink duplicate skipped: messageId={}, status={}",
                        envelope.getMsgId(), existing.get().getStatus());
                return true;
            }
            // 同 messageId 不同摘要 → 冲突隔离（不重复形成不同技术结果）
            log.error("FOTA downlink digest conflict: messageId={}", envelope.getMsgId());
            return moveToDlq(record, kafkaKeyVin, value, "digest_conflict");
        }

        // VIN → device_sn + 会话/权限/TTL 校验
        Optional<String> deviceSnOpt = bindingService.resolveDeviceSn(kafkaKeyVin);
        if (deviceSnOpt.isEmpty()) {
            return handleNonDeliverable(record, envelope, kafkaKeyVin, null, DeliveryReason.VIN_UNBOUND);
        }
        String deviceSn = deviceSnOpt.get();

        // deviceId 一致性：Envelope.device_sn（若提供）须与解析出的设备绑定一致
        if (envelope.getDeviceSn() != null && !envelope.getDeviceSn().isBlank()
                && !envelope.getDeviceSn().equalsIgnoreCase(deviceSn)) {
            return handleNonDeliverable(record, envelope, kafkaKeyVin, deviceSn, DeliveryReason.DEVICE_MISMATCH);
        }

        if (!sessionService.isOnlineByDeviceSn(deviceSn)) {
            return handleNonDeliverable(record, envelope, kafkaKeyVin, deviceSn, DeliveryReason.VEHICLE_OFFLINE);
        }

        // 构造下行 Envelope（补全 device_sn；不改写 messageId/关联链/摘要/payload）
        EnvelopeProto.Envelope outbound = envelope.toBuilder().setDeviceSn(deviceSn).build();
        String mqttTopic = "vehicle/" + deviceSn + "/down/fota";

        try {
            mqttClientManager.publish(mqttTopic, outbound.toByteArray(), FotaRouteConfig.QOS);
        } catch (Exception e) {
            log.error("FOTA downlink MQTT publish failed: vin={}, deviceSn={}, messageId={}",
                    LogMask.mask(kafkaKeyVin), LogMask.mask(deviceSn), envelope.getMsgId(), e);
            return handleNonDeliverable(record, envelope, kafkaKeyVin, deviceSn, DeliveryReason.MQTT_PUBLISH_FAILED);
        }

        // 记录技术 ACCEPTED；MQTT publish/PUBACK 只形成技术投递状态，不生成业务回执
        recordInbox(envelope, record, FotaBridgeInbox.Status.ACCEPTED, mqttTopic, null, null);
        log.info("FOTA downlink delivered: vin={}, deviceSn={}, messageId={}, topic={}",
                LogMask.mask(kafkaKeyVin), LogMask.mask(deviceSn), envelope.getMsgId(),
                LogMask.maskIn(mqttTopic, deviceSn));
        return true;
    }

    /**
     * 不可投递：向 iov.vagw.up.fota 生产 delivery-rejected 并记录 REJECTED。
     * 生产失败则不提交 offset，交由 Kafka 重投（幂等记录避免重复 MQTT 发布）。
     */
    private boolean handleNonDeliverable(ConsumerRecord<String, byte[]> record,
                                         EnvelopeProto.Envelope envelope, String vin,
                                         String deviceSn, DeliveryReason reason) {
        try {
            String resultMessageId =
                    deliveryResultService.produceDeliveryRejected(envelope, vin, deviceSn, reason);
            recordInbox(envelope, record, FotaBridgeInbox.Status.REJECTED, null, reason.name(), resultMessageId);
            log.info("FOTA downlink rejected: vin={}, deviceSn={}, messageId={}, reason={}",
                    LogMask.mask(vin), LogMask.mask(deviceSn), envelope.getMsgId(), reason);
            return true;
        } catch (Exception e) {
            log.error("FOTA downlink delivery-rejected produce failed: vin={}, reason={}, messageId={}",
                    LogMask.mask(vin), reason, envelope.getMsgId(), e);
            return false;
        }
    }

    private boolean moveToDlq(ConsumerRecord<String, byte[]> record, String key, byte[] value, String reason) {
        try {
            dlqPublisher.publish(FotaRouteConfig.DOWN_DLQ_TOPIC, key, value, reason);
            return true;
        } catch (Exception e) {
            log.error("FOTA downlink DLQ publish failed: key={}, reason={}", LogMask.mask(key), reason, e);
            return false;
        }
    }

    private void recordInbox(EnvelopeProto.Envelope envelope, ConsumerRecord<String, byte[]> record,
                             FotaBridgeInbox.Status status, String mqttTopic,
                             String reason, String resultMessageId) {
        fotaBridgeInbox.recordFinal(FotaBridgeInbox.InboxRecord.builder()
                .direction(FotaBridgeInbox.Direction.DOWNLINK.name())
                .messageId(envelope.getMsgId())
                .payloadDigest(FotaBridgeInbox.digestOf(envelope.getPayload().toByteArray()))
                .kafkaTopic(record.topic())
                .partition(record.partition())
                .offset(record.offset())
                .mqttTopic(mqttTopic)
                .status(status.name())
                .reason(reason)
                .resultMessageId(resultMessageId)
                .processedAt(Instant.now())
                .build());
    }

    /**
     * 上行处理结果（避免与 UplinkService.ProcessResult 形成类型环）。
     */
    public record FotaUplinkResult(boolean ok, ErrorCode errorCode, String reason) {
        public static FotaUplinkResult success() {
            return new FotaUplinkResult(true, null, null);
        }

        public static FotaUplinkResult fail(ErrorCode code, String reason) {
            return new FotaUplinkResult(false, code, reason);
        }
    }
}
