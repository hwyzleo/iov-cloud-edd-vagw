package net.hwyz.iov.cloud.edd.vagw.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.VehicleMessageDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.idempotency.VehicleBridgeInbox;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.EnvelopeDlqPublisher;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.EnvelopeKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import vehicle.common.v1.Envelope;

import java.time.Instant;
import java.util.Optional;

/**
 * FOTA 透明桥接编排服务（EDD-VAGW-DSN-CR-006 §6/§7/§9）。
 * <p>
 * 上行 MQTT→Kafka、下行 Kafka→MQTT 均原样转发同一 Envelope bytes；只解析公共 Envelope 元数据，
 * 不解析 payload、不重建 Envelope、不补写 VIN、不生成新业务身份。Kafka offset 仅在 Inbox 与技术
 * 结果可靠收敛后提交；PUBACK 不构造 FOTA RESPONSE。VAGW 只产生技术投递结果。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class VehicleMessageBridgeService {

    private final AccessIdentityValidator accessIdentityValidator;
    private final VehicleEnvelopeValidator envelopeValidator;
    private final BindingService bindingService;
    private final SessionService sessionService;
    private final VehicleBridgeInbox bridgeInbox;
    private final EnvelopeKafkaProducer kafkaProducer;
    private final EnvelopeDlqPublisher dlqPublisher;
    private final VehicleMessageDownlinkPublisher downlinkPublisher;
    private final GatewayDeliveryService gatewayDeliveryService;

    @Value("${vagw.fota.uplink-produce-retries:3}")
    private int uplinkProduceRetries;
    @Value("${vagw.fota.uplink-produce-retry-delay-ms:500}")
    private long uplinkProduceRetryDelayMs;

    // ------------------------------------------------------------------
    // 上行：MQTT → Kafka iov.vagw.up.fota（Key=VIN，value=原 Envelope bytes）
    // ------------------------------------------------------------------

    /**
     * 处理 FOTA 上行。
     *
     * @param envelopeBytes  原序列化 Envelope bytes（value 事实来源）
     * @param topicDeviceKey MQTT Topic device key（EMQX ACL 已约束为接入身份）
     * @return 处理结果
     */
    public UplinkResult processUplink(byte[] envelopeBytes, String topicDeviceKey) {
        // 1. 解码；不可恢复契约错误 → 上行 DLQ
        Envelope.VehicleMessageEnvelope envelope;
        try {
            envelope = Envelope.VehicleMessageEnvelope.parseFrom(envelopeBytes);
        } catch (Exception e) {
            log.warn("FOTA uplink contract invalid (parse): deviceKey={}, err={}",
                    LogMask.mask(topicDeviceKey), e.getMessage());
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, topicDeviceKey, envelopeBytes,
                    "contract_invalid: envelope parse failed");
            return UplinkResult.fail(ErrorCode.INVALID_ENVELOPE, "Envelope parse failed");
        }

        // 2. 公共元数据校验（service/type/kind/TTL/size/PayloadType allowlist）
        VehicleEnvelopeValidator.Failure vf = envelopeValidator.validateUplink(envelope, envelopeBytes);
        if (vf != null) {
            log.warn("FOTA uplink rejected: reason={}: {}", vf.reason(), vf.message());
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, topicDeviceKey, envelopeBytes,
                    "contract_invalid: " + vf.reason() + ": " + vf.message());
            recordUplinkFinal(envelope, envelopeBytes, VehicleBridgeInbox.State.DLQED,
                    vf.reason().name(), null, null);
            return UplinkResult.fail(ErrorCode.INVALID_ENVELOPE, "FOTA uplink invalid: " + vf.reason());
        }

        // 3. 接入身份：Topic device key == Envelope.device_id
        AccessIdentityValidator.Failure ai = accessIdentityValidator.validate(topicDeviceKey, envelope.getDeviceId());
        if (ai != null) {
            log.warn("FOTA uplink identity mismatch: reason={}: {}", ai.reason(), ai.message());
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, topicDeviceKey, envelopeBytes,
                    "identity_mismatch: " + ai.message());
            recordUplinkFinal(envelope, envelopeBytes, VehicleBridgeInbox.State.DLQED,
                    ai.reason().name(), null, null);
            return UplinkResult.fail(ErrorCode.IDENTITY_MISMATCH, "identity mismatch");
        }

        // 4. VIN 绑定：不得为富化改写 Envelope；Kafka Key 使用绑定 VIN
        Optional<String> vinOpt = bindingService.resolveVin(envelope.getDeviceId());
        if (vinOpt.isEmpty()) {
            log.warn("FOTA uplink rejected: VIN unbound, deviceId={}", LogMask.mask(envelope.getDeviceId()));
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, topicDeviceKey, envelopeBytes,
                    "vin_unbound");
            recordUplinkFinal(envelope, envelopeBytes, VehicleBridgeInbox.State.DLQED,
                    "VIN_UNBOUND", null, null);
            return UplinkResult.fail(ErrorCode.VIN_UNAUTHORIZED, "VIN not bound");
        }
        String vin = vinOpt.get();
        if (!envelope.getVin().isBlank() && !envelope.getVin().equalsIgnoreCase(vin)) {
            log.warn("FOTA uplink rejected: envelope vin mismatch binding, deviceId={}",
                    LogMask.mask(envelope.getDeviceId()));
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, topicDeviceKey, envelopeBytes,
                    "vin_mismatch_binding");
            recordUplinkFinal(envelope, envelopeBytes, VehicleBridgeInbox.State.DLQED,
                    "DEVICE_MISMATCH", null, null);
            return UplinkResult.fail(ErrorCode.IDENTITY_MISMATCH, "envelope vin mismatch binding");
        }

        // 5. 幂等：message_id + envelope_sha256 复用/冲突
        String digest = VehicleBridgeInbox.digestOf(envelopeBytes);
        Optional<VehicleBridgeInbox.InboxRecord> existing =
                bridgeInbox.findFinal(VehicleBridgeInbox.Direction.UPLINK, envelope.getMessageId());
        if (existing.isPresent()) {
            if (digest.equals(existing.get().getEnvelopeSha256())) {
                log.info("FOTA uplink duplicate skipped: messageId={}", envelope.getMessageId());
                return UplinkResult.success();
            }
            log.error("FOTA uplink digest conflict: messageId={}", envelope.getMessageId());
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, vin, envelopeBytes, "digest_conflict");
            return UplinkResult.fail(ErrorCode.INVALID_ENVELOPE, "digest conflict");
        }

        // 6. produce iov.vagw.up.fota, key=VIN（有限重试，原 bytes）
        EnvelopeKafkaProducer.SendResult result = produceWithRetry(envelopeBytes, envelope, vin);
        if (result == null) {
            bridgeInbox.recordFinal(VehicleBridgeInbox.InboxRecord.builder()
                    .direction(VehicleBridgeInbox.Direction.UPLINK.name())
                    .messageId(envelope.getMessageId())
                    .envelopeSha256(digest)
                    .kafkaTopic(VehicleRouteCatalog.KAFKA_UP_TOPIC)
                    .state(VehicleBridgeInbox.State.DLQED.name())
                    .outcome("OUTCOME_UNKNOWN")
                    .reason("produce_retries_exceeded")
                    .updatedAt(Instant.now())
                    .build());
            moveToDlqBestEffort(VehicleRouteCatalog.UP_DLQ_TOPIC, vin, envelopeBytes, "produce_retries_exceeded");
            return UplinkResult.fail(ErrorCode.ROUTE_UNAVAILABLE, "Kafka produce failed after retries");
        }

        bridgeInbox.recordFinal(VehicleBridgeInbox.InboxRecord.builder()
                .direction(VehicleBridgeInbox.Direction.UPLINK.name())
                .messageId(envelope.getMessageId())
                .envelopeSha256(digest)
                .kafkaTopic(VehicleRouteCatalog.KAFKA_UP_TOPIC)
                .partition(result.partition())
                .offset(result.offset())
                .state(VehicleBridgeInbox.State.ACCEPTED.name())
                .outcome("OUTCOME_ACCEPTED")
                .updatedAt(Instant.now())
                .build());

        log.info("FOTA uplink bridged: deviceId={}, vin={}, messageId={}, payloadType={}",
                LogMask.mask(envelope.getDeviceId()), LogMask.mask(vin), envelope.getMessageId(),
                LogMask.maskPayloadType(envelope.getPayloadType()));
        return UplinkResult.success();
    }

    private EnvelopeKafkaProducer.SendResult produceWithRetry(byte[] envelopeBytes,
                                                              Envelope.VehicleMessageEnvelope envelope,
                                                              String vin) {
        Exception last = null;
        for (int i = 0; i < uplinkProduceRetries; i++) {
            try {
                return kafkaProducer.sendUplink(VehicleRouteCatalog.KAFKA_UP_TOPIC, vin,
                        envelopeBytes, envelope, Instant.now().toString());
            } catch (Exception e) {
                last = e;
                log.warn("FOTA uplink Kafka produce attempt {}/{} failed: messageId={}, err={}",
                        i + 1, uplinkProduceRetries, envelope.getMessageId(), e.getMessage());
                if (i < uplinkProduceRetries - 1 && uplinkProduceRetryDelayMs > 0) {
                    try {
                        Thread.sleep(uplinkProduceRetryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            }
        }
        log.error("FOTA uplink Kafka produce exhausted retries: messageId={}", envelope.getMessageId(), last);
        return null;
    }

    private void recordUplinkFinal(Envelope.VehicleMessageEnvelope envelope, byte[] envelopeBytes,
                                   VehicleBridgeInbox.State state, String reason,
                                   Integer partition, Long offset) {
        bridgeInbox.recordFinal(VehicleBridgeInbox.InboxRecord.builder()
                .direction(VehicleBridgeInbox.Direction.UPLINK.name())
                .messageId(envelope.getMessageId())
                .envelopeSha256(VehicleBridgeInbox.digestOf(envelopeBytes))
                .kafkaTopic(VehicleRouteCatalog.KAFKA_UP_TOPIC)
                .partition(partition)
                .offset(offset)
                .state(state.name())
                .outcome(state == VehicleBridgeInbox.State.DLQED ? "OUTCOME_UNKNOWN" : "OUTCOME_REJECTED")
                .reason(reason)
                .updatedAt(Instant.now())
                .build());
    }

    // ------------------------------------------------------------------
    // 下行：Kafka iov.vagw.down.fota → MQTT vehicle/{device-key}/down/fota（原 bytes）
    // ------------------------------------------------------------------

    /**
     * 处理 FOTA 下行 Kafka 消息。
     *
     * @return true 表示已可靠处理（可提交 offset）；false 表示未可靠处理（不提交，Kafka 重投）
     */
    public boolean processDownlink(ConsumerRecord<String, byte[]> record) {
        String kafkaKeyVin = record.key();
        byte[] value = record.value();

        // 1. 解码；契约非法 → 下行 DLQ
        Envelope.VehicleMessageEnvelope envelope;
        try {
            envelope = Envelope.VehicleMessageEnvelope.parseFrom(value);
        } catch (Exception e) {
            log.warn("FOTA downlink contract invalid (parse): partition={}, offset={}, err={}",
                    record.partition(), record.offset(), e.getMessage());
            return moveToDlq(record, kafkaKeyVin, value, "contract_invalid: envelope parse failed");
        }

        // 2. 公共校验 + Kafka Key(VIN) 一致性
        VehicleEnvelopeValidator.Failure vf =
                envelopeValidator.validateDownlink(envelope, value, kafkaKeyVin);
        if (vf != null) {
            switch (vf.reason()) {
                case CONTRACT_INVALID, PAYLOAD_TYPE_DENIED, SIZE_EXCEEDED -> {
                    log.warn("FOTA downlink contract invalid: {}", vf.message());
                    return moveToDlq(record, kafkaKeyVin, value, "contract_invalid: " + vf.message());
                }
                case DEVICE_MISMATCH, MESSAGE_EXPIRED -> {
                    DeliveryReason dr = vf.reason() == VehicleEnvelopeValidator.Reason.MESSAGE_EXPIRED
                            ? DeliveryReason.MESSAGE_EXPIRED : DeliveryReason.DEVICE_MISMATCH;
                    return handleNonDeliverable(record, envelope, kafkaKeyVin, dr);
                }
            }
        }

        // 3. 下行幂等：message_id + envelope_sha256
        String digest = VehicleBridgeInbox.digestOf(value);
        Optional<VehicleBridgeInbox.InboxRecord> existing =
                bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, envelope.getMessageId());
        if (existing.isPresent()) {
            if (digest.equals(existing.get().getEnvelopeSha256())) {
                log.info("FOTA downlink duplicate skipped: messageId={}, state={}",
                        envelope.getMessageId(), existing.get().getState());
                return true;
            }
            log.error("FOTA downlink digest conflict: messageId={}", envelope.getMessageId());
            return moveToDlq(record, kafkaKeyVin, value, "digest_conflict");
        }

        // 4. VIN → device_id 绑定 + Envelope.device_id 一致性 + 在线会话
        Optional<String> deviceSnOpt = bindingService.resolveDeviceSn(kafkaKeyVin);
        if (deviceSnOpt.isEmpty()) {
            return handleNonDeliverable(record, envelope, kafkaKeyVin, DeliveryReason.VIN_UNBOUND);
        }
        String deviceId = deviceSnOpt.get();
        if (!envelope.getDeviceId().equalsIgnoreCase(deviceId)) {
            return handleNonDeliverable(record, envelope, kafkaKeyVin, DeliveryReason.DEVICE_MISMATCH);
        }
        if (!sessionService.isOnlineByDeviceSn(deviceId)) {
            return handleNonDeliverable(record, envelope, kafkaKeyVin, DeliveryReason.VEHICLE_OFFLINE);
        }

        // 5. 发布 MQTT 下行，复用原 Envelope bytes（QoS1）
        String mqttTopic = VehicleRouteCatalog.FOTA_ROUTE.mqttDownTopic(envelope.getDeviceId());
        try {
            downlinkPublisher.publish(mqttTopic, value);
        } catch (Exception e) {
            log.error("FOTA downlink MQTT publish failed: vin={}, deviceId={}, messageId={}",
                    LogMask.mask(kafkaKeyVin), LogMask.mask(envelope.getDeviceId()),
                    envelope.getMessageId(), e);
            return handleNonDeliverable(record, envelope, kafkaKeyVin, DeliveryReason.MQTT_PUBLISH_FAILED);
        }

        // 6. 技术 ACCEPTED：MQTT publish/PUBACK 仅形成技术投递状态，不生成业务回执
        bridgeInbox.recordFinal(VehicleBridgeInbox.InboxRecord.builder()
                .direction(VehicleBridgeInbox.Direction.DOWNLINK.name())
                .messageId(envelope.getMessageId())
                .envelopeSha256(digest)
                .kafkaTopic(record.topic())
                .partition(record.partition())
                .offset(record.offset())
                .mqttRoute(mqttTopic)
                .state(VehicleBridgeInbox.State.ACCEPTED.name())
                .outcome("OUTCOME_ACCEPTED")
                .updatedAt(Instant.now())
                .build());

        log.info("FOTA downlink delivered (technical): vin={}, deviceId={}, messageId={}, topic={}",
                LogMask.mask(kafkaKeyVin), LogMask.mask(envelope.getDeviceId()), envelope.getMessageId(),
                LogMask.maskIn(mqttTopic, envelope.getDeviceId()));
        return true;
    }

    /**
     * 不可投递：生产 GatewayDeliveryStatus（iov.vagw.delivery.fota，Key=VIN）并记录 REJECTED。
     * 生产失败则不提交 offset，交由 Kafka 重投（幂等记录避免重复 MQTT 发布）。
     */
    private boolean handleNonDeliverable(ConsumerRecord<String, byte[]> record,
                                         Envelope.VehicleMessageEnvelope envelope,
                                         String vin, DeliveryReason reason) {
        try {
            String resultMessageId = gatewayDeliveryService.produceRejected(envelope, vin, reason);
            bridgeInbox.recordFinal(VehicleBridgeInbox.InboxRecord.builder()
                    .direction(VehicleBridgeInbox.Direction.DOWNLINK.name())
                    .messageId(envelope.getMessageId())
                    .envelopeSha256(VehicleBridgeInbox.digestOf(record.value()))
                    .kafkaTopic(record.topic())
                    .partition(record.partition())
                    .offset(record.offset())
                    .state(VehicleBridgeInbox.State.REJECTED.name())
                    .outcome("OUTCOME_REJECTED")
                    .reason(reason.name())
                    .resultMessageId(resultMessageId)
                    .updatedAt(Instant.now())
                    .build());
            log.info("FOTA downlink rejected (technical): vin={}, messageId={}, reason={}",
                    LogMask.mask(vin), envelope.getMessageId(), reason);
            return true;
        } catch (Exception e) {
            log.error("FOTA downlink delivery-status produce failed: vin={}, reason={}, messageId={}",
                    LogMask.mask(vin), reason, envelope.getMessageId(), e);
            return false;
        }
    }

    private boolean moveToDlq(ConsumerRecord<String, byte[]> record, String key, byte[] value, String reason) {
        try {
            dlqPublisher.publish(VehicleRouteCatalog.DOWN_DLQ_TOPIC, key, value, reason);
            return true;
        } catch (Exception e) {
            log.error("FOTA downlink DLQ publish failed: key={}, reason={}", LogMask.mask(key), reason, e);
            return false;
        }
    }

    private void moveToDlqBestEffort(String dlqTopic, String key, byte[] value, String reason) {
        try {
            dlqPublisher.publish(dlqTopic, key, value, reason);
        } catch (Exception e) {
            log.error("FOTA DLQ publish failed: key={}, reason={}", LogMask.mask(key), reason, e);
        }
    }

    /**
     * 上行处理结果。
     */
    public record UplinkResult(boolean ok, ErrorCode errorCode, String reason) {
        public static UplinkResult success() {
            return new UplinkResult(true, null, null);
        }

        public static UplinkResult fail(ErrorCode code, String reason) {
            return new UplinkResult(false, code, reason);
        }
    }
}
