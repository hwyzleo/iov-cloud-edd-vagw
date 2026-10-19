package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovInbox;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovReadiness;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import vehicle.common.v1.Envelope;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * keyprov relay 编排服务（EDD-VAGW-DSN-CR-011 §3.4/§7，由 EDD-VAGW-DSN-CR-016 §2 增量修订）。
 * <p>
 * 本服务作为 Keyprov 专属执行器任务体运行：MQTT ingress 完成门禁并获得专属并发许可后，将最小
 * 不可变任务提交到 {@code KeyprovExecutor}，本服务在其线程内执行阻塞式 VMD 调用与编排。
 * 职责仅限：generation guard、correlation、VMD 调用与下行结果映射，保留 requestId/idempotencyKey。
 * 不负责设备绑定裁决、业务授权、选钥、businessKeyVersion、KMS Wrap、导入 ACK 业务完成或激活状态。
 * 不写 Kafka/DLQ/DB/Redis/Outbox；Wrapped Key、parameters、importProof 只在调用栈内短暂透传。
 * </p>
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovRelayAppService {

    private final KeyprovProperties properties;
    private final KeyprovContractGuard contractGuard;
    private final KeyprovProtocolValidator protocolValidator;
    private final KeyprovInbox inbox;
    private final VmdKeyProvisioningGateway vmdGateway;
    private final KeyprovEnvelopeFactory envelopeFactory;
    private final KeyprovMqttDownlinkPublisher downlinkPublisher;
    private final SessionService sessionService;
    private final KeyprovMetrics metrics;

    public KeyprovRelayResult relay(BrokerAuthenticatedPublishContext context,
                                    Envelope.VehicleMessageEnvelope uplink,
                                    byte[] rawEnvelope) {
        if (!properties.isEnabled()) {
            return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_DISABLED, messageId(uplink));
        }
        KeyprovMessageType uplinkType = uplink == null ? null
                : KeyprovMessageType.fromFullName(uplink.getPayloadType()).orElse(null);
        KeyprovReadiness readiness = contractGuard.readiness();
        if (uplinkType == null || readiness == null || !readiness.allows(uplinkType)) {
            KeyprovReason reason = readiness == null || readiness.reason() == null
                    ? KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED : readiness.reason();
            log.warn("keyprov relay rejected: readiness={}, reason={}, detail={}",
                    readiness == null ? null : readiness.ready(), reason,
                    readiness == null ? null : readiness.notReadyDetail());
            return KeyprovRelayResult.fail(reason, messageId(uplink));
        }

        KeyprovProtocolValidator.Failure failure = protocolValidator.validateUplink(uplink, rawEnvelope, context);
        if (failure != null) {
            log.warn("keyprov uplink rejected: reason={}, detail={}, hsmUid={}, payloadType={}",
                    failure.reason(), failure.message(), LogMask.mask(context == null ? null : context.hsmUid()),
                    LogMask.maskPayloadType(uplink == null ? null : uplink.getPayloadType()));
            return KeyprovRelayResult.fail(failure.reason(), messageId(uplink));
        }

        // validator 已按同一 readiness 门控校验；此处不再重新解析/判断 PayloadType 方向

        // 传输层短期去重：相同 messageId+摘要不重复调用 VMD；不同摘要冲突 fail-closed
        KeyprovInbox.State state = inbox.record(uplink.getMessageId(), sha256(rawEnvelope));
        if (state == KeyprovInbox.State.CONFLICT) {
            log.warn("keyprov uplink digest conflict: messageId={}", uplink.getMessageId());
            return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_PAYLOAD_INVALID, uplink.getMessageId());
        }
        if (state == KeyprovInbox.State.DUPLICATE) {
            log.info("keyprov uplink duplicate skipped: messageId={}, payloadType={}",
                    uplink.getMessageId(), LogMask.maskPayloadType(uplink.getPayloadType()));
            metrics.downlink("duplicate");
            return KeyprovRelayResult.duplicate(uplink.getMessageId());
        }

        VmdRelayResult response;
        byte[] canonicalPayload = uplink.getPayload().toByteArray();
        try {
            response = vmdGateway.relay(new VmdKeyProvisioningCommand(context, uplink, canonicalPayload));
        } catch (Exception e) {
            log.error("keyprov VMD relay failed: requestId={}, messageId={}",
                    uplink.getRequestId(), uplink.getMessageId(), e);
            inbox.forget(uplink.getMessageId());
            metrics.downlink("not_delivered");
            return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_VMD_UNAVAILABLE, uplink.getMessageId());
        } finally {
            // 敏感 canonical payload 引用在处理终态立即释放（清零），不进入后续日志/持久化
            Arrays.fill(canonicalPayload, (byte) 0);
        }
        if (response == null) {
            inbox.forget(uplink.getMessageId());
            metrics.downlink("not_delivered");
            return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_VMD_UNAVAILABLE, uplink.getMessageId());
        }

        if (!response.hasPayload()) {
            inbox.forget(uplink.getMessageId());
            KeyprovReason reason = response.reason() != null
                    ? response.reason() : mapNoPayloadReason(response.outcome());
            log.warn("keyprov relay failed (no canonical payload): requestId={}, messageId={}, outcome={}, reason={}",
                    uplink.getRequestId(), uplink.getMessageId(), response.outcome(), reason);
            metrics.downlink("not_delivered");
            return KeyprovRelayResult.fail(reason, uplink.getMessageId());
        }

        // VAGW 只按 payloadType 与 correlation 转发：类型必须与上行操作对应且在精确下行 registry 内
        try {
            KeyprovMessageType expectedDownlink = uplinkType.expectedDownlink();
            if (!expectedDownlink.fullName().equals(response.downlinkPayloadType())
                    || !protocolValidator.isAllowedDownlinkPayloadType(response.downlinkPayloadType())) {
                log.warn("keyprov downlink payload_type rejected: expected={}, actual={}, messageId={}",
                        expectedDownlink.fullName(), response.downlinkPayloadType(), uplink.getMessageId());
                inbox.forget(uplink.getMessageId());
                metrics.downlink("not_delivered");
                return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED, uplink.getMessageId());
            }

            // 下行前再次校验目标身份未被替换（generation guard，fail-closed）
            // 官方 wire 不再返回路由身份；VAGW 只以认证会话快照复核当前 active session（§2.6）
            if (!sessionService.isGenerationCurrent(context.hsmUid(), context.connectionGeneration())) {
                log.warn("keyprov downlink rejected: session replaced/stale, messageId={}, hsmUid={}",
                        uplink.getMessageId(), LogMask.mask(context.hsmUid()));
                inbox.forget(uplink.getMessageId());
                metrics.downlink("not_delivered");
                return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_SESSION_STALE, uplink.getMessageId());
            }

            byte[] downlinkBytes = envelopeFactory.buildDownlink(
                    context, uplink, response.downlinkPayloadType(), response.downlinkPayload(),
                    response.expireAtMs()).toByteArray();

            String topic;
            try {
                topic = downlinkPublisher.publish(context.hsmUid(), downlinkBytes);
            } catch (Exception e) {
                log.error("keyprov downlink MQTT publish failed: messageId={}, hsmUid={}",
                        uplink.getMessageId(), LogMask.mask(context.hsmUid()), e);
                inbox.forget(uplink.getMessageId());
                metrics.downlink("failed");
                return KeyprovRelayResult.fail(KeyprovReason.KEYPROV_DOWNLINK_PUBLISH_FAILED, uplink.getMessageId());
            }

            log.info("keyprov relayed: requestId={}, messageId={}, outcome={}, uplinkType={}, downlinkType={}, payloadBytes={}",
                    uplink.getRequestId(), uplink.getMessageId(), response.outcome(),
                    LogMask.maskPayloadType(uplink.getPayloadType()),
                    LogMask.maskPayloadType(response.downlinkPayloadType()),
                    response.downlinkPayload() == null ? 0 : response.downlinkPayload().length);
            metrics.downlink("delivered");
            return KeyprovRelayResult.delivered(uplink.getMessageId(), topic);
        } finally {
            // 任务终态（成功/失败/stale/deadline）立即释放 VMD 返回的敏感下行 payload 引用
            if (response.downlinkPayload() != null) {
                Arrays.fill(response.downlinkPayload(), (byte) 0);
            }
        }
    }

    private static KeyprovReason mapNoPayloadReason(KeyprovOutcome outcome) {
        if (outcome == null) {
            return KeyprovReason.KEYPROV_VMD_UNAVAILABLE;
        }
        return switch (outcome) {
            case REJECTED -> KeyprovReason.KEYPROV_VMD_REJECTED;
            case UNAVAILABLE -> KeyprovReason.KEYPROV_VMD_UNAVAILABLE;
            case OUTCOME_UNKNOWN -> KeyprovReason.KEYPROV_OUTCOME_UNKNOWN;
            case TIMEOUT -> KeyprovReason.KEYPROV_VMD_TIMEOUT;
            case CIRCUIT_OPEN -> KeyprovReason.KEYPROV_CIRCUIT_OPEN;
            case SUCCESS -> KeyprovReason.KEYPROV_VMD_UNAVAILABLE;
        };
    }

    private static String messageId(Envelope.VehicleMessageEnvelope envelope) {
        return envelope == null ? null : envelope.getMessageId();
    }

    private static String sha256(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(bytes == null ? new byte[0] : bytes));
        } catch (Exception e) {
            // SHA-256 必然可用；退化用长度避免 NPE（不输出内容）
            return "len:" + (bytes == null ? 0 : bytes.length);
        }
    }
}
