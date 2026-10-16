package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

import java.util.Optional;

import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_MESSAGE_EXPIRED;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_PAYLOAD_INVALID;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_PAYLOAD_TOO_LARGE;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING;

/**
 * keyprov canonical 协议校验器（EDD-VAGW-DSN-CR-011 §3.3）。
 * <p>
 * 校验 Topic/会话身份、service、protocol_version、message_kind、device_id、payload_type 精确
 * registry、TTL、size 与标识边界，并只做 canonical proto 结构门禁；不解释/改写 Wrapped Key
 * 密码学内容与业务语义，不生成新 requestId。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovProtocolValidator {

    private final KeyprovProperties properties;
    private final KeyprovContractGuard contractGuard;

    /**
     * 上行校验。
     *
     * @return null 通过；否则返回稳定失败 reason
     */
    public Failure validateUplink(Envelope.VehicleMessageEnvelope envelope, byte[] rawEnvelope,
                                  BrokerAuthenticatedPublishContext context) {
        if (envelope == null) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED, "envelope is null");
        }
        if (context == null || isBlank(context.hsmUid())) {
            return new Failure(KEYPROV_SESSION_CONTEXT_MISSING, "authenticated context missing");
        }
        if (!contractGuard.compatible()) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "keyprov contract incompatible: " + contractGuard.incompatibilityReason());
        }
        if (!properties.getService().equals(envelope.getService())) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED, "service mismatch: " + envelope.getService());
        }
        if (!properties.getProtocolVersion().equals(envelope.getProtocolVersion())) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "protocol_version mismatch: " + envelope.getProtocolVersion());
        }
        if (envelope.getMessageKind() != Envelope.MessageKind.MESSAGE_KIND_REQUEST) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "message_kind must be REQUEST: " + envelope.getMessageKind());
        }
        if (isBlank(envelope.getDeviceId()) || !context.hsmUid().equalsIgnoreCase(envelope.getDeviceId())) {
            return new Failure(KeyprovReason.IDENTITY_MISMATCH, "device_id does not match authenticated hsmUid");
        }
        Optional<KeyprovMessageType> type = KeyprovMessageType.fromFullName(envelope.getPayloadType());
        if (type.isEmpty() || !type.get().isUpstream()) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "payload_type not in keyprov upstream registry: " + envelope.getPayloadType());
        }
        int size = envelope.getPayload().size();
        if (size == 0) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "missing payload");
        }
        if (size > properties.getMaxPayloadBytes()) {
            return new Failure(KEYPROV_PAYLOAD_TOO_LARGE, "payload exceeds max size: " + size);
        }
        if (envelope.hasExpireAtMs() && envelope.getExpireAtMs() > 0) {
            long now = System.currentTimeMillis();
            long expiresAt = envelope.getExpireAtMs();
            if (expiresAt <= now) {
                // 只输出非敏感时间差：既证过期程度，又能定位设备时钟偏移 / 陈旧重投
                // （deviceTimestampSkewMs > 0 表示设备 timestamp_ms 落后于网关时钟；-1 表示未提供）
                long deviceTimestampSkewMs =
                        envelope.getTimestampMs() > 0 ? now - envelope.getTimestampMs() : -1L;
                return new Failure(KEYPROV_MESSAGE_EXPIRED,
                        "message expired (expire_at_ms), expiredByMs=" + (now - expiresAt)
                                + ", deviceTimestampSkewMs=" + deviceTimestampSkewMs);
            }
        }
        int maxId = properties.getIdentifierMaxLength();
        if (isBlank(envelope.getMessageId()) || exceeds(envelope.getMessageId(), maxId)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "message_id missing or too long");
        }
        if (isBlank(envelope.getRequestId()) || exceeds(envelope.getRequestId(), maxId)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request_id missing or too long");
        }
        // canonical 结构门禁：按精确 PayloadType 解析生成类型，失败即拒绝（不落盘、不回显内容）
        try {
            type.get().parser().parseFrom(envelope.getPayload().toByteArray());
        } catch (Exception e) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "canonical payload decode failed");
        }
        return null;
    }

    public boolean isAllowedDownlinkPayloadType(String payloadType) {
        return KeyprovMessageType.fromFullName(payloadType).filter(t -> !t.isUpstream()).isPresent();
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static boolean exceeds(String s, int maxBytes) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maxBytes;
    }

    public record Failure(KeyprovReason reason, String message) {
    }
}
