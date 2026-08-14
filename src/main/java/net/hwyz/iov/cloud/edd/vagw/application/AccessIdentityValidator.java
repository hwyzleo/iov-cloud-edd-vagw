package net.hwyz.iov.cloud.edd.vagw.application;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 接入身份校验（EDD-VAGW-DSN-CR-006 §6 校验顺序 3 / US-014）。
 * <p>
 * 要求 MQTT Topic device key、接入身份（EMQX mTLS/ACL 认证的设备标识）与 Envelope.device_id
 * 使用同一规范值；不一致按身份越权拒绝并审计。接入身份由 EMQX 在接入层强制（Topic 权限），
 * 本组件负责 Topic 标识与 Envelope 声明身份的一致性校验，不静默猜测或兜底映射。
 * </p>
 */
@Slf4j
@Component
public class AccessIdentityValidator {

    /**
     * 校验 Topic device key 与 Envelope.device_id 一致。
     *
     * @return null 通过；否则返回失败原因
     */
    public Failure validate(String topicDeviceKey, String envelopeDeviceId) {
        if (topicDeviceKey == null || topicDeviceKey.isBlank()) {
            return new Failure(Reason.CONTRACT_INVALID, "topic device key is blank");
        }
        if (envelopeDeviceId == null || envelopeDeviceId.isBlank()) {
            return new Failure(Reason.CONTRACT_INVALID, "missing envelope device_id");
        }
        if (!topicDeviceKey.equalsIgnoreCase(envelopeDeviceId)) {
            return new Failure(Reason.DEVICE_MISMATCH,
                    "topic device key does not match envelope device_id (identity mismatch)");
        }
        return null;
    }

    public enum Reason { CONTRACT_INVALID, DEVICE_MISMATCH }

    public record Failure(Reason reason, String message) {
    }
}
