package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;

/**
 * VAGW 内部 VMD relay 结果（EDD-VAGW-DSN-CR-016 §2.4）。
 * <p>
 * 由官方 {@code VmdKeyProvisioning*} 响应映射得到；不是跨服务 wire DTO，不承载 KMS 内部 keyRef，
 * 也不承载 VMD 路由身份（官方 wire 只返回 canonical payload bytes 与稳定 806xxx 错误）。
 * {@code downlinkPayload} 为敏感密文，不得写入日志/持久化/异常正文。
 * </p>
 *
 * @param requestId           原上行 requestId（幂等/关联）
 * @param downlinkPayloadType canonical 下行 PayloadType（仅允许 security.keyprov.v1 三类）
 * @param downlinkPayload     canonical 下行 payload bytes（敏感密文）
 * @param expireAtMs          下行 TTL（VMD 提供时）
 * @param outcome             传输/路由层结果类别
 * @param reason              非敏感稳定 reason（审计）；成功时为 {@code null}
 */
public record VmdRelayResult(
        String requestId,
        String downlinkPayloadType,
        byte[] downlinkPayload,
        Long expireAtMs,
        KeyprovOutcome outcome,
        KeyprovReason reason) {

    public boolean hasPayload() {
        return downlinkPayloadType != null && !downlinkPayloadType.isBlank()
                && downlinkPayload != null;
    }

    public static VmdRelayResult unavailable(KeyprovReason reason) {
        return new VmdRelayResult(null, null, null, null, KeyprovOutcome.UNAVAILABLE, reason);
    }

    public static VmdRelayResult timeout(KeyprovReason reason) {
        return new VmdRelayResult(null, null, null, null, KeyprovOutcome.TIMEOUT, reason);
    }

    public static VmdRelayResult circuitOpen(KeyprovReason reason) {
        return new VmdRelayResult(null, null, null, null, KeyprovOutcome.CIRCUIT_OPEN, reason);
    }

    public static VmdRelayResult rejected(String requestId, KeyprovReason reason) {
        return new VmdRelayResult(requestId, null, null, null, KeyprovOutcome.REJECTED, reason);
    }

    public static VmdRelayResult outcomeUnknown(String requestId, KeyprovReason reason) {
        return new VmdRelayResult(requestId, null, null, null, KeyprovOutcome.OUTCOME_UNKNOWN, reason);
    }

    /** 敏感密文安全摘要：不输出 payload 内容。 */
    @Override
    public String toString() {
        return "VmdRelayResult{requestId=" + requestId
                + ", downlinkPayloadType=" + downlinkPayloadType
                + ", expireAtMs=" + expireAtMs
                + ", outcome=" + outcome
                + ", reason=" + reason
                + ", payloadBytes=" + (downlinkPayload == null ? 0 : downlinkPayload.length)
                + '}';
    }
}
