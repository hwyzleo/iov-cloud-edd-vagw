package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;

/**
 * VMD relay 响应（EDD-VAGW-DSN-CR-011 §3.5）。
 * <p>
 * 只承载 canonical 下行 payload 类型/bytes 与非敏感路由结果；不返回 KMS 内部 keyRef。
 * {@code downlinkPayload} 为敏感密文，不得写入日志/持久化/异常正文。
 * </p>
 *
 * @param requestId           原上行 requestId（幂等/关联）
 * @param downlinkPayloadType canonical 下行 PayloadType（仅允许 security.keyprov.v1 三类）
 * @param downlinkPayload     canonical 下行 payload bytes（敏感密文）
 * @param expireAtMs          下行 TTL（VMD 提供时）
 * @param routingHsmUid       VMD 返回的目标 hsmUid（仅用于与认证上下文交叉校验）
 * @param outcome             传输/路由层结果类别
 * @param reasonCode          非敏感稳定 reason（审计）
 */
public record VmdRelayResponse(
        String requestId,
        String downlinkPayloadType,
        byte[] downlinkPayload,
        Long expireAtMs,
        String routingHsmUid,
        KeyprovOutcome outcome,
        String reasonCode) {

    public boolean hasPayload() {
        return downlinkPayloadType != null && !downlinkPayloadType.isBlank()
                && downlinkPayload != null;
    }

    public static VmdRelayResponse unavailable(String reasonCode) {
        return new VmdRelayResponse(null, null, null, null, null, KeyprovOutcome.UNAVAILABLE, reasonCode);
    }

    public static VmdRelayResponse rejected(String requestId, String reasonCode) {
        return new VmdRelayResponse(requestId, null, null, null, null, KeyprovOutcome.REJECTED, reasonCode);
    }

    public static VmdRelayResponse outcomeUnknown(String requestId, String reasonCode) {
        return new VmdRelayResponse(requestId, null, null, null, null, KeyprovOutcome.OUTCOME_UNKNOWN, reasonCode);
    }
}
