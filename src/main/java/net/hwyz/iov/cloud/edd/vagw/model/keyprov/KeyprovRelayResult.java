package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;

/**
 * keyprov relay 处理结果（仅传输/审计层结果，不含 Wrapped Key 内容）。
 *
 * @param delivered      是否已把 VMD 返回的 canonical 下行 payload 可靠发布到 MQTT
 * @param errorCode      网关错误码（成功为 null；失败统一 804017，身份不一致为 804003）
 * @param reason         稳定失败 reason（成功为 null）
 * @param messageId      上行 messageId（关联/审计用）
 * @param correlationId  下行 correlation_id（= 上行 messageId）
 * @param downlinkTopic  下行 MQTT Topic（已发布时）
 */
public record KeyprovRelayResult(
        boolean delivered,
        ErrorCode errorCode,
        KeyprovReason reason,
        String messageId,
        String correlationId,
        String downlinkTopic) {

    public static KeyprovRelayResult delivered(String messageId, String downlinkTopic) {
        return new KeyprovRelayResult(true, null, null, messageId, messageId, downlinkTopic);
    }

    public static KeyprovRelayResult duplicate(String messageId) {
        return new KeyprovRelayResult(true, null, null, messageId, messageId, null);
    }

    public static KeyprovRelayResult fail(KeyprovReason reason) {
        return fail(reason, null);
    }

    public static KeyprovRelayResult fail(KeyprovReason reason, String messageId) {
        ErrorCode code = reason == KeyprovReason.IDENTITY_MISMATCH
                ? ErrorCode.IDENTITY_MISMATCH : ErrorCode.KEYPROV_UPSTREAM_FAILURE;
        return new KeyprovRelayResult(false, code, reason, messageId, messageId, null);
    }
}
