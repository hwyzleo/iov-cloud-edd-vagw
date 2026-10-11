package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

/**
 * keyprov 稳定失败/审计 reason（EDD-VAGW-DSN-CR-011 §7）。
 * <p>
 * 全部 keyprov 失败统一映射网关错误码 804017（KEYPROV_UPSTREAM_FAILURE），
 * 具体原因由本枚举表达；身份不一致单独使用 804003。
 * </p>
 */
public enum KeyprovReason {
    /** 能力未启用（keyprov.enabled=false）。 */
    KEYPROV_DISABLED,
    /** service/protocol_version/message_kind/payload_type/方向不匹配精确契约。 */
    KEYPROV_PROTOCOL_UNSUPPORTED,
    /** payload 结构非法或不可 canonical 解码。 */
    KEYPROV_PAYLOAD_INVALID,
    /** payload 超过 16 KiB。 */
    KEYPROV_PAYLOAD_TOO_LARGE,
    /** 会话/认证上下文缺失（无活动会话或无法关联认证身份）。 */
    KEYPROV_SESSION_CONTEXT_MISSING,
    /** 会话已失效或 connection generation 过期。 */
    KEYPROV_SESSION_STALE,
    /** VMD 明确拒绝且未返回 canonical 下行 payload。 */
    KEYPROV_VMD_REJECTED,
    /** VMD 不可达（超时/连接失败/熔断）。 */
    KEYPROV_VMD_UNAVAILABLE,
    /** VMD 结果未知（已发送未收到确定结果）；不得自动更换 requestId 重试。 */
    KEYPROV_OUTCOME_UNKNOWN,
    /** MQTT 下行发布失败。 */
    KEYPROV_DOWNLINK_PUBLISH_FAILED,
    /** Topic/会话/Envelope 设备身份不一致（804003）。 */
    IDENTITY_MISMATCH
}
