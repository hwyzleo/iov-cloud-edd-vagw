package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

/**
 * VMD relay 结果类别（非 VAGW 业务语义，仅路由/传输层结果）。
 */
public enum KeyprovOutcome {
    /** VMD 返回 canonical 下行 payload。 */
    SUCCESS,
    /** VMD 明确拒绝（可能携带 canonical 错误 payload，也可能无 payload）。 */
    REJECTED,
    /** VMD 不可达。 */
    UNAVAILABLE,
    /** 结果未知（超时/网络中断，已发送未确认）。 */
    OUTCOME_UNKNOWN
}
