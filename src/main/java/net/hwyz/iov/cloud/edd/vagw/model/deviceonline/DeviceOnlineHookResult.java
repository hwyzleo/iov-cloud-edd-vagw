package net.hwyz.iov.cloud.edd.vagw.model.deviceonline;

/**
 * {@code client.connected} Hook 处理结果（EDD-VAGW-DSN-CR-012 §3.2/§6）。
 * <p>
 * 用于结构化观测与告警；除 {@link #ENQUEUED}/{@link #DUPLICATE} 外均表示 fail-closed，
 * 不建立可信会话、不写 Outbox。
 * </p>
 */
public enum DeviceOnlineHookResult {
    /** 已固化会话事实与待发布事件 */
    ENQUEUED,
    /** 同一 Hook 重试命中已有 eventId，幂等成功 */
    DUPLICATE,
    /** 无准入上下文（DENY/未知/未认证/快照丢失） */
    NO_ADMISSION_CONTEXT,
    /** 准入快照过期 */
    EXPIRED,
    /** 身份不一致（Hook 自报 hsmUid 与快照不符） */
    IDENTITY_MISMATCH,
    /** 旧 generation 迟到 */
    STALE_GENERATION,
    /** 有界恢复后仍无法持久化会话事实与待发布事件 */
    PERSIST_FAILED,
    /** 设备上线生产链路未启用 */
    DISABLED
}
