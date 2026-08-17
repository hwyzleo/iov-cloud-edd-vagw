package net.hwyz.iov.cloud.edd.vagw.service;

/**
 * 绑定解析状态（EDD-VAGW-DSN-CR-007 §4/§5/§7）。
 * <p>
 * 区分业务未绑定与基础设施/上下文状态，避免把系统未初始化静默等同于真实 UNBOUND：
 * 只有 TSP 明确返回未绑定时才为 UNBOUND；缓存/会话映射缺失记为 CONTEXT_MISSING；
 * TSP 不可达记为 DEPENDENCY_UNAVAILABLE；同 VIN 多条 ACTIVE 绑定记为 BINDING_CONFLICT。
 * </p>
 */
public enum ResolutionStatus {
    /** 已解析到当前绑定 */
    RESOLVED,
    /** TSP 明确未绑定（VIN_UNKNOWN / UNBOUND / TBOX_NOT_FOUND / HSM_UID_MISSING） */
    UNBOUND,
    /** 同 VIN 多条 ACTIVE 绑定，fail-closed */
    BINDING_CONFLICT,
    /** 绑定反查依赖（TSP / Redis 读取异常）不可用 */
    DEPENDENCY_UNAVAILABLE,
    /** 上行会话/缓存绑定上下文缺失（基础设施状态，非真实 UNBOUND） */
    CONTEXT_MISSING
}
