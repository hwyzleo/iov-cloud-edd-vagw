package net.hwyz.iov.cloud.edd.vagw.model.binding;

/**
 * 绑定来源（EDD-VAGW-DSN-CR-007 §2）。
 * <p>
 * 记录绑定关系写入缓存的来源，用于审计与版本治理：
 * 准入成功（ADMISSION）建立的会话级映射、TSP 反查回源（TSP）写回的映射。
 * </p>
 */
public enum BindingSource {
    /** 缓存命中（未写入，仅读取） */
    CACHE,
    /** 连接准入 ALLOW 后由 VAGW 写入 */
    ADMISSION,
    /** TSP resolveByVin 回源解析结果写入 */
    TSP
}
