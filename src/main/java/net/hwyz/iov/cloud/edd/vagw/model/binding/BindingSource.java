package net.hwyz.iov.cloud.edd.vagw.model.binding;

/**
 * 绑定来源（EDD-VAGW-DSN-CR-007 §2，owner 由 EDD-VAGW-DSN-CR-019 切换 VIAM）。
 * <p>
 * 记录绑定关系写入缓存的来源，用于审计与版本治理：
 * 准入成功（VIAM ALLOW）建立的会话级映射、VIAM resolveByVin 回源（VIAM_RESOLVE）
 * 写回的映射。
 * </p>
 * <p>
 * 兼容性（CR-019 §4.2）：旧 Redis 缓存中的 {@code TSP}/{@code ADMISSION} 枚举值在切换后
 * 反序列化失败 → 缓存未命中 → 回源 VIAM 重新解析；来源不明或版本不可验证的旧缓存
 * 不继续使用（缓存是投影而非 SSOT，不得误写业务事实）。
 * </p>
 */
public enum BindingSource {
    /** 缓存命中（未写入，仅读取） */
    CACHE,
    /** 连接准入 VIAM ALLOW 后由 VAGW 写入 */
    VIAM_ADMISSION,
    /** VIAM resolveByVin 回源解析结果写入 */
    VIAM_RESOLVE
}
