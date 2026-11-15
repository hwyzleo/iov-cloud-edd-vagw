package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;

import java.util.Optional;

/**
 * 设备绑定解析服务（EDD-VAGW-DSN-CR-007 §4，上行自愈由 EDD-VAGW-DSN-CR-020 补充）。
 * <p>
 * 负责 VIN ↔ hsmUid（南向接入身份）双向解析：下行按 VIN 走版本化缓存 + VIAM 回源；
 * 上行按 {@link #resolveForUplink(String, String)} 三级解析（会话缓存 → VIAM 回源 →
 * 收缩的 CONTEXT_MISSING），绑定缓存由 VAGW 自主建立，不依赖外部预热。
 * 缓存不是 SSOT：TTL 只是加速层陈旧上限，不再是映射业务可用期。
 * </p>
 */
public interface BindingService {

    // ------------------------------------------------------------------
    // 新 API（CR-007）
    // ------------------------------------------------------------------

    /**
     * 按 VIN 解析当前接入绑定（权威：版本化缓存命中 → VIAM 回源）。
     *
     * @param vin 车辆 VIN
     * @return 结构化解析结果，区分 UNBOUND / CONFLICT / DEPENDENCY_UNAVAILABLE
     */
    BindingResolution resolveByVin(String vin);

    /**
     * 按 hsmUid（南向接入身份）解析当前接入绑定，<b>只读缓存</b>（不做回源）。
     * 用于会话富化等内部只读场景；上行桥接必须使用 {@link #resolveForUplink(String, String)}。
     * 映射意外缺失时返回 CONTEXT_MISSING，不得把基础设施状态误记为真实 UNBOUND。
     *
     * @param hsmUid HSM UID（证书 CN / MQTT device key）
     * @return 结构化解析结果
     */
    BindingResolution resolveByHsmUid(String hsmUid);

    /**
     * 上行绑定解析（EDD-VAGW-DSN-CR-020 §4.1）：会话级缓存 → 以 Envelope.vin 回源 VIAM
     * resolveByVin 并做双向校验 → 收缩的 CONTEXT_MISSING。
     * <p>
     * 回源只在“回源 hsmUid ＝ 入参 hsmUid”时放行并回写缓存；身份不一致视为 VIN 已改绑，
     * 原子失效双向 key 并返回 BINDING_CONFLICT。Envelope.vin 只作解析输入，不作为身份声明；
     * 本方法不回写、不补写 Envelope 任何字段。
     * </p>
     *
     * @param hsmUid     接入身份＝Topic device key＝Envelope.device_id
     * @param envelopeVin Envelope.vin（解析输入；调用方已保证非空）
     * @return 结构化解析结果（含回源治理的过载/超时细分 reason）
     */
    BindingResolution resolveForUplink(String hsmUid, String envelopeVin);

    /**
     * 会话期滑动续期 hsmUid→VIN 映射（EDD-VAGW-DSN-CR-020 §4.4）。
     * <p>
     * 仅对已存在的 key 生效（不复活已失效映射），TTL 取
     * {@code min(cache-ttl, session-max-ttl 锚点剩余)}。
     * </p>
     *
     * @param hsmUid HSM UID
     */
    void renewSessionMapping(String hsmUid);

    /**
     * 可信 client.disconnected 后清理会话级 hsmUid→VIN 映射（EDD-VAGW-DSN-CR-020 §4.4）。
     * <p>
     * 只删除 hsm 方向会话级映射（含单会话锚点），不删除 VIN 方向映射（下行仍需解析），
     * 不影响安全失效语义。
     * </p>
     *
     * @param hsmUid HSM UID
     */
    void clearSessionMapping(String hsmUid);

    /**
     * 准入 ALLOW 成功后写入本次会话所需的 hsmUid ↔ VIN 双向映射。
     *
     * @param hsmUid         南向接入身份
     * @param vin            车辆 VIN
     * @param bindingVersion 绑定版本（准入接口未返回时传 null）
     */
    void rememberAdmission(String hsmUid, String vin, Long bindingVersion);

    /**
     * 使绑定失效（换件 / 解绑 / 身份不一致 / 版本变化时旧双向 key 原子失效）。
     */
    void invalidate(String vin, String hsmUid, InvalidateReason reason);

    // ------------------------------------------------------------------
    // 兼容 API（deprecated，只委托新实现，不得包含空桩）
    // ------------------------------------------------------------------

    /**
     * @deprecated 使用 {@link #resolveByHsmUid(String)}；参数实际为南向 hsmUid
     */
    @Deprecated
    Optional<String> resolveVin(String hsmUid);

    /**
     * @deprecated 使用 {@link #resolveByVin(String)}（缓存未命中回源 VIAM，不再固定返回空）
     */
    @Deprecated
    Optional<String> resolveDeviceSn(String vin);

    /**
     * @deprecated 使用 {@link #resolveByHsmUid(String)} + {@link ResolutionStatus#RESOLVED}
     */
    @Deprecated
    boolean isValidAndBound(String hsmUid);
}
