package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;

import java.util.Optional;

/**
 * 设备绑定解析服务（EDD-VAGW-DSN-CR-007 §4）。
 * <p>
 * 负责 VIN ↔ hsmUid（南向接入身份）双向解析：下行按 VIN 走版本化缓存 + TSP 回源；
 * 上行优先使用准入成功时写入的会话绑定；绑定缓存由 VAGW 自主建立，不依赖外部预热。
 * </p>
 */
public interface BindingService {

    // ------------------------------------------------------------------
    // 新 API（CR-007）
    // ------------------------------------------------------------------

    /**
     * 按 VIN 解析当前接入绑定（权威：版本化缓存命中 → TSP 回源）。
     *
     * @param vin 车辆 VIN
     * @return 结构化解析结果，区分 UNBOUND / CONFLICT / DEPENDENCY_UNAVAILABLE
     */
    BindingResolution resolveByVin(String vin);

    /**
     * 按 hsmUid（南向接入身份）解析当前接入绑定。
     * 优先使用当前连接/会话绑定（准入时写入），不依赖外部预热 Redis；
     * 映射意外缺失时返回 CONTEXT_MISSING，不得把基础设施状态误记为真实 UNBOUND。
     *
     * @param hsmUid HSM UID（证书 CN / MQTT device key）
     * @return 结构化解析结果
     */
    BindingResolution resolveByHsmUid(String hsmUid);

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
     * @deprecated 使用 {@link #resolveByVin(String)}（缓存未命中回源 TSP，不再固定返回空）
     */
    @Deprecated
    Optional<String> resolveDeviceSn(String vin);

    /**
     * @deprecated 使用 {@link #resolveByHsmUid(String)} + {@link ResolutionStatus#RESOLVED}
     */
    @Deprecated
    boolean isValidAndBound(String hsmUid);
}
