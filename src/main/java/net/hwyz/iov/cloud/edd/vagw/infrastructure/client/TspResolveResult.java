package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * TSP 车辆接入身份反查结果（EDD-VAGW-DSN-CR-007 §3）。
 * <p>
 * 客户端将 TSP Feign 响应校验并映射为 VAGW 内部结果，不丢失原始语义；
 * 基础设施异常（超时/连接失败/熔断/非法响应）统一映射为 UNRESOLVED + DEPENDENCY_UNAVAILABLE，
 * fail-closed，不返回伪造 HSM UID。
 * </p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TspResolveResult {

    /** 解析状态 */
    private Status status;
    /** 原始 TSP reason（VIN_UNKNOWN / UNBOUND / TBOX_NOT_FOUND / HSM_UID_MISSING / BINDING_CONFLICT / DEPENDENCY_UNAVAILABLE） */
    private String reason;
    /** 车辆 VIN */
    private String vin;
    /** HSM UID（仅 RESOLVED 时有效） */
    private String hsmUid;
    /** TBOX 序列号（仅资产追溯） */
    private String tboxSn;
    /** 绑定版本 */
    private Long bindingVersion;
    /** 绑定最近更新时间 */
    private Instant bindingUpdatedAt;

    public enum Status {
        RESOLVED,
        UNRESOLVED
    }

    public static TspResolveResult resolved(String vin, String hsmUid, String tboxSn,
                                            Long bindingVersion, Instant bindingUpdatedAt) {
        return TspResolveResult.builder()
                .status(Status.RESOLVED)
                .reason("RESOLVED")
                .vin(vin)
                .hsmUid(hsmUid)
                .tboxSn(tboxSn)
                .bindingVersion(bindingVersion)
                .bindingUpdatedAt(bindingUpdatedAt)
                .build();
    }

    public static TspResolveResult unresolved(String vin, String reason) {
        return TspResolveResult.builder()
                .status(Status.UNRESOLVED)
                .reason(reason)
                .vin(vin)
                .build();
    }
}
