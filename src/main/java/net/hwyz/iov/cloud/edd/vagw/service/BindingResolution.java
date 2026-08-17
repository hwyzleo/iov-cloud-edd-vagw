package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;

/**
 * 绑定解析结果（EDD-VAGW-DSN-CR-007 §4）。
 * <p>
 * 携带结构化状态与原始细分 reason，调用方据此决定拒绝/隔离/告警，
 * 不得把不同失败压缩成单一 VIN_UNBOUND。
 * </p>
 */
public record BindingResolution(ResolutionStatus status, VehicleAccessBinding binding, String reason) {

    public static BindingResolution resolved(VehicleAccessBinding binding) {
        return new BindingResolution(ResolutionStatus.RESOLVED, binding, null);
    }

    public static BindingResolution unbound(String reason) {
        return new BindingResolution(ResolutionStatus.UNBOUND, null, reason);
    }

    public static BindingResolution conflict() {
        return new BindingResolution(ResolutionStatus.BINDING_CONFLICT, null, "BINDING_CONFLICT");
    }

    public static BindingResolution dependencyUnavailable(String reason) {
        return new BindingResolution(ResolutionStatus.DEPENDENCY_UNAVAILABLE, null, reason);
    }

    public static BindingResolution contextMissing(String reason) {
        return new BindingResolution(ResolutionStatus.CONTEXT_MISSING, null, reason);
    }
}
