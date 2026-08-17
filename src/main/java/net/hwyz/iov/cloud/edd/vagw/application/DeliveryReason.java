package net.hwyz.iov.cloud.edd.vagw.application;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import vagw.v1.Delivery;

/**
 * VAGW 技术投递原因（EDD-VAGW-DSN-CR-006 §8）。
 * <p>
 * 仅表达 VAGW／MQTT 技术接管层面的拒绝原因；不宣称 FOTA Task／Execution 成功或失败。
 * 与 proto-vagw 的 OUTCOME 枚举配合：REJECTED + retryable 语义。
 * </p>
 */
@Getter
@RequiredArgsConstructor
public enum DeliveryReason {
    VEHICLE_OFFLINE(Delivery.Outcome.OUTCOME_REJECTED, true, 300_000L),
    VIN_UNBOUND(Delivery.Outcome.OUTCOME_REJECTED, true, 300_000L),
    BINDING_CONFLICT(Delivery.Outcome.OUTCOME_REJECTED, false, null),
    BINDING_DEPENDENCY_UNAVAILABLE(Delivery.Outcome.OUTCOME_REJECTED, true, 30_000L),
    BINDING_CONTEXT_MISSING(Delivery.Outcome.OUTCOME_REJECTED, true, 30_000L),
    DEVICE_MISMATCH(Delivery.Outcome.OUTCOME_REJECTED, false, null),
    PERMISSION_DENIED(Delivery.Outcome.OUTCOME_REJECTED, false, null),
    MESSAGE_EXPIRED(Delivery.Outcome.OUTCOME_REJECTED, false, null),
    MQTT_PUBLISH_FAILED(Delivery.Outcome.OUTCOME_REJECTED, true, 300_000L);

    private final Delivery.Outcome outcome;
    private final boolean retryable;
    private final Long retryAfterMs;
}
