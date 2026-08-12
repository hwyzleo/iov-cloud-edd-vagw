package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * FOTA 技术投递结果 reasonCode（OTA 契约），对齐 EDD-VAGW-DSN-CR-005 §7。
 * <p>
 * VAGW 只产生技术投递结果；reasonCode 为 OTA 契约仓定义的枚举，供 IOV-OTA 领域消费。
 * </p>
 */
@Getter
@RequiredArgsConstructor
public enum DeliveryReason {
    VEHICLE_OFFLINE(true, 300),
    VIN_UNBOUND(true, 300),
    DEVICE_MISMATCH(false, null),
    PERMISSION_DENIED(false, null),
    MESSAGE_EXPIRED(false, null),
    MQTT_PUBLISH_FAILED(true, 300),
    CONTRACT_INVALID(false, null);

    private final boolean retryable;
    private final Integer retryAfterSec;
}
