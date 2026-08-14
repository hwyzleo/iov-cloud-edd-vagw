package net.hwyz.iov.cloud.edd.vagw.infrastructure;

/**
 * FOTA 透明桥接基础设施异常（EDD-VAGW-DSN-CR-006）。
 * <p>
 * 用于 Kafka produce / MQTT publish / Inbox / DLQ 等桥接层技术故障；
 * 调用方据此决定有限重试、进 DLQ 或记录技术投递结果。
 * </p>
 */
public class VehicleBridgeException extends RuntimeException {

    public VehicleBridgeException(String message) {
        super(message);
    }

    public VehicleBridgeException(String message, Throwable cause) {
        super(message, cause);
    }
}
