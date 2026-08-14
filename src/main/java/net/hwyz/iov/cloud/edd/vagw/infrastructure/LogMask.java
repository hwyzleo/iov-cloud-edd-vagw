package net.hwyz.iov.cloud.edd.vagw.infrastructure;

/**
 * 日志脱敏工具，对齐 EDD-VAGW-DSN-CR-005 §9。
 * <p>
 * VAGW 不记录完整 VIN / device_sn / 原始 OTA payload 到普通日志，使用掩码标识。
 * </p>
 */
public final class LogMask {

    private LogMask() {
    }

    /**
     * 对设备/车辆标识做掩码：保留前 2 与后 2 位，中间以 **** 替代。
     * 如 DEVICE00012345 → DE****45。
     */
    public static String mask(String id) {
        if (id == null) {
            return null;
        }
        if (id.length() <= 4) {
            return "****";
        }
        return id.substring(0, 2) + "****" + id.substring(id.length() - 2);
    }

    /**
     * 将 value 中出现的敏感标识（如 device_sn）替换为掩码。
     */
    public static String maskIn(String value, String sensitive) {
        if (value == null || sensitive == null || sensitive.isEmpty()) {
            return value;
        }
        return value.replace(sensitive, mask(sensitive));
    }

    /**
     * PayloadType 摘要（EDD-VAGW-DSN-CR-006 §10）：仅输出消息名摘要，不输出完整类型路径。
     * 如 vehicle.fota.v1.TaskCheckRequest → TaskCheckRequest。
     */
    public static String maskPayloadType(String payloadType) {
        if (payloadType == null || payloadType.isBlank()) {
            return "";
        }
        int idx = payloadType.lastIndexOf('.');
        return idx >= 0 ? payloadType.substring(idx + 1) : payloadType;
    }
}
