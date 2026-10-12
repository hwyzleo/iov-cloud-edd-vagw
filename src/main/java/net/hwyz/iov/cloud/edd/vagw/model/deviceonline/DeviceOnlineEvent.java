package net.hwyz.iov.cloud.edd.vagw.model.deviceonline;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * 设备上线事件契约（EDD-VAGW-DSN-CR-012 §3.3 / REQ-CR-012 §4）。
 * <p>
 * eventType 固定 {@code DEVICE_ONLINE}；hsmUid 为规范接入身份；VIN 来自本次 TSP ALLOW 快照；
 * tboxSn 为可选资产字段，无可靠来源时不填；可选字段缺失时省略（不以空串代替未知值）；
 * payload 不含密钥材料、Wrapped Key、私钥或 VMD 密钥目录字段。
 * </p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DeviceOnlineEvent(
        String eventId,
        String eventType,
        String hsmUid,
        String vin,
        String sessionId,
        long connectionGeneration,
        String certSerial,
        String tboxSn,
        Instant occurredAt,
        String timeSource) {

    public static final String TYPE = "DEVICE_ONLINE";
    /** occurredAt 来源：EMQX 连接建立时间 */
    public static final String TIME_SOURCE_EMQX = "EMQX";
    /** occurredAt 来源：VAGW 接收时间（EMQX 时间缺失或明显越界） */
    public static final String TIME_SOURCE_VAGW_RECEIVED = "VAGW_RECEIVED";
}
