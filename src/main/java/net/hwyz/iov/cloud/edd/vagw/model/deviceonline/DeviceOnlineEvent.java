package net.hwyz.iov.cloud.edd.vagw.model.deviceonline;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * 设备上线事件契约（EDD-VAGW-DSN-CR-012 §3.3 / REQ-CR-012 §4，证据字段由 EDD-VAGW-DSN-CR-019 补充）。
 * <p>
 * eventType 固定 {@code DEVICE_ONLINE}；hsmUid 为规范接入身份；VIN 来自本次 VIAM ALLOW 快照；
 * tboxSn 为可选资产字段，无可靠来源时不填；可选字段缺失时省略（不以空串代替未知值）；
 * payload 不含密钥材料、Wrapped Key、私钥或 VIAM 密钥目录字段。
 * </p>
 * <p>
 * FR-019-04 可信证据：{@code admissionDecisionId} 来自 VIAM ALLOW（必填，缺失视为证据不完整，
 * VIAM 侧 EvidenceCheck 拒绝/隔离）；{@code admissionSnapshotExpiresAt/admissionVin/admissionCertSerial}
 * 为不可变准入快照等效证据（VIAM {@code DeviceOnlineEvidenceVo} 增强视图）；历史事件 bytes/eventId 不变，
 * 旧 Outbox 不重编码（CR-019 §4.4）。
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
        String timeSource,
        String admissionDecisionId,
        Instant admissionSnapshotExpiresAt,
        Long bindingVersion,
        String admissionVin,
        String admissionCertSerial) {

    public static final String TYPE = "DEVICE_ONLINE";
    /** occurredAt 来源：EMQX 连接建立时间 */
    public static final String TIME_SOURCE_EMQX = "EMQX";
    /** occurredAt 来源：VAGW 接收时间（EMQX 时间缺失或明显越界） */
    public static final String TIME_SOURCE_VAGW_RECEIVED = "VAGW_RECEIVED";
}
