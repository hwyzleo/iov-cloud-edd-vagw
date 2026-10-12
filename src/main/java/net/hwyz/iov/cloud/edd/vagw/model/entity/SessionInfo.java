package net.hwyz.iov.cloud.edd.vagw.model.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SessionInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String deviceSn;  // Primary key (changed from vin)
    private String vin;       // Resolved from binding (enriched for northbound)
    private String clientId;
    private boolean online;
    private Instant connectedAt;
    private Instant disconnectedAt;
    private String node;
    private String sourceIp;
    private Integer protoVer;
    private Instant lastSeen;

    // ---- keyprov 认证会话快照（EDD-VAGW-DSN-CR-011 §3.2/§5，ACL_SESSION_CORRELATED）----
    /** 连接代际：每次成功连接递增，用于 generation guard（重连/会话替换后旧消息 fail-closed）。 */
    private long connectionGeneration;
    /** broker 侧会话标识（当前等于 clientId，非身份来源）。 */
    private String brokerSessionId;
    /** 认证期证书序列号（来自 /mqtt/auth，仅标识元数据，可能为 null）。 */
    private String certSerial;
    /** 认证期证书指纹（如可用，可能为 null）。 */
    private String certFingerprint;

    // ---- 设备上线事件（EDD-VAGW-DSN-CR-012 §3.5）----
    /** 本连接已固化并进入 Outbox 的 DEVICE_ONLINE eventId（用于会话对账与幂等）。 */
    private String lastDeviceOnlineEventId;
}
