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
}
