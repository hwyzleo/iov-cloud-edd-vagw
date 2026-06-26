package net.hwyz.iov.cloud.edd.vagw.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MqttEventRequest {
    @JsonProperty("event")
    private String event;  // "client.connected" or "client.disconnected"

    @JsonProperty("username")
    private String username;  // VIN

    @JsonProperty("clientid")
    private String clientId;

    @JsonProperty("ts")
    private Long timestamp;

    @JsonProperty("peer_host")
    private String peerHost;

    @JsonProperty("proto_ver")
    private Integer protoVer;

    @JsonProperty("mountpoint")
    private String mountpoint;
}
