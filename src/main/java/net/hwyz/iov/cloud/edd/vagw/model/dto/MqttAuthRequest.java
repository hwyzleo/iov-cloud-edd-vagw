package net.hwyz.iov.cloud.edd.vagw.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MqttAuthRequest {
    @JsonProperty("username")
    private String username;  // = CN(VIN)

    @JsonProperty("clientid")
    private String clientId;

    @JsonProperty("peer_host")
    private String peerHost;

    @JsonProperty("peer_port")
    private Integer peerPort;

    @JsonProperty("proto_ver")
    private Integer protoVer;

    @JsonProperty("mountpoint")
    private String mountpoint;
}
