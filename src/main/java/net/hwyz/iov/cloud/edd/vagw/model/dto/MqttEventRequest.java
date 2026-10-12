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

    /** EMQX 会话标识（client.connected 事件透传；缺失时以 clientId 作为稳定输入） */
    @JsonProperty("sessionid")
    private String sessionId;

    /** EMQX 节点标识（事件来源节点，用于稳定 eventId 与来源审计） */
    @JsonProperty("node")
    private String node;

    @JsonProperty("ts")
    private Long timestamp;

    @JsonProperty("peer_host")
    private String peerHost;

    @JsonProperty("proto_ver")
    private Integer protoVer;

    @JsonProperty("mountpoint")
    private String mountpoint;
}
