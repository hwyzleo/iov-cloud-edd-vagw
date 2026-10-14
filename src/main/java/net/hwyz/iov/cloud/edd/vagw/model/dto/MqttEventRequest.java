package net.hwyz.iov.cloud.edd.vagw.model.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * EMQX 会话事件请求（webhook）。
 * <p>
 * EMQX 不同事件类型（client.connected / client.disconnected）及不同版本会携带额外字段
 * （如 {@code disconnected_at}、{@code receive_maximum}、{@code keepalive}、{@code expiry_interval} 等），
 * 本 DTO 仅消费所需子集，其余字段一律忽略，避免因 EMQX 版本/事件差异导致反序列化失败。
 * </p>
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
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
