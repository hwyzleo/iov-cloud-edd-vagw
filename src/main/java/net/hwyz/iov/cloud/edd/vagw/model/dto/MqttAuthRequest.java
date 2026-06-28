package net.hwyz.iov.cloud.edd.vagw.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * MQTT认证请求
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MqttAuthRequest {

    /**
     * 用户名（=CN(device_sn)）
     */
    @JsonProperty("username")
    private String username;

    /**
     * 客户端ID（=device_sn）
     */
    @JsonProperty("clientid")
    private String clientId;

    /**
     * 对端证书CN
     */
    @JsonProperty("peer_cert_cn")
    private String peerCertCn;

    /**
     * 对端证书序列号
     */
    @JsonProperty("peer_cert_serial")
    private String peerCertSerial;

    /**
     * 客户端IP地址
     */
    @JsonProperty("peer_host")
    private String peerHost;

    /**
     * 协议版本
     */
    @JsonProperty("proto_ver")
    private Integer protoVer;

    @JsonProperty("mountpoint")
    private String mountpoint;
}
