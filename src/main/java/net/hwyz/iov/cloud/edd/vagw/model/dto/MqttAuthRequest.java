package net.hwyz.iov.cloud.edd.vagw.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * MQTT认证请求（EMQX HTTP authn hook）
 * <p>
 * 身份口径（EDD-VAGW-DSN-CR-009 / EDD-VAGW-DSN-CR-013 / EDD-VAGW-DSN-CR-014）：权威身份 =
 * peer_cert_cn（EMQX 已验证 mTLS 证书 CN）；peer_cert 为 EMQX {@code ${cert_pem}} 原样值（通常为
 * 叶子证书 DER 的标准 Base64，兼容严格 PEM），供 VAGW 解析 serial/fingerprint/DER；username 仅作
 * 客户端声明值参与一致性校验。
 * </p>
 * <p>
 * peer_cert 与 password 为敏感字段：不参与自动生成的 toString/equals 诊断输出，避免证书正文与
 * 服务凭据进入普通日志。
 * </p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MqttAuthRequest {

    /**
     * 用户名（客户端声明值，仅用于与 peer_cert_cn 一致性校验）
     */
    @JsonProperty("username")
    private String username;

    /**
     * 密码（VAGW 明文账号模式下，EMQX authn hook 透传的服务凭据；常量时间比较，不落日志）
     */
    @JsonProperty("password")
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private String password;

    /**
     * 客户端ID（=device_sn）
     */
    @JsonProperty("clientid")
    private String clientId;

    /**
     * 对端证书CN（EMQX {@code ${cert_common_name}}，权威接入身份）
     */
    @JsonProperty("peer_cert_cn")
    private String peerCertCn;

    /**
     * 对端叶子证书（EMQX {@code ${cert_pem}} 原样值，仅本次调用内存使用）。
     * EMQX 5.8.x 实际为标准 Base64(DER)（名称中的 pem 不代表 BEGIN/END 边界），VAGW 同时兼容严格 PEM；
     * EMQX 5.8.8 HTTP Authentication 不提供 serial 占位符，serial/fingerprint/DER 均由 VAGW 从本字段解析。
     */
    @JsonProperty("peer_cert")
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private String peerCert;

    /**
     * 客户端IP地址（EMQX peerhost，仅作为可信来源 IP 传给 TSP 审计）
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
