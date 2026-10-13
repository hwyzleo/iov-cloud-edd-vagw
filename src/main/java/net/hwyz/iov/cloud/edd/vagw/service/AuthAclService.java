package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;

import java.util.List;

/**
 * 认证ACL服务接口
 */
public interface AuthAclService {

    /**
     * 认证设备并返回ACL。
     * <p>
     * 调用方（MqttAuthController）已完成 username==peer_cert_cn 与 CN 格式校验，并把本次
     * mTLS 叶子证书 PEM（EMQX {@code ${cert_pem}}）与可信来源 IP 透传；本服务在调用 TSP 前
     * 构造叶子证书上下文（EDD-VAGW-DSN-CR-013 §4.2/§6），任一异常 fail-closed。
     * </p>
     *
     * @param deviceSn    已验证证书 CN 的归一化值（EMQX peer_cert_cn，禁止回退 MQTT username）
     * @param clientId    MQTT 客户端 ID
     * @param peerCertPem 本次 mTLS 叶子证书 PEM（仅本次调用内存使用）
     * @param sourceIp    可信来源 IP（EMQX peerhost，仅用于 TSP 审计）
     * @return 认证结果
     */
    AuthResult authenticate(String deviceSn, String clientId, String peerCertPem, String sourceIp);

    /**
     * 认证结果记录。
     *
     * @param certSerial 由叶子证书解析的 canonical serial（ALLOW 时非空），供既有
     *                   AdmissionSessionContext/AuthenticatedIdentityStore 契约受控保存
     */
    record AuthResult(boolean allowed, ErrorCode errorCode, String reason,
                      List<MqttAuthResponse.AclRule> acl, String deviceSn, String vin, String certSerial) {

        public static AuthResult allow(List<MqttAuthResponse.AclRule> acl, String deviceSn, String vin,
                                       String certSerial) {
            return new AuthResult(true, null, null, acl, deviceSn, vin, certSerial);
        }

        public static AuthResult deny(ErrorCode code, String reason) {
            return new AuthResult(false, code, reason, null, null, null, null);
        }
    }
}
