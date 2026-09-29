package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;

import java.util.List;

/**
 * 认证ACL服务接口
 */
public interface AuthAclService {

    /**
     * 认证设备并返回ACL
     * @param deviceSn 已验证证书 CN 的归一化值（EMQX peer_cert_cn，EDD-VAGW-DSN-CR-009 §2）；
     *                 调用方（MqttAuthController）已强制 username==peer_cert_cn 且通过格式校验，
     *                 本服务禁止回退使用 MQTT username
     * @param clientId MQTT客户端ID
     * @param certSerial 证书序列号
     * @return 认证结果
     */
    AuthResult authenticate(String deviceSn, String clientId, String certSerial);

    /**
     * 认证结果记录
     */
    record AuthResult(boolean allowed, ErrorCode errorCode, String reason,
                      List<MqttAuthResponse.AclRule> acl, String deviceSn, String vin) {

        public static AuthResult allow(List<MqttAuthResponse.AclRule> acl, String deviceSn, String vin) {
            return new AuthResult(true, null, null, acl, deviceSn, vin);
        }

        public static AuthResult deny(ErrorCode code, String reason) {
            return new AuthResult(false, code, reason, null, null, null);
        }
    }
}
