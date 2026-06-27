package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthAclService {

    private static final Pattern DEVICE_SN_PATTERN = Pattern.compile("^[A-Z0-9\\-]{1,64}$");
    
    private final BindingService bindingService;

    /**
     * 认证设备并返回 ACL
     * @param deviceSn 设备序列号（来自证书 CN）
     * @param clientId MQTT 客户端 ID
     * @return 认证结果
     */
    public AuthResult authenticate(String deviceSn, String clientId) {
        if (deviceSn == null || deviceSn.isBlank()) {
            log.warn("Auth failed: empty device_sn");
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Missing device_sn");
        }

        String normalizedDeviceSn = deviceSn.toUpperCase();
        if (!DEVICE_SN_PATTERN.matcher(normalizedDeviceSn).matches()) {
            log.warn("Auth failed: invalid device_sn format: {}", deviceSn);
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Invalid device_sn format");
        }

        // Resolve device_sn → VIN binding
        Optional<String> vinOpt = bindingService.resolveVin(normalizedDeviceSn);
        if (vinOpt.isEmpty()) {
            log.warn("Auth failed: device_sn not bound to any VIN: {}", normalizedDeviceSn);
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "device_sn not bound");
        }
        
        String vin = vinOpt.get();
        log.info("Auth success: deviceSn={}, vin={}", normalizedDeviceSn, vin);

        // Build ACL based on device_sn namespace
        List<MqttAuthResponse.AclRule> acl = buildAcl(normalizedDeviceSn);
        return AuthResult.allow(acl, normalizedDeviceSn, vin);
    }

    /**
     * 构建 ACL 规则
     * Topic 命名空间使用 device_sn
     */
    private List<MqttAuthResponse.AclRule> buildAcl(String deviceSn) {
        String topicNamespace = "vehicle/" + deviceSn + "/#";
        return List.of(
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("publish")
                        .topic(topicNamespace)
                        .build(),
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("subscribe")
                        .topic(topicNamespace)
                        .build()
        );
    }

    public record AuthResult(boolean allowed, ErrorCode errorCode, String reason,
                              List<MqttAuthResponse.AclRule> acl, String deviceSn, String vin) {
        public static AuthResult allow(List<MqttAuthResponse.AclRule> acl, String deviceSn, String vin) {
            return new AuthResult(true, null, null, acl, deviceSn, vin);
        }

        public static AuthResult deny(ErrorCode code, String reason) {
            return new AuthResult(false, code, reason, null, null, null);
        }
    }
}
