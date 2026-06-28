package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.iov.tsp.api.service.TspDeviceAdmissionService;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionCheckVo;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionResultVo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 认证ACL服务实现
 * 调用TSP设备接入裁决接口进行统一准入判断
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthAclServiceImpl implements AuthAclService {

    private static final Pattern DEVICE_SN_PATTERN = Pattern.compile("^[A-Z0-9\\-]{1,64}$");
    private static final String TOPIC_PREFIX = "vehicle/";
    private static final String ADMISSION_ALLOW = "ALLOW";

    private final TspDeviceAdmissionService tspDeviceAdmissionService;

    @Override
    public AuthResult authenticate(String deviceSn, String clientId, String certSerial) {
        if (deviceSn == null || deviceSn.isBlank()) {
            log.warn("Auth failed: empty device_sn");
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Missing device_sn");
        }

        String normalizedDeviceSn = deviceSn.toUpperCase();
        if (!DEVICE_SN_PATTERN.matcher(normalizedDeviceSn).matches()) {
            log.warn("Auth failed: invalid device_sn format: {}", deviceSn);
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Invalid device_sn format");
        }

        // 构建TSP裁决请求
        DeviceAdmissionCheckVo checkVo = DeviceAdmissionCheckVo.builder()
                .hsm(normalizedDeviceSn)
                .certSerial(certSerial)
                .clientId(clientId)
                .build();

        try {
            // 调用TSP设备接入裁决接口
            DeviceAdmissionResultVo result = tspDeviceAdmissionService.checkDeviceAdmission(checkVo);

            if (ADMISSION_ALLOW.equals(result.getAdmission())) {
                String vin = result.getVin();
                log.info("Auth success: deviceSn={}, vin={}", normalizedDeviceSn, vin);

                // 构建ACL规则
                List<MqttAuthResponse.AclRule> acl = buildAcl(normalizedDeviceSn);
                return AuthResult.allow(acl, normalizedDeviceSn, vin);
            } else {
                // 映射TSP reason到网关错误码
                ErrorCode errorCode = mapReasonToErrorCode(result.getReason());
                log.warn("Auth denied: deviceSn={}, reason={}, errorCode={}",
                        normalizedDeviceSn, result.getReason(), errorCode);
                return AuthResult.deny(errorCode, result.getReason());
            }
        } catch (Exception e) {
            // TSP服务不可用时fail-closed
            log.error("Auth failed: TSP service error for deviceSn={}", normalizedDeviceSn, e);
            return AuthResult.deny(ErrorCode.DEPENDENCY_UNAVAILABLE, "TSP service error");
        }
    }

    /**
     * 映射TSP裁决原因到网关错误码
     */
    private ErrorCode mapReasonToErrorCode(String reason) {
        if (reason == null) {
            return ErrorCode.DEPENDENCY_UNAVAILABLE;
        }

        return switch (reason) {
            case "UID_UNKNOWN", "UNBOUND" -> ErrorCode.DEVICE_UNKNOWN;
            case "CERT_REVOKED", "CERT_EXPIRED", "DEVICE_RETIRED", "DEVICE_BLOCKED" -> ErrorCode.DEVICE_BLOCKED;
            case "DEPENDENCY_UNAVAILABLE" -> ErrorCode.DEPENDENCY_UNAVAILABLE;
            default -> ErrorCode.DEVICE_BLOCKED;
        };
    }

    /**
     * 构建ACL规则
     * Topic命名空间使用device_sn
     */
    private List<MqttAuthResponse.AclRule> buildAcl(String deviceSn) {
        String topicNamespace = TOPIC_PREFIX + deviceSn + "/#";
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
}
