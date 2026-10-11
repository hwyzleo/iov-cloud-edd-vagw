package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.iov.tsp.api.service.TspDeviceAdmissionService;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionCheckVo;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionResultVo;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

    /**
     * 设备可用业务 service 白名单（EDD-VAGW-DSN-CR-011 §4，方向敏感显式 ACL，不再使用 vehicle/{cn}/#）。
     * keyprov 仅在 enabled 且契约兼容时追加，保证关闭时不授 keyprov ACL 且不影响其他 service。
     */
    private static final List<String> DEVICE_SERVICES = List.of("fota", "remotecontrol", "remotecontrol_ack");

    private final TspDeviceAdmissionService tspDeviceAdmissionService;
    private final BindingService bindingService;
    private final KeyprovProperties keyprovProperties;
    private final KeyprovContractGuard keyprovContractGuard;

    @Override
    public AuthResult authenticate(String deviceSn, String clientId, String certSerial) {
        if (deviceSn == null || deviceSn.isBlank()) {
            log.warn("Auth failed: empty device_sn");
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Missing device_sn");
        }

        String normalizedDeviceSn = deviceSn.toUpperCase(Locale.ROOT);
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

                // 准入成功后建立本次会话 hsm_uid ↔ VIN 双向绑定（EDD-VAGW-DSN-CR-007 §4.2），
                // 上行不依赖外部预热 Redis；bindingVersion 准入接口不返回，由 TSP 回源补齐
                if (vin != null && !vin.isBlank()) {
                    bindingService.rememberAdmission(normalizedDeviceSn, vin, null);
                } else {
                    log.warn("Auth allowed but TSP admission returned blank vin, binding not remembered: deviceSn={}",
                            normalizedDeviceSn);
                }

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
     * 构建方向敏感的显式 ACL 白名单（EDD-VAGW-DSN-CR-011 §4）。
     * <p>
     * 设备侧只允许 publish {@code vehicle/{cn}/up/{service}} 与 subscribe
     * {@code vehicle/{cn}/down/{service}}；keyprov 仅在 keyprov.enabled=true 且 proto-keyprov
     * 契约兼容时追加，关闭/不兼容时不返回 keyprov ACL（EMQX 对下行订阅返回 SUBACK failure），
     * 且其他 service ACL 不受影响。
     * </p>
     */
    private List<MqttAuthResponse.AclRule> buildAcl(String deviceSn) {
        List<MqttAuthResponse.AclRule> rules = new ArrayList<>();
        for (String service : DEVICE_SERVICES) {
            rules.add(aclRule("publish", TOPIC_PREFIX + deviceSn + "/up/" + service));
            rules.add(aclRule("subscribe", TOPIC_PREFIX + deviceSn + "/down/" + service));
        }
        if (keyprovProperties.isEnabled() && keyprovContractGuard.compatible()) {
            rules.add(aclRule("publish", TOPIC_PREFIX + deviceSn + "/up/keyprov"));
            rules.add(aclRule("subscribe", TOPIC_PREFIX + deviceSn + "/down/keyprov"));
        }
        return rules;
    }

    private static MqttAuthResponse.AclRule aclRule(String action, String topic) {
        return MqttAuthResponse.AclRule.builder()
                .permission("allow")
                .action(action)
                .topic(topic)
                .build();
    }
}
