package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAdmissionProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.security.PeerCertificateContextFactory;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.security.PeerCertificateException;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.model.security.VerifiedPeerCertificate;
import net.hwyz.iov.cloud.iov.tsp.api.service.TspDeviceAdmissionService;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionCheckVo;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionResultVo;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.AdmissionDecision;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.AdmissionReason;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.BindStatus;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.CertificateStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 认证ACL服务实现（EDD-VAGW-DSN-CR-009 / EDD-VAGW-DSN-CR-013）。
 * <p>
 * 在调用 TSP 准入裁决前，由 {@link PeerCertificateContextFactory} 从 EMQX 透传的叶子证书 PEM
 * 构造瞬时证书上下文（serial/fingerprint/Base64 DER），任一异常 fail-closed；
 * 适配 TSP-DSN-CR-011 canonical {@code /decide} 与新 VO 枚举。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthAclServiceImpl implements AuthAclService {

    private static final Pattern DEVICE_SN_PATTERN = Pattern.compile("^[A-Z0-9\\-]{1,64}$");
    private static final String TOPIC_PREFIX = "vehicle/";

    /**
     * 设备可用业务 service 白名单（EDD-VAGW-DSN-CR-011 §4，方向敏感显式 ACL，不再使用 vehicle/{cn}/#）。
     * keyprov 仅在 enabled 且契约兼容时追加，保证关闭时不授 keyprov ACL 且不影响其他 service。
     */
    private static final List<String> DEVICE_SERVICES = List.of("fota", "remotecontrol", "remotecontrol_ack");

    private final TspDeviceAdmissionService tspDeviceAdmissionService;
    private final BindingService bindingService;
    private final KeyprovProperties keyprovProperties;
    private final KeyprovContractGuard keyprovContractGuard;
    private final PeerCertificateContextFactory peerCertificateContextFactory;
    private final VagwAdmissionProperties admissionProperties;
    private final AuthSecurityMetrics metrics;

    @Override
    public AuthResult authenticate(String deviceSn, String clientId, String peerCertPem, String sourceIp) {
        if (deviceSn == null || deviceSn.isBlank()) {
            log.warn("Auth failed: empty device_sn");
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Missing device_sn");
        }

        String normalizedDeviceSn = deviceSn.toUpperCase(Locale.ROOT);
        if (!DEVICE_SN_PATTERN.matcher(normalizedDeviceSn).matches()) {
            log.warn("Auth failed: invalid device_sn format");
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Invalid device_sn format");
        }

        // 发布编排门禁（EDD-VAGW-DSN-CR-013 §7）：required=false 时准入 NOT_READY/拒绝，
        // 绝不回退到仅凭 CN、跳过叶子证书的旧允许路径。
        if (!admissionProperties.getPeerCertificate().isRequired()) {
            log.error("SEC-AUTH | device admission NOT_READY: vagw.admission.peer-certificate.required=false");
            metrics.incPeerCertificateValidation("ADMISSION_NOT_READY");
            return AuthResult.deny(ErrorCode.AUTH_IDENTITY_MISMATCH, "ADMISSION_NOT_READY");
        }

        // 调用 TSP 前构造证书上下文：缺失/畸形/超限/多证书/私钥/CN 不一致均 fail-closed（不调用 TSP）
        VerifiedPeerCertificate certificate;
        try {
            certificate = peerCertificateContextFactory.create(peerCertPem, normalizedDeviceSn);
        } catch (PeerCertificateException e) {
            metrics.incPeerCertificateValidation(e.getOutcome());
            log.warn("Auth denied: peer certificate context invalid, deviceSn={}, outcome={}",
                    LogMask.mask(normalizedDeviceSn), e.getOutcome());
            return AuthResult.deny(ErrorCode.AUTH_IDENTITY_MISMATCH, e.getOutcome());
        }

        DeviceAdmissionCheckVo checkVo = DeviceAdmissionCheckVo.builder()
                .uid(certificate.normalizedUid())
                .certSerial(certificate.canonicalSerial())
                .certFingerprint(certificate.sha256Fingerprint())
                .leafCertificateDer(Base64.getEncoder().encodeToString(certificate.der()))
                .clientId(clientId)
                .sourceIp(sourceIp)
                .ts(Instant.now())
                .build();

        try {
            long startedAt = System.nanoTime();
            DeviceAdmissionResultVo result = tspDeviceAdmissionService.decide(checkVo);
            metrics.recordTspAdmissionLatency((System.nanoTime() - startedAt) / 1_000_000L);
            return evaluate(result, normalizedDeviceSn, certificate.canonicalSerial());
        } catch (Exception e) {
            // TSP 超时/网络异常/5xx/反序列化失败 → fail-closed，保留 cause 供受控诊断（不落证书正文）
            metrics.incTspAdmission("DENY", AdmissionReason.DEPENDENCY_UNAVAILABLE.name());
            log.error("Auth failed: TSP admission error for deviceSn={}", LogMask.mask(normalizedDeviceSn), e);
            return AuthResult.deny(ErrorCode.DEVICE_BLOCKED, AdmissionReason.DEPENDENCY_UNAVAILABLE.name());
        }
    }

    /**
     * 裁决结果映射（EDD-VAGW-DSN-CR-013 §5）：
     * UID_UNKNOWN/UNBOUND/BINDING_CONFLICT → 804001；其余 DENY/依赖不可用/响应非法 → 804002。
     */
    private AuthResult evaluate(DeviceAdmissionResultVo result, String deviceSn, String canonicalSerial) {
        if (result == null || result.getDecision() == null) {
            return denyDependency(deviceSn, "INVALID_ADMISSION_RESPONSE");
        }

        if (result.getDecision() == AdmissionDecision.ALLOW) {
            // ALLOW 必要状态字段完整性校验：缺失即视为响应非法，fail-closed
            if (result.getReason() != AdmissionReason.NONE
                    || isBlank(result.getVin())
                    || result.getBindStatus() != BindStatus.BOUND
                    || result.getCertStatus() != CertificateStatus.VALID
                    || result.getDeviceStatus() == null
                    || result.getBindVersion() == null
                    || result.getUpdatedAt() == null) {
                return denyDependency(deviceSn, "INCOMPLETE_ADMISSION_ALLOW");
            }

            metrics.incTspAdmission("ALLOW", AdmissionReason.NONE.name());
            // 准入成功后建立本次会话 hsm_uid ↔ VIN 双向绑定（EDD-VAGW-DSN-CR-007 §4.2），
            // 版本号由 TSP-DSN-CR-011 准入接口返回
            bindingService.rememberAdmission(deviceSn, result.getVin(), result.getBindVersion());
            List<MqttAuthResponse.AclRule> acl = buildAcl(deviceSn);
            log.info("Auth success: deviceSn={}, vin={}", LogMask.mask(deviceSn), LogMask.mask(result.getVin()));
            return AuthResult.allow(acl, deviceSn, result.getVin(), canonicalSerial);
        }

        AdmissionReason reason = result.getReason();
        ErrorCode errorCode = mapReasonToErrorCode(reason);
        metrics.incTspAdmission("DENY", reason == null ? "UNKNOWN" : reason.name());
        log.warn("Auth denied: deviceSn={}, reason={}, errorCode={}",
                LogMask.mask(deviceSn), reason, errorCode);
        return AuthResult.deny(errorCode, reason == null ? "UNKNOWN_REASON" : reason.name());
    }

    /**
     * 依赖不可用 / 响应非法 / 未知 reason 统一 804002，不得伪装为证书已吊销或降级为 ALLOW。
     */
    private AuthResult denyDependency(String deviceSn, String detail) {
        metrics.incTspAdmission("DENY", AdmissionReason.DEPENDENCY_UNAVAILABLE.name());
        log.warn("Auth denied: TSP admission unusable, deviceSn={}, detail={}", LogMask.mask(deviceSn), detail);
        return AuthResult.deny(ErrorCode.DEVICE_BLOCKED,
                AdmissionReason.DEPENDENCY_UNAVAILABLE.name() + ":" + detail);
    }

    /**
     * 映射 TSP 裁决原因到网关错误码（EDD-VAGW-DSN-CR-013 §5）。
     */
    private ErrorCode mapReasonToErrorCode(AdmissionReason reason) {
        if (reason == null) {
            return ErrorCode.DEVICE_BLOCKED;
        }
        return switch (reason) {
            case UID_UNKNOWN, UNBOUND, BINDING_CONFLICT -> ErrorCode.DEVICE_UNKNOWN;
            case CERT_REVOKED, CERT_EXPIRED, DEVICE_BLOCKED, DEVICE_RETIRED,
                 DEPENDENCY_UNAVAILABLE, NONE -> ErrorCode.DEVICE_BLOCKED;
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

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
