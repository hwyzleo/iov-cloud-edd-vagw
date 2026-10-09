package net.hwyz.iov.cloud.edd.vagw.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.service.AuthAclService;
import net.hwyz.iov.cloud.edd.vagw.service.AuthSecurityMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SuperuserBypassGuard;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * MQTT 认证控制面（EMQX HTTP authn hook）。
 * <p>
 * 身份口径（EDD-VAGW-DSN-CR-009）：权威身份 = peer_cert_cn（EMQX 已验证 mTLS 证书 CN）；
 * username 仅作客户端声明值参与一致性校验，缺失或不一致一律 fail-closed，绝不信任客户端自报身份。
 * </p>
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class MqttAuthController {

    /**
     * 证书 CN 格式约束，与 {@code AuthAclServiceImpl.DEVICE_SN_PATTERN} 保持一致。
     * 归一化后不匹配即拒绝（EDD-VAGW-DSN-CR-009 §2.5），也是对 PKI 证书 CN 签发规范的约束反馈。
     */
    private static final Pattern DEVICE_SN_PATTERN = Pattern.compile("^[A-Z0-9\\-]{1,64}$");

    private final AuthAclService authAclService;
    private final SuperuserBypassGuard bypassGuard;
    private final AuthSecurityMetrics metrics;

    @PostMapping("/mqtt/auth")
    public ResponseEntity<MqttAuthResponse> authenticate(@RequestBody MqttAuthRequest request) {
        // ---- 1. VAGW 自身受信连接 bypass（EDD-VAGW-DSN-CR-009 §2.6/§3/§5）----
        // 四因子 AND 校验由 SuperuserBypassGuard 完成；任一因子缺失/无法验证即 fail-closed，
        // 绝不降级为仅凭 clientId 前缀放行（原实现存在外部客户端伪造前缀提权风险，已移除）。
        if (bypassGuard.isBypassAllowed(request)) {
            log.info("VAGW self connection allowed (superuser bypass): clientId={}", request.getClientId());
            metrics.incBypassAllowed();
            return ResponseEntity.ok(MqttAuthResponse.builder()
                    .result("allow")
                    .isSuperuser(true)
                    .acl(Collections.emptyList())
                    .build());
        }

        // ---- 2. TBOX 设备认证 ----
        String certCn = request.getPeerCertCn();
        String claimedUsername = request.getUsername();
        // 占位符残留防御：EMQX HTTP 认证器不支持的占位符（如 ${ssl_cert_serial_number}）不会被替换、
        // 而是原样透传到 VAGW。这类值既非真实数据、也不该进入日志与 TSP 准入请求；
        // 检测到 "${" 即判定为 EMQX 配置错误：置空（fail-safe）+ SEC-AUTH 审计，绝不当作真实值下传。
        String certSerial = sanitizeEmqxPlaceholder(request.getPeerCertSerial(), "peer_cert_serial");

        // 2a. 证书身份缺失 → fail-closed（需求 US-001：无 peer cert 拒绝接入）
        if (certCn == null || certCn.isBlank()) {
            log.warn("Auth denied: peer cert CN missing, claimedUsername={}, clientId={}",
                    LogMask.mask(claimedUsername), request.getClientId());
            metrics.incPeerCertMissing();
            return deny(ErrorCode.AUTH_IDENTITY_MISMATCH, "PEER_CERT_CN_MISSING");
        }
        String deviceSn = normalize(certCn);

        // 2b. 客户端声明身份缺失 → fail-closed
        if (claimedUsername == null || claimedUsername.isBlank()) {
            log.warn("Auth denied: username missing, certCn={}, clientId={}",
                    LogMask.mask(deviceSn), request.getClientId());
            metrics.incIdentityMismatch();
            return deny(ErrorCode.AUTH_IDENTITY_MISMATCH, "USERNAME_MISSING");
        }

        // 2c. 声明身份与证书身份不一致 → fail-closed + 安全审计
        if (!deviceSn.equals(normalize(claimedUsername))) {
            log.warn("Auth denied: username != peer cert CN, claimedUsername={}, certCn={}, clientId={}",
                    LogMask.mask(claimedUsername), LogMask.mask(deviceSn), request.getClientId());
            metrics.incIdentityMismatch();
            return deny(ErrorCode.AUTH_IDENTITY_MISMATCH, "IDENTITY_MISMATCH");
        }

        // 2d. 证书身份格式约束 → fail-closed（EDD-VAGW-DSN-CR-009 §2.5）
        if (!DEVICE_SN_PATTERN.matcher(deviceSn).matches()) {
            log.warn("Auth denied: invalid cert identity format, certCn={}, clientId={}",
                    LogMask.mask(deviceSn), request.getClientId());
            metrics.incInvalidCertIdentity();
            return deny(ErrorCode.AUTH_IDENTITY_MISMATCH, "INVALID_CERT_IDENTITY");
        }

        log.info("MQTT auth request: deviceSn={}, clientId={}, certSerial={}",
                LogMask.mask(deviceSn), request.getClientId(), certSerial);

        AuthAclService.AuthResult result = authAclService.authenticate(
                deviceSn, request.getClientId(), certSerial);

        if (result.allowed()) {
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("allow")
                    .isSuperuser(false)
                    .acl(result.acl())
                    .build();
            log.info("Auth allowed: deviceSn={}, vin={}", LogMask.mask(result.deviceSn()), LogMask.mask(result.vin()));
            return ResponseEntity.ok(response);
        } else {
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("deny")
                    .reason(result.errorCode().getCode() + ": " + result.errorCode().getMessage())
                    .build();
            log.warn("Auth denied: deviceSn={}, reason={}", LogMask.mask(deviceSn), result.reason());
            metrics.incDeny(String.valueOf(result.errorCode().getCode()));
            return ResponseEntity.ok(response);
        }
    }

    /**
     * EMQX HTTP authn 占位符残留防御（EDD-VAGW-DSN-CR-009 §2 扩展）：
     * EMQX 配置里不支持的占位符（如 ${cn}、${ssl_cert_serial_number}）不会被替换、而是原样透传。
     * 检测到 "${" 即判定为 EMQX 配置错误：返回 null（fail-safe）并触发 SEC-AUTH 审计，
     * 绝不把占位符残留当真实值写入日志或传给 TSP 准入。
     */
    private String sanitizeEmqxPlaceholder(String value, String fieldName) {
        if (value == null || value.isBlank() || !value.contains("${")) {
            return value;
        }
        metrics.incPlaceholderLeak(fieldName);
        return null;
    }

    /**
     * 归一化：去除首尾空白后按 Locale.ROOT 转为大写；不做模糊匹配/截断/fallback。
     */
    private static String normalize(String value) {
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private ResponseEntity<MqttAuthResponse> deny(ErrorCode code, String reason) {
        metrics.incDeny(String.valueOf(code.getCode()));
        return ResponseEntity.ok(MqttAuthResponse.builder()
                .result("deny")
                .reason(code.getCode() + ": " + reason)
                .build());
    }
}
