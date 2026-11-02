package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Target;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwViamSecurityProperties;
import net.hwyz.iov.cloud.framework.common.constant.ServiceNameConstants;
import net.hwyz.iov.cloud.sec.viam.api.security.ViamServiceAuthHeaders;
import net.hwyz.iov.cloud.sec.viam.api.security.ViamServiceSignature;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.UUID;

/**
 * VAGW → VIAM 服务间签名请求拦截器（SEC-VIAM-DSN-CR-001 / G-4，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * 对官方 {@link ServiceNameConstants#SEC_VIAM} 服务的全部 Feign 调用（decide/resolve/Keyprov/
 * access-invalidation receipt）附加 {@code X-Viam-*} 签名头：调用方身份 + keyId + audience +
 * method + 规范化 path + body digest + timestamp + nonce + HMAC-SHA256 签名。VIAM 验签端
 * fail-closed（403），VAGW 不得绕过或静默降级。
 * </p>
 * <p>
 * Secret 经环境变量注入且仅存在于本拦截器内存（每次调用解码缓存），不写入日志/异常/持久化；
 * nonce 每次调用新生成（防重放）；timestamp 使用当前 epoch 毫秒（VIAM 有效窗口默认 300s）。
 * 仅作用于 VIAM 服务（{@code sec-viam}），不影响其它 Feign 客户端；不定义第二套
 * {@code @FeignClient}、URL、服务名常量或签名协议。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.security.viam-service-identity", name = "enabled",
        havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class ViamServiceCallerAuthInterceptor implements RequestInterceptor {

    private final VagwViamSecurityProperties properties;

    @Override
    public void apply(RequestTemplate template) {
        Target<?> target = template.feignTarget();
        if (target == null || !ServiceNameConstants.SEC_VIAM.equals(target.name())) {
            return;
        }
        if (!properties.isEnabled()) {
            return;
        }
        String clientId = properties.getClientId();
        String keyId = properties.getKeyId();
        String audience = properties.getAudience();
        String method = template.method() == null ? "" : template.method();
        String path = template.path() == null ? "" : template.path();
        byte[] body = template.body();
        String bodyDigest = sha256Hex(body == null ? new byte[0] : body);
        String timestamp = Long.toString(System.currentTimeMillis());
        String nonce = UUID.randomUUID().toString().replace("-", "");

        String canonical = ViamServiceSignature.canonicalString(
                clientId, keyId, audience, method, path, bodyDigest, timestamp, nonce);
        String signature = ViamServiceSignature.sign(properties.secretBytes(), canonical);

        template.header(ViamServiceAuthHeaders.CLIENT_ID, clientId);
        template.header(ViamServiceAuthHeaders.KEY_ID, keyId);
        template.header(ViamServiceAuthHeaders.AUDIENCE, audience);
        template.header(ViamServiceAuthHeaders.METHOD, method.toLowerCase(Locale.ROOT));
        template.header(ViamServiceAuthHeaders.PATH, path);
        template.header(ViamServiceAuthHeaders.BODY_DIGEST, bodyDigest);
        template.header(ViamServiceAuthHeaders.TIMESTAMP, timestamp);
        template.header(ViamServiceAuthHeaders.NONCE, nonce);
        template.header(ViamServiceAuthHeaders.SIGNATURE, signature);
        template.header(ViamServiceAuthHeaders.ALGORITHM, ViamServiceSignature.ALGORITHM_V1);

        if (log.isDebugEnabled()) {
            // 仅输出身份/路径/摘要指纹，不输出 Secret、nonce、timestamp 或 body
            log.debug("VIAM 服务间签名已附加: clientId={}, keyId={}, method={}, path={}, bodyDigestFp={}",
                    clientId, keyId, method, path, digestFingerprint(bodyDigest));
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String digestFingerprint(String digest) {
        if (digest == null || digest.isBlank()) {
            return "<missing>";
        }
        String value = digest.trim();
        return value.length() <= 12 ? value : value.substring(0, 12) + "…";
    }
}
