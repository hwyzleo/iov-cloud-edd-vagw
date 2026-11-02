package net.hwyz.iov.cloud.edd.vagw.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * VAGW → VIAM 服务间签名认证配置（SEC-VIAM-DSN-CR-001 / G-4，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * VIAM {@code /api/service/**} 全部端点强制 HMAC-SHA256 验签（fail-closed 403）；VAGW 作为
 * 调用方必须携带 {@code X-Viam-*} 签名头。Secret 经环境变量注入（Base64，禁止 Nacos/日志/
 * 请求体）；keyId 支持轮换（新签名用 currentKeyId，旧版本共存至滚动完成）。
 * </p>
 * <p>
 * 启动校验（enabled=true）：clientId/keyId/audience 非空、secret 非空且 Base64 可解码；
 * 配置缺失 fail-fast，绝不静默降级为未认证调用。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.security.viam-service-identity")
public class VagwViamSecurityProperties {

    /** 是否启用服务间签名（生产必须 true；仅隔离测试环境可关闭，关闭时不得开放真实安全写入） */
    private boolean enabled = true;

    /** 调用方服务身份（VIAM 授权矩阵 clientId，默认 VAGW） */
    private String clientId = "VAGW";

    /** 当前签名 keyId（与 VIAM secrets.&lt;clientId&gt;.&lt;keyId&gt; 对应） */
    private String keyId = "k1";

    /** 调用方 Secret（Base64；经环境变量注入，禁止 Nacos/日志） */
    private String secret = "";

    /** 目标服务 audience（VIAM 验签固定值，默认 sec-viam） */
    private String audience = "sec-viam";

    /** Base64 解码后的 Secret 字节（启动校验后缓存） */
    private byte[] secretBytes = new byte[0];

    @PostConstruct
    public void validate() {
        if (!enabled) {
            return;
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalStateException(
                    "vagw.security.viam-service-identity.client-id must be configured when enabled");
        }
        if (keyId == null || keyId.isBlank()) {
            throw new IllegalStateException(
                    "vagw.security.viam-service-identity.key-id must be configured when enabled");
        }
        if (audience == null || audience.isBlank()) {
            throw new IllegalStateException(
                    "vagw.security.viam-service-identity.audience must be configured when enabled");
        }
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException(
                    "vagw.security.viam-service-identity.secret must be injected via environment variable "
                            + "(Base64) when enabled; VAGW must not call VIAM unauthenticated");
        }
        try {
            secretBytes = Base64.getDecoder().decode(secret.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "vagw.security.viam-service-identity.secret is not valid Base64", e);
        }
        if (secretBytes.length == 0) {
            throw new IllegalStateException(
                    "vagw.security.viam-service-identity.secret decodes to empty bytes");
        }
    }

    /**
     * Secret 原始字节（启动校验后可用）。
     */
    public byte[] secretBytes() {
        return secretBytes;
    }

    /** Base64 编码 Secret 字符串（禁止 toString/日志输出正文）。 */
    public String secretRef() {
        if (secret == null || secret.isBlank()) {
            return "<unset>";
        }
        // 仅输出长度与尾部标识，不输出正文（与 LogMask 一致的受控摘要）
        return "Base64(len=" + secret.getBytes(StandardCharsets.UTF_8).length + ")";
    }
}
