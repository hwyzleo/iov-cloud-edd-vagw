package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 认证安全审计计数器（EDD-VAGW-DSN-CR-009 §6）。
 * <p>
 * 当前项目未引入 micrometer/actuator，先以内存计数器 + 结构化日志承载可观测性；
 * 接入 Prometheus 时按同名 metric 透传。日志仅输出脱敏摘要，不输出 payload。
 * </p>
 */
@Slf4j
@Component
public class AuthSecurityMetrics {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    private void increment(String name) {
        counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    }

    /** 证书 CN 缺失（无 peer cert） */
    public void incPeerCertMissing() {
        increment("mqtt_auth_peer_cert_missing_total");
        log.warn("SEC-AUTH | peer cert CN missing");
    }

    /** username 与证书 CN 不一致 / 声明缺失 */
    public void incIdentityMismatch() {
        increment("mqtt_auth_identity_mismatch_total");
        log.warn("SEC-AUTH | identity mismatch (username != peer cert CN)");
    }

    /** 证书身份格式非法 */
    public void incInvalidCertIdentity() {
        increment("mqtt_auth_invalid_cert_identity_total");
        log.warn("SEC-AUTH | invalid cert identity format");
    }

    /** EMQX HTTP authn 占位符残留（配置了 EMQX 不支持的占位符，原样透传） */
    public void incPlaceholderLeak(String field) {
        increment("mqtt_auth_placeholder_leak_total{field=" + field + "}");
        log.warn("SEC-AUTH | EMQX placeholder leaked, field={}", field);
    }

    /** VAGW 超级用户 bypass 被拒（按因子） */
    public void incBypassDenied(String factor) {
        increment("mqtt_auth_bypass_denied_total{factor=" + factor + "}");
        log.warn("SEC-AUTH | superuser bypass denied, factor={}", factor);
    }

    /** VAGW 超级用户 bypass 放行 */
    public void incBypassAllowed() {
        increment("mqtt_auth_bypass_allowed_total");
    }

    /** 认证拒绝（按错误码） */
    public void incDeny(String code) {
        increment("mqtt_auth_deny_total{reason=" + code + "}");
    }

    public long get(String name) {
        AtomicLong value = counters.get(name);
        return value == null ? 0L : value.get();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
}
