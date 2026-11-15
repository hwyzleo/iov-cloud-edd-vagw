package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 接入身份绑定解析可观测计数器（EDD-VAGW-DSN-CR-020 §9）。
 * <p>
 * 与 {@link VagwMqttSubscriptionMetrics} 一致：项目未引入 micrometer，先以内存计数器 +
 * 结构化日志承载，接入 Prometheus 时按同名 metric 透传，进程重启允许清零。
 * </p>
 * <p>
 * 指标：{@code vagw_binding_cache_total{direction,result}}、
 * {@code vagw_binding_uplink_resolve_total{result,reason}}、
 * {@code vagw_binding_session_renew_total}、{@code vagw_binding_session_clear_total}、
 * {@code vagw_binding_uplink_resolve_overloaded_total}、
 * {@code vagw_binding_uplink_resolve_deadline_exceeded_total}。
 * </p>
 * <p>
 * 禁止把 hsmUid、VIN、device_sn 或 payload 作为 label；日志只输出脱敏摘要。
 * 常态下 {@code binding_context_missing} 应为 0，非 0 即明确故障。
 * </p>
 */
@Slf4j
@Component
public class BindingResolutionMetrics {

    public static final String REASON_CONTEXT_MISSING = "context_missing";

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    private void increment(String name) {
        counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    }

    /** 绑定缓存命中/未命中计数（direction=hsm|vin）。 */
    public void cache(String direction, boolean hit) {
        increment("vagw_binding_cache_total{direction=" + direction + ",result="
                + (hit ? "hit" : "miss") + "}");
    }

    /** 上行绑定解析结果计数；reason 为有界枚举，不含身份信息。 */
    public void uplinkResolve(String result, String reason) {
        increment("vagw_binding_uplink_resolve_total{result=" + result
                + ",reason=" + (reason == null ? "none" : reason) + "}");
        if (REASON_CONTEXT_MISSING.equals(reason)) {
            // 语义收缩后常态应为 0：出现即代表无法获得且无法重建绑定上下文
            log.warn("SEC-BINDING | uplink binding context missing (should stay 0 in steady state)");
        }
    }

    /** 上行回源被本地过载拒绝（未调用 VIAM）。 */
    public void uplinkResolveOverloaded() {
        increment("vagw_binding_uplink_resolve_overloaded_total");
        log.warn("SEC-BINDING | uplink binding resolve overloaded (permit exhausted, VIAM not called)");
    }

    /** 上行回源超出 deadline（VIAM 慢响应或依赖抖动）。 */
    public void uplinkResolveDeadlineExceeded() {
        increment("vagw_binding_uplink_resolve_deadline_exceeded_total");
        log.warn("SEC-BINDING | uplink binding resolve deadline exceeded");
    }

    /** 会话期映射续期次数。 */
    public void sessionRenewed() {
        increment("vagw_binding_session_renew_total");
    }

    /** 断连清理会话级映射次数。 */
    public void sessionCleared() {
        increment("vagw_binding_session_clear_total");
    }

    public long get(String name) {
        AtomicLong value = counters.get(name);
        return value == null ? 0L : value.get();
    }
}
