package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keyprov 专属低基数可观测计数器（EDD-VAGW-DSN-CR-016 §6）。
 * <p>
 * 沿用项目现有做法：内存计数器 + 结构化日志承载可观测性，接入 Prometheus 时按同名 metric
 * 透传；允许随进程重启清零。指标标签不得包含 hsmUid、VIN、requestId、messageId、Topic 或
 * payload；日志仅输出阶段、稳定 reason、时延桶与大小桶。
 * </p>
 * <p>
 * 指标：{@code keyprov_inflight_current}、{@code keyprov_admission_total{result}}、
 * {@code keyprov_executor_active}、{@code keyprov_executor_rejected_total{reason}}、
 * {@code keyprov_vmd_calls_total{result}}、{@code keyprov_vmd_latency}、
 * {@code keyprov_circuit_state}、{@code keyprov_downlink_total{result}}。
 * </p>
 */
@Slf4j
@Component
public class KeyprovMetrics {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicReference<String> readinessReason =
            new java.util.concurrent.atomic.AtomicReference<>();

    private void increment(String name) {
        counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    }

    private AtomicLong gauge(String name) {
        return gauges.computeIfAbsent(name, key -> new AtomicLong());
    }

    /** 当前 in-flight（许可覆盖完整生命周期）。 */
    public void inflightDelta(long delta) {
        AtomicLong value = gauge("keyprov_inflight_current");
        value.addAndGet(delta);
        if (value.get() < 0) {
            value.set(0L);
        }
    }

    /** 准入结果计数（admitted / overloaded）。 */
    public void admission(String result) {
        increment("keyprov_admission_total{result=" + result + "}");
    }

    /** 专属执行器活跃 worker 数 gauge。 */
    public void executorActiveDelta(long delta) {
        AtomicLong value = gauge("keyprov_executor_active");
        value.addAndGet(delta);
        if (value.get() < 0) {
            value.set(0L);
        }
    }

    /** 专属执行器拒绝计数（有界 reason，不携带任务内容）。 */
    public void executorRejected(String reason) {
        increment("keyprov_executor_rejected_total{reason=" + reason + "}");
    }

    /** VMD 调用结果计数（success / rejected / unavailable / timeout / unknown / circuit_open / exception）。 */
    public void vmdCall(String result) {
        increment("keyprov_vmd_calls_total{result=" + result + "}");
    }

    /** VMD 调用时延（毫秒）；只累加与计数，不记录身份。 */
    public void vmdLatency(long latencyMs) {
        increment("keyprov_vmd_latency_count");
        counters.computeIfAbsent("keyprov_vmd_latency_sum", key -> new AtomicLong())
                .addAndGet(Math.max(0L, latencyMs));
    }

    /** 熔断状态 gauge（0=closed, 1=half_open, 2=open）。 */
    public void circuitState(String state) {
        long value = switch (state == null ? "" : state.toLowerCase(java.util.Locale.ROOT)) {
            case "open" -> 2L;
            case "half_open" -> 1L;
            default -> 0L;
        };
        gauge("keyprov_circuit_state").set(value);
    }

    /** 下行发布结果计数（delivered / duplicate / failed / not_delivered）。 */
    public void downlink(String result) {
        increment("keyprov_downlink_total{result=" + result + "}");
    }

    /**
     * Keyprov readiness gauge（低基数）：{@code vagw_keyprov_ready{reason=...}} 1/0。
     * <p>状态变化时把旧 reason 的 gauge 置 0，只保留当前 reason 为 1；不包含设备/租户/requestId。</p>
     */
    public void readiness(boolean ready, String reason) {
        String normalized = reason == null ? "ready" : reason;
        String key = "vagw_keyprov_ready{reason=" + normalized + "}";
        String previous = readinessReason.getAndSet(normalized);
        if (previous != null && !previous.equals(normalized)) {
            AtomicLong old = gauges.get("vagw_keyprov_ready{reason=" + previous + "}");
            if (old != null) {
                old.set(0L);
            }
        }
        gauge(key).set(ready ? 1L : 0L);
    }

    /** 当前 readiness reason（只用于日志/测试，不参与业务判断）。 */
    public String readinessReason() {
        return readinessReason.get();
    }

    public long get(String name) {
        AtomicLong value = counters.get(name);
        return value == null ? 0L : value.get();
    }

    public long gaugeValue(String name) {
        AtomicLong value = gauges.get(name);
        return value == null ? 0L : value.get();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((k, v) -> result.put(k, v.get()));
        gauges.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
}
