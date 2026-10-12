package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 设备上线事件可观测计数器（EDD-VAGW-DSN-CR-012 §6）。
 * <p>
 * 项目当前未引入 micrometer/actuator，先以内存计数器 + 结构化日志承载可观测性，接入
 * Prometheus 时按同名 metric 透传。禁止把 hsmUid、VIN、sessionId 或 certSerial 作为 label；
 * 日志只输出脱敏摘要与身份摘要。
 * </p>
 */
@Slf4j
@Component
public class DeviceOnlineMetrics {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final AtomicLong publishLatencyMs = new AtomicLong(0);
    private final AtomicLong outboxLagSeconds = new AtomicLong(0);
    private final AtomicLong topicReadyGauge = new AtomicLong(0);

    private void increment(String name) {
        counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    }

    /** connected Hook 结果（accepted/duplicate/no-context/...） */
    public void hookResult(String result) {
        increment("vagw_device_online_hook_total{result=" + result + "}");
    }

    /** Outbox 状态写入计数 */
    public void outboxStatus(String status) {
        increment("vagw_device_online_outbox_total{status=" + status + "}");
    }

    /** 发布结果（success/failure/not-ready） */
    public void publishResult(String result) {
        increment("vagw_device_online_publish_total{result=" + result + "}");
    }

    /** 发布失败的连续告警（结构化日志） */
    public void publishFailureAlert(String eventIdDigest, String error) {
        log.error("SEC-DEVICE-ONLINE | publish failure: eventDigest={}, error={}", eventIdDigest, error);
    }

    /** Hook 无准入上下文的告警（激增需告警） */
    public void admissionContextMissing(String result) {
        log.warn("SEC-DEVICE-ONLINE | connected hook without trusted admission context: result={}", result);
    }

    /** 会话对账恢复计数 */
    public void reconcile(String result) {
        increment("vagw_device_online_reconcile_total{result=" + result + "}");
    }

    /** fail-closed 后断连计数 */
    public void disconnect(String reason) {
        increment("vagw_device_online_disconnect_total{reason=" + reason + "}");
    }

    public void setPublishLatencyMs(long latencyMs) {
        publishLatencyMs.set(latencyMs);
    }

    public void setOutboxLagSeconds(long lagSeconds) {
        outboxLagSeconds.set(Math.max(0, lagSeconds));
    }

    public void setTopicReady(boolean ready) {
        topicReadyGauge.set(ready ? 1 : 0);
    }

    public long publishLatencyMs() {
        return publishLatencyMs.get();
    }

    public long outboxLagSeconds() {
        return outboxLagSeconds.get();
    }

    public long topicReady() {
        return topicReadyGauge.get();
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
