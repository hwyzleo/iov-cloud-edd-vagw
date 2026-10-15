package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * VAGW MQTT 共享订阅可观测计数器（EDD-VAGW-DSN-CR-015 §6）。
 * <p>
 * 项目当前未引入 micrometer/actuator，先以内存计数器 + 结构化日志承载可观测性，接入 Prometheus
 * 时按同名 metric 透传。内存计数器允许进程重启清零；结构化日志用于现有平台聚合与告警。
 * 禁止把 hsmUid、VIN、device_sn 或 payload 作为 label，日志只输出脱敏摘要。
 * </p>
 * <p>
 * 指标：{@code vagw_mqtt_subscription_state}、{@code vagw_mqtt_suback_total}、
 * {@code vagw_mqtt_uplink_received_total}、{@code vagw_mqtt_uplink_duplicate_total}、
 * {@code vagw_mqtt_uplink_rejected_total}、{@code vagw_mqtt_inflight}。
 * </p>
 */
@Slf4j
@Component
public class VagwMqttSubscriptionMetrics {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    private void increment(String name) {
        counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    }

    private AtomicLong gauge(String name) {
        return gauges.computeIfAbsent(name, key -> new AtomicLong());
    }

    /** 订阅状态 gauge：SUBACK 成功=1，失败/降级=0。 */
    public void subscriptionState(String group, String service, String filter, boolean active) {
        String name = "vagw_mqtt_subscription_state{group=" + group + ",service=" + service
                + ",filter=" + filter + "}";
        gauge(name).set(active ? 1L : 0L);
        if (!active) {
            log.warn("SEC-MQTT-SUB | subscription state inactive, group={}, service={}, filter={}",
                    group, service, filter);
        }
    }

    /** SUBACK 结果计数（success/failure）；failure 只降级对应 capability。 */
    public void suback(String group, String service, String result) {
        increment("vagw_mqtt_suback_total{group=" + group + ",service=" + service + ",result=" + result + "}");
    }

    /** 已投递到 VAGW 的上行消息计数（每条共享投递各计一次，重复由幂等键收敛）。 */
    public void uplinkReceived(String group, String service) {
        increment("vagw_mqtt_uplink_received_total{group=" + group + ",service=" + service + "}");
    }

    /** 幂等键判定为重复的上行消息计数（FOTA messageId / Keyprov requestId+digest）。 */
    public void uplinkDuplicate(String service) {
        increment("vagw_mqtt_uplink_duplicate_total{service=" + service + "}");
    }

    /** 上行拒绝计数（按有界 reason 分类，不携带身份或 payload）。 */
    public void uplinkRejected(String service, String reason) {
        increment("vagw_mqtt_uplink_rejected_total{service=" + service + ",reason=" + reason + "}");
    }

    /** 在途处理量 gauge；delta 为 +1/-1。 */
    public void inflight(String service, long delta) {
        AtomicLong value = gauge("vagw_mqtt_inflight{service=" + service + "}");
        value.addAndGet(delta);
        if (value.get() < 0) {
            value.set(0L);
        }
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
