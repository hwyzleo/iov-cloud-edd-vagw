package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keyprov 专属熔断器（EDD-VAGW-DSN-CR-016 §2.5）。
 * <p>
 * 仅统计 VMD 连接失败、超时和依赖不可用；业务拒绝、协议拒绝和过载不计入依赖故障率。
 * OPEN 时快速失败（{@code KEYPROV_CIRCUIT_OPEN}），不占用 HTTP 连接；HALF_OPEN 探测数量有界，
 * 不允许恢复瞬间放大为全量并发；熔断状态只作用于 Keyprov，不修改 MQTT 连接、FOTA／远控订阅或
 * VAGW 全局 readiness。
 * </p>
 * <p>
 * 每个被允许的请求必须在终态调用 {@link #recordSuccess()} 或 {@link #recordDependencyFailure()}
 * 恰好一次。{@code enabled=false} 时所有方法退化为直通。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KeyprovCircuitBreaker {

    /** 熔断状态。 */
    public enum State { CLOSED, OPEN, HALF_OPEN }

    private enum Outcome { SUCCESS, FAILURE }

    private final boolean enabled;
    private final Duration failureWindow;
    private final Duration openDuration;
    private final int halfOpenProbes;
    private final int minimumCalls;
    private final double failureRateThreshold;
    private final KeyprovMetrics metrics;

    private final Deque<WindowEntry> window = new ArrayDeque<>();
    private final AtomicInteger halfOpenInFlight = new AtomicInteger(0);

    private State state = State.CLOSED;
    private long openUntilMillis = 0L;

    public KeyprovCircuitBreaker(KeyprovProperties properties, KeyprovMetrics metrics) {
        KeyprovProperties.CircuitBreaker cb = properties.getCircuitBreaker();
        this.enabled = cb != null && cb.isEnabled();
        this.failureWindow = cb == null ? null : cb.getFailureWindow();
        this.openDuration = cb == null ? null : cb.getOpenDuration();
        this.halfOpenProbes = cb == null ? 0 : cb.getHalfOpenProbes();
        this.minimumCalls = cb == null ? 0 : cb.getMinimumCalls();
        this.failureRateThreshold = cb == null ? 0.5d : cb.getFailureRateThreshold();
        this.metrics = metrics;
        metrics.circuitState("closed");
    }

    /**
     * 是否允许发起一次 VMD 调用。
     *
     * @return {@code false} 表示熔断 OPEN 且未到恢复探测窗口，调用方必须快速失败
     */
    public synchronized boolean allowRequest() {
        if (!enabled) {
            return true;
        }
        long now = System.currentTimeMillis();
        if (state == State.OPEN) {
            if (now < openUntilMillis) {
                metrics.circuitState("open");
                return false;
            }
            state = State.HALF_OPEN;
            halfOpenInFlight.set(0);
            metrics.circuitState("half_open");
            log.warn("Keyprov circuit breaker entering HALF_OPEN after open-duration");
        }
        if (state == State.HALF_OPEN) {
            if (halfOpenInFlight.incrementAndGet() > halfOpenProbes) {
                halfOpenInFlight.decrementAndGet();
                metrics.circuitState("half_open");
                return false;
            }
        }
        return true;
    }

    /** 依赖调用成功（含 VMD 业务拒绝）：HALF_OPEN 探测成功即关闭熔断。 */
    public synchronized void recordSuccess() {
        if (!enabled) {
            return;
        }
        if (state == State.HALF_OPEN) {
            exitHalfOpen();
            close();
            return;
        }
        if (state == State.CLOSED) {
            record(Outcome.SUCCESS);
        }
    }

    /** 依赖失败（连接失败/超时/不可用）：累计故障率，达到阈值即 OPEN。 */
    public synchronized void recordDependencyFailure() {
        if (!enabled) {
            return;
        }
        if (state == State.HALF_OPEN) {
            exitHalfOpen();
            open();
            return;
        }
        if (state == State.CLOSED) {
            record(Outcome.FAILURE);
            evaluate();
        }
    }

    public synchronized State state() {
        return state;
    }

    private void exitHalfOpen() {
        halfOpenInFlight.updateAndGet(v -> Math.max(0, v - 1));
    }

    private void record(Outcome outcome) {
        long now = System.currentTimeMillis();
        window.addLast(new WindowEntry(now, outcome));
        prune(now);
    }

    private void evaluate() {
        if (window.size() < minimumCalls || minimumCalls <= 0) {
            return;
        }
        long failures = window.stream().filter(e -> e.outcome == Outcome.FAILURE).count();
        double rate = (double) failures / window.size();
        if (rate >= failureRateThreshold) {
            open();
        }
    }

    private void prune(long now) {
        if (failureWindow == null) {
            return;
        }
        long cutoff = now - failureWindow.toMillis();
        while (!window.isEmpty() && window.peekFirst().atMillis < cutoff) {
            window.removeFirst();
        }
    }

    private void open() {
        state = State.OPEN;
        openUntilMillis = System.currentTimeMillis() + (openDuration == null ? 0L : openDuration.toMillis());
        window.clear();
        halfOpenInFlight.set(0);
        metrics.circuitState("open");
        log.warn("Keyprov circuit breaker OPEN: dependency failures reached threshold");
    }

    private void close() {
        state = State.CLOSED;
        window.clear();
        halfOpenInFlight.set(0);
        metrics.circuitState("closed");
        log.info("Keyprov circuit breaker CLOSED after successful HALF_OPEN probe");
    }

    private record WindowEntry(long atMillis, Outcome outcome) {
    }
}
