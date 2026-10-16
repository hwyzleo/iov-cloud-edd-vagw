package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Keyprov 专属熔断器测试（EDD-VAGW-DSN-CR-016 §2.5/§7-5/§7-10）。
 */
class KeyprovCircuitBreakerTest {

    private KeyprovProperties properties(int minimumCalls, double threshold, int halfOpenProbes,
                                         Duration openDuration) {
        KeyprovProperties properties = new KeyprovProperties();
        KeyprovProperties.CircuitBreaker cb = properties.getCircuitBreaker();
        cb.setEnabled(true);
        cb.setFailureWindow(Duration.ofSeconds(30));
        cb.setOpenDuration(openDuration);
        cb.setHalfOpenProbes(halfOpenProbes);
        cb.setMinimumCalls(minimumCalls);
        cb.setFailureRateThreshold(threshold);
        return properties;
    }

    @Test
    void dependencyFailures_shouldOpenCircuit() {
        KeyprovCircuitBreaker cb = new KeyprovCircuitBreaker(
                properties(2, 0.5, 1, Duration.ofSeconds(5)), new KeyprovMetrics());

        assertTrue(cb.allowRequest());
        cb.recordDependencyFailure();
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, cb.state());
        assertTrue(cb.allowRequest());
        cb.recordDependencyFailure();

        assertEquals(KeyprovCircuitBreaker.State.OPEN, cb.state());
        assertFalse(cb.allowRequest(), "OPEN 时必须快速失败，不占用连接");
    }

    @Test
    void businessSuccess_shouldKeepCircuitClosed() {
        KeyprovCircuitBreaker cb = new KeyprovCircuitBreaker(
                properties(3, 0.5, 1, Duration.ofSeconds(5)), new KeyprovMetrics());

        cb.allowRequest();
        cb.recordDependencyFailure();
        cb.allowRequest();
        cb.recordSuccess();
        cb.allowRequest();
        cb.recordSuccess();

        assertEquals(KeyprovCircuitBreaker.State.CLOSED, cb.state(),
                "业务成功/拒绝不得把失败率推过阈值");
    }

    @Test
    void halfOpen_shouldBoundProbesAndCloseOnSuccess() throws Exception {
        KeyprovCircuitBreaker cb = new KeyprovCircuitBreaker(
                properties(1, 1.0, 1, Duration.ofMillis(100)), new KeyprovMetrics());

        assertTrue(cb.allowRequest());
        cb.recordDependencyFailure();
        assertEquals(KeyprovCircuitBreaker.State.OPEN, cb.state());

        Thread.sleep(150L);
        assertTrue(cb.allowRequest(), "OPEN 到期后进入 HALF_OPEN 并放行一个探测");
        assertEquals(KeyprovCircuitBreaker.State.HALF_OPEN, cb.state());
        assertFalse(cb.allowRequest(), "HALF_OPEN 探测数量必须有界，恢复瞬间不得放大为全量并发");

        cb.recordSuccess();
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, cb.state());
        assertTrue(cb.allowRequest());
    }

    @Test
    void halfOpenFailure_shouldReopen() throws Exception {
        KeyprovCircuitBreaker cb = new KeyprovCircuitBreaker(
                properties(1, 1.0, 1, Duration.ofMillis(100)), new KeyprovMetrics());

        cb.allowRequest();
        cb.recordDependencyFailure();
        Thread.sleep(150L);

        assertTrue(cb.allowRequest());
        cb.recordDependencyFailure();
        assertEquals(KeyprovCircuitBreaker.State.OPEN, cb.state());
        assertFalse(cb.allowRequest());
    }

    @Test
    void disabledCircuitBreaker_shouldAlwaysAllow() {
        KeyprovProperties properties = properties(1, 1.0, 1, Duration.ofMillis(1));
        properties.getCircuitBreaker().setEnabled(false);
        KeyprovCircuitBreaker cb = new KeyprovCircuitBreaker(properties, new KeyprovMetrics());

        for (int i = 0; i < 10; i++) {
            assertTrue(cb.allowRequest());
            cb.recordDependencyFailure();
        }
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, cb.state());
    }
}
