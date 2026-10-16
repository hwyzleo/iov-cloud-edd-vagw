package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Keyprov 有界并发准入测试（EDD-VAGW-DSN-CR-016 §2.2/§7-4）。
 */
class KeyprovAdmissionBulkheadTest {

    private KeyprovAdmissionBulkhead bulkhead(int maxInFlight, Duration queueWait) {
        KeyprovProperties properties = new KeyprovProperties();
        properties.setMaxInFlight(maxInFlight);
        properties.setQueueWait(queueWait);
        return new KeyprovAdmissionBulkhead(properties, new KeyprovMetrics());
    }

    @Test
    void permitExhaustion_shouldRejectImmediately() {
        KeyprovAdmissionBulkhead bulkhead = bulkhead(2, Duration.ZERO);

        KeyprovAdmissionBulkhead.Permit first = bulkhead.tryAcquire();
        KeyprovAdmissionBulkhead.Permit second = bulkhead.tryAcquire();
        assertNotNull(first);
        assertNotNull(second);

        long startedAt = System.nanoTime();
        assertNull(bulkhead.tryAcquire(), "许可耗尽必须立即拒绝，不进入无界等待队列");
        long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;
        assertTrue(elapsedMs < 200, "拒绝必须是即时的，实际 " + elapsedMs + "ms");

        first.close();
        assertNotNull(bulkhead.tryAcquire(), "释放后许可应可再次获取");
    }

    @Test
    void permitRelease_isIdempotent() {
        KeyprovAdmissionBulkhead bulkhead = bulkhead(1, Duration.ZERO);
        KeyprovAdmissionBulkhead.Permit permit = bulkhead.tryAcquire();
        assertNotNull(permit);

        permit.close();
        permit.close();

        assertEquals(1, bulkhead.availablePermits(), "重复 close 不得重复释放许可");
    }

    @Test
    void metrics_shouldTrackAdmissionAndInflight() {
        KeyprovMetrics metrics = new KeyprovMetrics();
        KeyprovProperties properties = new KeyprovProperties();
        properties.setMaxInFlight(1);
        KeyprovAdmissionBulkhead bulkhead = new KeyprovAdmissionBulkhead(properties, metrics);

        KeyprovAdmissionBulkhead.Permit permit = bulkhead.tryAcquire();
        assertNotNull(permit);
        assertEquals(1L, metrics.get("keyprov_admission_total{result=admitted}"));
        assertEquals(1L, metrics.gaugeValue("keyprov_inflight_current"));

        assertNull(bulkhead.tryAcquire());
        assertEquals(1L, metrics.get("keyprov_admission_total{result=overloaded}"));

        permit.close();
        assertEquals(0L, metrics.gaugeValue("keyprov_inflight_current"));
    }

    @Test
    void zeroPermits_shouldAlwaysReject() {
        KeyprovAdmissionBulkhead bulkhead = bulkhead(0, Duration.ZERO);
        assertNull(bulkhead.tryAcquire());
    }
}
