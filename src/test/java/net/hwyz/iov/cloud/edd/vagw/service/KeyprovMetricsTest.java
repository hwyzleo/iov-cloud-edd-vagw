package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Keyprov readiness gauge 测试（EDD-VAGW-DSN-CR-017 §9）。
 * <p>
 * gauge 必须低基数：`vagw_keyprov_ready{reason=...}` 只保留当前 reason，切换时旧 reason 归零。
 * </p>
 */
class KeyprovMetricsTest {

    @Test
    void readiness_shouldExposeLowCardinalityGaugeAndResetPreviousReason() {
        KeyprovMetrics metrics = new KeyprovMetrics();

        metrics.readiness(false, KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH.name());
        assertEquals(0L, metrics.gaugeValue(
                "vagw_keyprov_ready{reason=KEYPROV_GOLDEN_VECTOR_SET_MISMATCH}"));
        assertEquals(KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH.name(), metrics.readinessReason());

        metrics.readiness(true, "ready");
        assertEquals(1L, metrics.gaugeValue("vagw_keyprov_ready{reason=ready}"));
        assertEquals(0L, metrics.gaugeValue(
                "vagw_keyprov_ready{reason=KEYPROV_GOLDEN_VECTOR_SET_MISMATCH}"),
                "状态切换后旧 reason 的 gauge 必须归零");
        assertEquals("ready", metrics.readinessReason());
    }
}
