package net.hwyz.iov.cloud.edd.vagw.infrastructure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LogMask 单元测试（EDD-VAGW-DSN-CR-005 §9 日志脱敏）。
 */
class LogMaskTest {

    @Test
    void mask_shouldMaskMiddle() {
        assertEquals("DE****45", LogMask.mask("DEVICE00012345"));
    }

    @Test
    void mask_shortId_shouldFullyMask() {
        assertEquals("****", LogMask.mask("ABCD"));
        assertEquals("****", LogMask.mask("AB"));
    }

    @Test
    void mask_null_shouldReturnNull() {
        assertNull(LogMask.mask(null));
    }

    @Test
    void maskIn_shouldMaskSensitiveInsideTopic() {
        assertEquals("vehicle/DE****45/down/fota",
                LogMask.maskIn("vehicle/DEVICE00012345/down/fota", "DEVICE00012345"));
    }
}
