package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * keyprov 契约与 readiness 门禁测试（EDD-VAGW-DSN-CR-011 §2/§3.3，EDD-VAGW-DSN-CR-017 §4/§9）。
 */
class KeyprovContractGuardTest {

    @Test
    void verify_withProtoKeyprovAndMatchingLock_shouldBeReady() {
        KeyprovContractGuard guard = new KeyprovContractGuard();
        guard.verify();

        assertTrue(guard.compatible(), () -> "keyprov contract incompatible: " + guard.incompatibilityReason());
        assertEquals(6, guard.registry().size());
        for (KeyprovMessageType type : KeyprovMessageType.values()) {
            assertTrue(guard.registry().contains(type.fullName()));
        }

        KeyprovReadiness readiness = guard.readiness();
        assertTrue(readiness.ready(), () -> "reason=" + readiness.reason() + ", detail="
                + readiness.notReadyDetail() + ", conditions=" + readiness.snapshot());
        assertNull(guard.incompatibilityReason());
        assertTrue(guard.statusPathAvailable());

        KeyprovProtocolLock lock = guard.lock();
        assertNotNull(lock, "READY 时必须暴露实际锁版清单");
        assertEquals(KeyprovProtocolLock.FIXED_PACKAGE, lock.packageName());
        assertEquals(KeyprovProtocolLock.FIXED_SERVICE, lock.service());
        assertEquals(KeyprovProtocolLock.FIXED_PROTOCOL_VERSION, lock.protocolVersion());
    }
}
