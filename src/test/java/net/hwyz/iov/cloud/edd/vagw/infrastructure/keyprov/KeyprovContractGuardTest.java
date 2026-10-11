package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * keyprov 契约门禁测试（EDD-VAGW-DSN-CR-011 §2/§3.3）。
 */
class KeyprovContractGuardTest {

    @Test
    void verify_withProtoKeyprovOnClasspath_shouldBeCompatible() {
        KeyprovContractGuard guard = new KeyprovContractGuard();
        guard.verify();

        assertTrue(guard.compatible(), () -> "keyprov contract incompatible: " + guard.incompatibilityReason());
        assertEquals(6, guard.registry().size());
        for (KeyprovMessageType type : KeyprovMessageType.values()) {
            assertTrue(guard.registry().contains(type.fullName()));
        }
    }
}
