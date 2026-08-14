package net.hwyz.iov.cloud.edd.vagw.application;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 接入身份校验测试（EDD-VAGW-DSN-CR-006 §6 / US-014）。
 */
class AccessIdentityValidatorTest {

    private final AccessIdentityValidator validator = new AccessIdentityValidator();

    @Test
    void validate_match_shouldPass() {
        assertNull(validator.validate("DEVICE001", "DEVICE001"));
        assertNull(validator.validate("DEVICE001", "device001")); // 大小写不敏感
    }

    @Test
    void validate_topicBlank_shouldFailContractInvalid() {
        AccessIdentityValidator.Failure f = validator.validate(" ", "DEVICE001");
        assertNotNull(f);
        assertEquals(AccessIdentityValidator.Reason.CONTRACT_INVALID, f.reason());
    }

    @Test
    void validate_envelopeBlank_shouldFailContractInvalid() {
        AccessIdentityValidator.Failure f = validator.validate("DEVICE001", "");
        assertNotNull(f);
        assertEquals(AccessIdentityValidator.Reason.CONTRACT_INVALID, f.reason());
    }

    @Test
    void validate_mismatch_shouldFailDeviceMismatch() {
        AccessIdentityValidator.Failure f = validator.validate("DEVICE001", "DEVICE002");
        assertNotNull(f);
        assertEquals(AccessIdentityValidator.Reason.DEVICE_MISMATCH, f.reason());
    }
}
