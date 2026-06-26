package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class AuthAclServiceTest {

    @InjectMocks
    private AuthAclService authAclService;

    @Test
    void authenticate_validVin_shouldAllow() {
        var result = authAclService.authenticate("LSGJA52U7YA000001", "client001");
        assertTrue(result.allowed());
        assertNotNull(result.acl());
        assertEquals(2, result.acl().size());
        assertTrue(result.acl().get(0).getTopic().contains("LSGJA52U7YA000001"));
    }

    @Test
    void authenticate_emptyVin_shouldDeny() {
        var result = authAclService.authenticate("", "client001");
        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_nullVin_shouldDeny() {
        var result = authAclService.authenticate(null, "client001");
        assertFalse(result.allowed());
    }

    @Test
    void authenticate_invalidFormat_shouldDeny() {
        var result = authAclService.authenticate("INVALID", "client001");
        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_lowercaseVin_shouldNormalize() {
        var result = authAclService.authenticate("lsgja52u7ya000001", "client001");
        assertTrue(result.allowed());
        assertTrue(result.acl().get(0).getTopic().contains("LSGJA52U7YA000001"));
    }
}
