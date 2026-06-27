package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuthAclServiceTest {

    @Mock
    private BindingService bindingService;
    
    @InjectMocks
    private AuthAclService authAclService;

    @Test
    void authenticate_validDeviceSnWithBinding_shouldAllow() {
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("LSGJA52U7YA000001"));
        
        var result = authAclService.authenticate("DEVICE001", "client001");
        assertTrue(result.allowed());
        assertNotNull(result.acl());
        assertEquals(2, result.acl().size());
        assertEquals("DEVICE001", result.deviceSn());
        assertEquals("LSGJA52U7YA000001", result.vin());
        assertTrue(result.acl().get(0).getTopic().contains("DEVICE001"));
    }

    @Test
    void authenticate_validDeviceSnWithoutBinding_shouldDeny() {
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.empty());
        
        var result = authAclService.authenticate("DEVICE001", "client001");
        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_emptyDeviceSn_shouldDeny() {
        var result = authAclService.authenticate("", "client001");
        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_nullDeviceSn_shouldDeny() {
        var result = authAclService.authenticate(null, "client001");
        assertFalse(result.allowed());
    }

    @Test
    void authenticate_invalidFormat_shouldDeny() {
        var result = authAclService.authenticate("invalid@sn", "client001");
        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_lowercaseDeviceSn_shouldNormalize() {
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("LSGJA52U7YA000001"));
        
        var result = authAclService.authenticate("device001", "client001");
        assertTrue(result.allowed());
        assertTrue(result.acl().get(0).getTopic().contains("DEVICE001"));
    }
}
