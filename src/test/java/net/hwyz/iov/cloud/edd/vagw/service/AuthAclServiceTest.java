package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.client.TspDeviceAdmissionClient;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DeviceAdmissionRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DeviceAdmissionResult;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionDecision;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionReason;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuthAclServiceTest {

    @Mock
    private TspDeviceAdmissionClient tspDeviceAdmissionClient;

    @InjectMocks
    private AuthAclServiceImpl authAclService;

    @Test
    void authenticate_validDeviceSnWithBinding_shouldAllow() {
        DeviceAdmissionResult result = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.ALLOW)
                .vin("LSGJA52U7YA000001")
                .build();
        when(tspDeviceAdmissionClient.decide(any(DeviceAdmissionRequest.class))).thenReturn(result);

        var authResult = authAclService.authenticate("DEVICE001", "client001", "cert-serial-001");
        assertTrue(authResult.allowed());
        assertNotNull(authResult.acl());
        assertEquals(2, authResult.acl().size());
        assertEquals("DEVICE001", authResult.deviceSn());
        assertEquals("LSGJA52U7YA000001", authResult.vin());
        assertTrue(authResult.acl().get(0).getTopic().contains("DEVICE001"));
    }

    @Test
    void authenticate_validDeviceSnWithoutBinding_shouldDeny() {
        DeviceAdmissionResult result = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.DENY)
                .reason(AdmissionReason.UNBOUND)
                .build();
        when(tspDeviceAdmissionClient.decide(any(DeviceAdmissionRequest.class))).thenReturn(result);

        var authResult = authAclService.authenticate("DEVICE001", "client001", "cert-serial-001");
        assertFalse(authResult.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, authResult.errorCode());
    }

    @Test
    void authenticate_emptyDeviceSn_shouldDeny() {
        var authResult = authAclService.authenticate("", "client001", "cert-serial-001");
        assertFalse(authResult.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, authResult.errorCode());
    }

    @Test
    void authenticate_nullDeviceSn_shouldDeny() {
        var authResult = authAclService.authenticate(null, "client001", "cert-serial-001");
        assertFalse(authResult.allowed());
    }

    @Test
    void authenticate_invalidFormat_shouldDeny() {
        var authResult = authAclService.authenticate("invalid@sn", "client001", "cert-serial-001");
        assertFalse(authResult.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, authResult.errorCode());
    }

    @Test
    void authenticate_lowercaseDeviceSn_shouldNormalize() {
        DeviceAdmissionResult result = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.ALLOW)
                .vin("LSGJA52U7YA000001")
                .build();
        when(tspDeviceAdmissionClient.decide(any(DeviceAdmissionRequest.class))).thenReturn(result);

        var authResult = authAclService.authenticate("device001", "client001", "cert-serial-001");
        assertTrue(authResult.allowed());
        assertTrue(authResult.acl().get(0).getTopic().contains("DEVICE001"));
    }

    @Test
    void authenticate_tspServiceError_shouldDenyWithDependencyUnavailable() {
        when(tspDeviceAdmissionClient.decide(any(DeviceAdmissionRequest.class)))
                .thenThrow(new RuntimeException("Service unavailable"));

        var authResult = authAclService.authenticate("DEVICE001", "client001", "cert-serial-001");
        assertFalse(authResult.allowed());
        assertEquals(ErrorCode.DEPENDENCY_UNAVAILABLE, authResult.errorCode());
    }

    @Test
    void authenticate_certRevoked_shouldDenyWithDeviceBlocked() {
        DeviceAdmissionResult result = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.DENY)
                .reason(AdmissionReason.CERT_REVOKED)
                .build();
        when(tspDeviceAdmissionClient.decide(any(DeviceAdmissionRequest.class))).thenReturn(result);

        var authResult = authAclService.authenticate("DEVICE001", "client001", "cert-serial-001");
        assertFalse(authResult.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, authResult.errorCode());
    }
}
