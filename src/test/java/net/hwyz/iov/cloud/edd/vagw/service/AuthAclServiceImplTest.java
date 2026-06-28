package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.client.TspDeviceAdmissionClient;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DeviceAdmissionResult;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionDecision;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionReason;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * AuthAclServiceImpl单元测试
 */
@ExtendWith(MockitoExtension.class)
class AuthAclServiceImplTest {

    @Mock
    private TspDeviceAdmissionClient tspDeviceAdmissionClient;

    @InjectMocks
    private AuthAclServiceImpl authAclService;

    @Test
    void testAuthenticateWithValidDevice() {
        String deviceSn = "DEVICE-001";
        String clientId = "DEVICE-001";
        String certSerial = "cert-serial-001";

        DeviceAdmissionResult admissionResult = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.ALLOW)
                .vin("VIN001")
                .build();

        when(tspDeviceAdmissionClient.decide(any())).thenReturn(admissionResult);

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, clientId, certSerial);

        assertTrue(result.allowed());
        assertEquals("VIN001", result.vin());
        assertEquals(deviceSn, result.deviceSn());
        assertNotNull(result.acl());
    }

    @Test
    void testAuthenticateWithUnknownDevice() {
        String deviceSn = "DEVICE-001";
        String clientId = "DEVICE-001";
        String certSerial = "cert-serial-001";

        DeviceAdmissionResult admissionResult = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.DENY)
                .reason(AdmissionReason.UID_UNKNOWN)
                .build();

        when(tspDeviceAdmissionClient.decide(any())).thenReturn(admissionResult);

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, clientId, certSerial);

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void testAuthenticateWithBlockedDevice() {
        String deviceSn = "DEVICE-001";
        String clientId = "DEVICE-001";
        String certSerial = "cert-serial-001";

        DeviceAdmissionResult admissionResult = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.DENY)
                .reason(AdmissionReason.DEVICE_BLOCKED)
                .build();

        when(tspDeviceAdmissionClient.decide(any())).thenReturn(admissionResult);

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, clientId, certSerial);

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
    }

    @Test
    void testAuthenticateWithTspUnavailable() {
        String deviceSn = "DEVICE-001";
        String clientId = "DEVICE-001";
        String certSerial = "cert-serial-001";

        when(tspDeviceAdmissionClient.decide(any()))
                .thenThrow(new RuntimeException("TSP service unavailable"));

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, clientId, certSerial);

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEPENDENCY_UNAVAILABLE, result.errorCode());
    }

    @Test
    void testAuthenticateWithEmptyDeviceSn() {
        String deviceSn = "";
        String clientId = "DEVICE-001";
        String certSerial = "cert-serial-001";

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, clientId, certSerial);

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void testAuthenticateWithInvalidDeviceSnFormat() {
        String deviceSn = "invalid@device#sn";
        String clientId = "DEVICE-001";
        String certSerial = "cert-serial-001";

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, clientId, certSerial);

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }
}
