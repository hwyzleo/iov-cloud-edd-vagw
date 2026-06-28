package net.hwyz.iov.cloud.edd.vagw.client;

import net.hwyz.iov.cloud.edd.vagw.client.fallback.TspDeviceAdmissionFallbackFactory;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DeviceAdmissionRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DeviceAdmissionResult;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionDecision;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionReason;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * TSP设备接入裁决服务Feign客户端测试
 */
class TspDeviceAdmissionClientTest {

    @Test
    void testDecideWithValidDevice() {
        TspDeviceAdmissionClient mockClient = mock(TspDeviceAdmissionClient.class);
        DeviceAdmissionRequest request = DeviceAdmissionRequest.builder()
                .uid("DEVICE-001")
                .certSerial("cert-serial-001")
                .build();

        DeviceAdmissionResult expected = DeviceAdmissionResult.builder()
                .decision(AdmissionDecision.ALLOW)
                .vin("VIN123456")
                .build();
        when(mockClient.decide(request)).thenReturn(expected);

        DeviceAdmissionResult result = mockClient.decide(request);

        assertNotNull(result);
        assertEquals(AdmissionDecision.ALLOW, result.getDecision());
        assertEquals("VIN123456", result.getVin());
        verify(mockClient).decide(request);
    }

    @Test
    void testDecideFallback() {
        TspDeviceAdmissionFallbackFactory fallbackFactory = new TspDeviceAdmissionFallbackFactory();
        TspDeviceAdmissionClient fallbackClient = fallbackFactory.create(new RuntimeException("TSP unavailable"));

        DeviceAdmissionRequest request = DeviceAdmissionRequest.builder()
                .uid("DEVICE-001")
                .certSerial("cert-serial-001")
                .build();

        DeviceAdmissionResult result = fallbackClient.decide(request);

        assertNotNull(result);
        assertEquals(AdmissionDecision.DENY, result.getDecision());
        assertEquals(AdmissionReason.DEPENDENCY_UNAVAILABLE, result.getReason());
    }
}
