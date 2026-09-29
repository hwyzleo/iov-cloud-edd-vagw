package net.hwyz.iov.cloud.edd.vagw.controller;

import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * MqttEventController 单元测试（EDD-VAGW-DSN-CR-009 §5/§7）
 * 覆盖 connected 事件的 fail-closed（未准入/未绑定设备不建立会话）。
 */
@ExtendWith(MockitoExtension.class)
class MqttEventControllerTest {

    @Mock
    private SessionService sessionService;

    @Mock
    private BindingService bindingService;

    @InjectMocks
    private MqttEventController controller;

    @Test
    void handleEvent_connected_boundDevice_shouldCallSessionService() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername("DEVICE-SN-001");
        request.setClientId("client001");
        when(bindingService.isValidAndBound("DEVICE-SN-001")).thenReturn(true);

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verify(sessionService).onConnected("DEVICE-SN-001", "client001", null, null);
    }

    @Test
    void handleEvent_connected_unboundDevice_shouldFailClosedWithoutSession() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername("UNKNOWN-SN");
        request.setClientId("client001");
        when(bindingService.isValidAndBound("UNKNOWN-SN")).thenReturn(false);

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verifyNoInteractions(sessionService);
    }

    @Test
    void handleEvent_disconnected_shouldCallSessionService() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.disconnected");
        request.setUsername("DEVICE-SN-001");

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verify(sessionService).onDisconnected("DEVICE-SN-001");
    }

    @Test
    void handleEvent_blankDeviceSn_shouldIgnore() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername("");

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verifyNoInteractions(sessionService);
    }
}
