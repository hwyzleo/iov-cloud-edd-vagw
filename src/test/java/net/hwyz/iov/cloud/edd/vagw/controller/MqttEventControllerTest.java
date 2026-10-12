package net.hwyz.iov.cloud.edd.vagw.controller;

import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.MqttConnectionEventHandler;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
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
 * MqttEventController 单元测试（EDD-VAGW-DSN-CR-012 §3.2）。
 * <p>
 * connected 事件委派设备上线处理器；disconnected 仅更新会话状态；空身份忽略。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class MqttEventControllerTest {

    @Mock
    private SessionService sessionService;

    @Mock
    private MqttConnectionEventHandler deviceOnlineHandler;

    @InjectMocks
    private MqttEventController controller;

    private MqttEventRequest connectedRequest(String deviceSn) {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername(deviceSn);
        request.setClientId("client001");
        return request;
    }

    @Test
    void handleEvent_connected_shouldDelegateToDeviceOnlineHandler() {
        MqttEventRequest request = connectedRequest("DEVICE-SN-001");

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verify(deviceOnlineHandler).handleConnected(request);
        verifyNoInteractions(sessionService);
    }

    @Test
    void handleEvent_disconnected_shouldOnlyUpdateSession() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.disconnected");
        request.setUsername("DEVICE-SN-001");

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verify(sessionService).onDisconnected("DEVICE-SN-001");
        verifyNoInteractions(deviceOnlineHandler);
    }

    @Test
    void handleEvent_blankDeviceSn_shouldIgnore() {
        MqttEventRequest request = connectedRequest("");

        ResponseEntity<Void> response = controller.handleEvent(request);

        assertEquals(200, response.getStatusCode().value());
        verifyNoInteractions(deviceOnlineHandler);
        verifyNoInteractions(sessionService);
    }
}
