package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DownlinkCommandResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DownlinkServiceTest {

    @Mock
    private SessionService sessionService;

    @Mock
    private RouteService routeService;

    @Mock
    private MqttClientManager mqttClientManager;

    @Mock
    private BindingService bindingService;

    @InjectMocks
    private DownlinkService downlinkService;

    @Test
    void sendCommand_vinNotBound_shouldReject() {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.empty());

        DownlinkCommandResponse response = downlinkService.sendCommand(
                "VIN001", "remotecontrol", new byte[0], "msg-001", null);

        assertFalse(response.isAccepted());
        assertEquals(ErrorCode.VIN_UNAUTHORIZED.getCode(), response.getErrorCode());
    }

    @Test
    void sendCommand_vehicleOffline_shouldReject() {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(false);

        DownlinkCommandResponse response = downlinkService.sendCommand(
                "VIN001", "remotecontrol", new byte[0], "msg-001", null);

        assertFalse(response.isAccepted());
        assertEquals(ErrorCode.VEHICLE_OFFLINE.getCode(), response.getErrorCode());
    }

    @Test
    void sendCommand_noRoute_shouldReject() {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);
        when(routeService.hasRoute("unknown")).thenReturn(false);

        DownlinkCommandResponse response = downlinkService.sendCommand(
                "VIN001", "unknown", new byte[0], "msg-001", null);

        assertFalse(response.isAccepted());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE.getCode(), response.getErrorCode());
    }

    @Test
    void sendCommand_valid_shouldPublish() throws Exception {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);
        when(routeService.hasRoute("remotecontrol")).thenReturn(true);

        DownlinkCommandResponse response = downlinkService.sendCommand(
                "VIN001", "remotecontrol", new byte[0], "msg-001", null);

        assertTrue(response.isAccepted());
        verify(mqttClientManager).publish(contains("vehicle/DEVICE001/down/remotecontrol"), any(), eq(1));
    }

    @Test
    void sendCommand_mqttException_shouldReturnError() throws Exception {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);
        when(routeService.hasRoute("remotecontrol")).thenReturn(true);
        doThrow(new MqttException(0)).when(mqttClientManager).publish(any(), any(), anyInt());

        DownlinkCommandResponse response = downlinkService.sendCommand(
                "VIN001", "remotecontrol", new byte[0], "msg-001", null);

        assertFalse(response.isAccepted());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE.getCode(), response.getErrorCode());
    }
}
