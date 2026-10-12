package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.EmqxClientKickClient;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox.DeviceOnlineOutboxStore;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineHookResult;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * MqttConnectionEventHandler 单元测试（EDD-VAGW-DSN-CR-012 §3.2/§4/§8）。
 * <p>
 * 覆盖 ALLOW 关联、无上下文/过期/旧 generation fail-closed、持久化有界恢复与断连。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("MqttConnectionEventHandler 测试")
class MqttConnectionEventHandlerTest {

    @Mock
    private AdmissionSessionContextRegistry registry;

    @Mock
    private SessionService sessionService;

    @Mock
    private DeviceOnlineOutboxStore outboxStore;

    @Mock
    private DeviceOnlineMetrics metrics;

    @Mock
    private EmqxClientKickClient kicker;

    private VagwDeviceOnlineProperties properties;
    private MqttConnectionEventHandler handler;

    @BeforeEach
    void setUp() {
        properties = new VagwDeviceOnlineProperties();
        properties.setPersistRetryAttempts(1);
        properties.setPersistRetryBackoff(Duration.ZERO);
        DeviceOnlineEventFactory factory =
                new DeviceOnlineEventFactory(new ObjectMapper().registerModule(new JavaTimeModule()));
        handler = new MqttConnectionEventHandler(registry, sessionService, outboxStore, factory,
                properties, metrics, kicker);
    }

    private MqttEventRequest request() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername("device-1");
        request.setClientId("client1");
        request.setSessionId("sess-1");
        request.setNode("node-a");
        return request;
    }

    private AdmissionSessionContext context(Instant expiresAt) {
        return new AdmissionSessionContext("DEVICE-1", "VIN1", "CERT", "client1",
                "10.0.0.1", 5, 12L, Instant.now(), expiresAt);
    }

    @Test
    @DisplayName("ALLOW + connected：固化会话并写 Outbox，generation 来自准入快照")
    void handleConnected_shouldPersistSessionAndEnqueue() {
        when(registry.findCandidate("DEVICE-1", "client1"))
                .thenReturn(Optional.of(context(Instant.now().plusSeconds(300))));
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());
        when(outboxStore.enqueue(any(), eq("DEVICE-1"), anyString()))
                .thenReturn(DeviceOnlineOutboxStore.EnqueueResult.ENQUEUED);

        assertEquals(DeviceOnlineHookResult.ENQUEUED, handler.handleConnected(request()));

        verify(sessionService).onConnected(eq("DEVICE-1"), eq("client1"), any(), any(), eq(12L), anyString());
        verify(kicker, never()).kick(any(), any());
    }

    @Test
    @DisplayName("Hook 重试命中已有 eventId：幂等成功，不重复业务事件")
    void handleConnected_duplicate_shouldReturnDuplicate() {
        when(registry.findCandidate("DEVICE-1", "client1"))
                .thenReturn(Optional.of(context(Instant.now().plusSeconds(300))));
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());
        when(outboxStore.enqueue(any(), eq("DEVICE-1"), anyString()))
                .thenReturn(DeviceOnlineOutboxStore.EnqueueResult.DUPLICATE);

        assertEquals(DeviceOnlineHookResult.DUPLICATE, handler.handleConnected(request()));
    }

    @Test
    @DisplayName("无准入上下文：fail-closed，告警并断连，不建立会话")
    void handleConnected_noContext_shouldFailClosed() {
        when(registry.findCandidate("DEVICE-1", "client1")).thenReturn(Optional.empty());

        assertEquals(DeviceOnlineHookResult.NO_ADMISSION_CONTEXT, handler.handleConnected(request()));

        verify(sessionService).onDisconnected("DEVICE-1");
        verify(outboxStore).removeOnline("DEVICE-1");
        verify(kicker).kick("client1", "NO_ADMISSION_CONTEXT");
        verify(sessionService, never()).onConnected(any(), any(), any(), any(), any(), any());
        verify(outboxStore, never()).enqueue(any(), any(), any());
    }

    @Test
    @DisplayName("准入快照过期：fail-closed")
    void handleConnected_expired_shouldFailClosed() {
        when(registry.findCandidate("DEVICE-1", "client1"))
                .thenReturn(Optional.of(context(Instant.now().minusSeconds(1))));

        assertEquals(DeviceOnlineHookResult.EXPIRED, handler.handleConnected(request()));
        verify(kicker).kick("client1", "EXPIRED");
        verify(outboxStore, never()).enqueue(any(), any(), any());
    }

    @Test
    @DisplayName("旧 generation 迟到：拒绝且不写 Outbox")
    void handleConnected_staleGeneration_shouldReject() {
        when(registry.findCandidate("DEVICE-1", "client1"))
                .thenReturn(Optional.of(context(Instant.now().plusSeconds(300))));
        SessionInfo newer = SessionInfo.builder().deviceSn("DEVICE-1").online(true)
                .connectionGeneration(13L).build();
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.of(newer));

        assertEquals(DeviceOnlineHookResult.STALE_GENERATION, handler.handleConnected(request()));
        verify(outboxStore, never()).enqueue(any(), any(), any());
    }

    @Test
    @DisplayName("有界恢复后仍失败：fail-closed 并断连")
    void handleConnected_persistFailure_shouldFailClosed() {
        when(registry.findCandidate("DEVICE-1", "client1"))
                .thenReturn(Optional.of(context(Instant.now().plusSeconds(300))));
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());
        doThrow(new RuntimeException("redis down")).when(sessionService)
                .onConnected(any(), any(), any(), any(), any(), any());

        assertEquals(DeviceOnlineHookResult.PERSIST_FAILED, handler.handleConnected(request()));

        verify(kicker).kick("client1", "PERSIST_FAILED");
        verify(outboxStore, never()).enqueue(any(), any(), any());
    }

    @Test
    @DisplayName("链路关闭：直接返回 DISABLED，不产生副作用")
    void handleConnected_disabled_shouldReturnDisabled() {
        properties.setEnabled(false);

        assertEquals(DeviceOnlineHookResult.DISABLED, handler.handleConnected(request()));
        verifyNoInteractions(registry, sessionService, outboxStore);
    }
}
