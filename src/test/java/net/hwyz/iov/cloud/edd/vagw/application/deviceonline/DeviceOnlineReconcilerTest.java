package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox.DeviceOnlineOutboxStore;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DeviceOnlineReconciler 单元测试（EDD-VAGW-DSN-CR-012 §3.5/§4）。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DeviceOnlineReconciler 测试")
class DeviceOnlineReconcilerTest {

    @Mock
    private DeviceOnlineOutboxStore store;

    @Mock
    private SessionService sessionService;

    @Mock
    private DeviceOnlineMetrics metrics;

    private VagwDeviceOnlineProperties properties;
    private DeviceOnlineReconciler reconciler;

    @BeforeEach
    void setUp() {
        properties = new VagwDeviceOnlineProperties();
        DeviceOnlineEventFactory factory =
                new DeviceOnlineEventFactory(new ObjectMapper().registerModule(new JavaTimeModule()));
        reconciler = new DeviceOnlineReconciler(store, sessionService, factory, properties, metrics);
    }

    private SessionInfo onlineSession(String eventId) {
        return SessionInfo.builder()
                .deviceSn("DEVICE-1")
                .vin("VIN1")
                .brokerSessionId("sess-1")
                .connectionGeneration(12L)
                .certSerial("CERT")
                .online(true)
                .connectedAt(Instant.now())
                .lastDeviceOnlineEventId(eventId)
                .build();
    }

    @Test
    @DisplayName("在线会话但 Outbox 记录缺失：按已固化 eventId 重建并重新入队")
    void reconcile_missingRecord_shouldReenqueue() {
        when(store.onlineMembers(anyInt())).thenReturn(Set.of("DEVICE-1"));
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(onlineSession("evt-1")));
        when(store.recordExists("evt-1")).thenReturn(false);

        reconciler.reconcile();

        verify(store).enqueue(argThat(event -> event != null && "evt-1".equals(event.eventId())),
                eq("DEVICE-1"), anyString());
        verify(metrics).reconcile("reenqueued");
    }

    @Test
    @DisplayName("记录已存在：不重复入队")
    void reconcile_existingRecord_shouldSkip() {
        when(store.onlineMembers(anyInt())).thenReturn(Set.of("DEVICE-1"));
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(onlineSession("evt-1")));
        when(store.recordExists("evt-1")).thenReturn(true);

        reconciler.reconcile();

        verify(store, never()).enqueue(any(), any(), any());
        verify(metrics).reconcile("ok");
    }

    @Test
    @DisplayName("会话已离线：清理在线索引")
    void reconcile_offlineSession_shouldRemoveIndex() {
        when(store.onlineMembers(anyInt())).thenReturn(Set.of("DEVICE-1"));
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());

        reconciler.reconcile();

        verify(store).removeOnline("DEVICE-1");
        verify(metrics).reconcile("stale-session-removed");
    }

    @Test
    @DisplayName("会话无 eventId：仅记录，不重建")
    void reconcile_missingEventId_shouldSkip() {
        when(store.onlineMembers(anyInt())).thenReturn(Set.of("DEVICE-1"));
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(onlineSession(null)));

        reconciler.reconcile();

        verify(store, never()).enqueue(any(), any(), any());
        verify(metrics).reconcile("missing-event-id");
    }
}
