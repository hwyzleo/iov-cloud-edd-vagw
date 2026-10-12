package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.AdmissionSessionContextRegistry;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineEventFactory;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.MqttConnectionEventHandler;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.EmqxClientKickClient;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineHookResult;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 设备上线全链路集成测试（EDD-VAGW-DSN-CR-012 §3/§4/§8）。
 * <p>
 * Hook → 会话固化 → Outbox 入队 → 发布 的端到端串联，使用内存 Outbox 替身验证：
 * Key=hsmUid、eventId 稳定、至少一次与失败重试；不依赖真实 Redis/Kafka。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("设备上线全链路集成测试")
class DeviceOnlinePathIntegrationTest {

    private static final String TOPIC = "vagw.device.online";
    private static final String HSM_UID = "DEVICE-1";

    @Mock
    private AdmissionSessionContextRegistry registry;

    @Mock
    private SessionService sessionService;

    @Mock
    private EmqxClientKickClient kicker;

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Mock
    private ObjectProvider<KafkaTopicProvisioningStatus> provisioningStatusProvider;

    @Mock
    private KafkaTopicProvisioningStatus provisioningStatus;

    private VagwDeviceOnlineProperties properties;
    private DeviceOnlineMetrics metrics;
    private InMemoryOutboxStore store;
    private MqttConnectionEventHandler handler;
    private DeviceOnlineOutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        properties = new VagwDeviceOnlineProperties();
        properties.setPersistRetryAttempts(1);
        properties.setPersistRetryBackoff(Duration.ZERO);
        properties.setBackoffInitial(Duration.ZERO);
        properties.setBackoffMax(Duration.ZERO);
        metrics = new DeviceOnlineMetrics();
        store = new InMemoryOutboxStore(properties);
        DeviceOnlineEventFactory factory =
                new DeviceOnlineEventFactory(new ObjectMapper().registerModule(new JavaTimeModule()));
        handler = new MqttConnectionEventHandler(registry, sessionService, store, factory,
                properties, metrics, kicker);
        publisher = new DeviceOnlineOutboxPublisher(store, properties, kafkaTemplate,
                provisioningStatusProvider, metrics);
        publisher.init();
    }

    private MqttEventRequest request() {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername(HSM_UID);
        request.setClientId("client1");
        request.setSessionId("sess-1");
        request.setNode("node-a");
        return request;
    }

    private void givenAdmissionContext() {
        when(registry.findCandidate(HSM_UID, "client1")).thenReturn(Optional.of(
                new AdmissionSessionContext(HSM_UID, "VIN1", "CERT", "client1", null, null,
                        12L, Instant.now(), Instant.now().plusSeconds(300))));
        when(sessionService.getSessionByDeviceSn(HSM_UID)).thenReturn(Optional.empty());
    }

    private void givenReadyTopics() {
        when(provisioningStatusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.READY);
    }

    @Test
    @DisplayName("ALLOW+connected 产生一条事件，Kafka Key=hsmUid，payload eventId 稳定")
    void fullPath_shouldPublishWithHsmUidKey() {
        givenAdmissionContext();
        givenReadyTopics();
        when(kafkaTemplate.send(eq(TOPIC), eq(HSM_UID), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        assertEquals(DeviceOnlineHookResult.ENQUEUED, handler.handleConnected(request()));
        publisher.publishPending();

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(kafkaTemplate).send(eq(TOPIC), eq(HSM_UID), payload.capture());
        String json = new String(payload.getValue(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"eventType\":\"DEVICE_ONLINE\""));
        assertTrue(json.contains("\"hsmUid\":\"" + HSM_UID + "\""));
        assertTrue(json.contains("\"connectionGeneration\":12"));
        assertTrue(store.entries.isEmpty(), "发布成功后 Outbox 条目应清理");
    }

    @Test
    @DisplayName("Kafka 暂时不可用：事件保留可补偿，恢复后按同一 eventId 发布")
    void fullPath_kafkaFailureThenRecovery_shouldKeepSameEventId() {
        givenAdmissionContext();
        givenReadyTopics();
        when(kafkaTemplate.send(eq(TOPIC), eq(HSM_UID), any(byte[].class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        assertEquals(DeviceOnlineHookResult.ENQUEUED, handler.handleConnected(request()));
        String eventId = store.entries.keySet().iterator().next();

        publisher.publishPending();
        assertFalse(store.entries.isEmpty(), "失败时事件必须保留可补偿");

        publisher.publishPending();
        assertTrue(store.entries.isEmpty(), "恢复后应清空积压");
        verify(kafkaTemplate, times(2)).send(eq(TOPIC), eq(HSM_UID), any(byte[].class));
        assertEquals("PUBLISHED", store.statuses.get(eventId).status());
    }

    /**
     * 内存 Outbox 替身：仅用于串联链路测试，不接触 Redis。
     */
    static class InMemoryOutboxStore extends DeviceOnlineOutboxStore {

        final Map<String, DeviceOnlineOutboxEntry> entries = new LinkedHashMap<>();
        final Map<String, OutboxStatus> statuses = new HashMap<>();
        final Set<String> online = new HashSet<>();

        InMemoryOutboxStore(VagwDeviceOnlineProperties properties) {
            super(null, properties);
        }

        @Override
        public EnqueueResult enqueue(DeviceOnlineEvent event, String hsmUid, String payloadJson) {
            if (entries.containsKey(event.eventId())) {
                return EnqueueResult.DUPLICATE;
            }
            entries.put(event.eventId(), new DeviceOnlineOutboxEntry(
                    RecordId.autoGenerate(), event.eventId(), event.hsmUid(), payloadJson, "digest", "1000"));
            statuses.put(event.eventId(), new OutboxStatus("PENDING", 0, null, null));
            online.add(hsmUid);
            return EnqueueResult.ENQUEUED;
        }

        @Override
        public List<DeviceOnlineOutboxEntry> readNew(String group, String consumer, int count) {
            return new ArrayList<>(entries.values());
        }

        @Override
        public List<DeviceOnlineOutboxEntry> claimStale(String group, String consumer, Duration minIdle, int count) {
            return List.of();
        }

        @Override
        public Optional<OutboxStatus> status(String eventId) {
            return Optional.ofNullable(statuses.get(eventId));
        }

        @Override
        public void markPublished(String eventId, long publishedAtMs) {
            statuses.put(eventId, new OutboxStatus("PUBLISHED", 1, null, publishedAtMs));
        }

        @Override
        public void markRetry(String eventId, int attempts, long nextRetryAtMs, String error) {
            statuses.put(eventId, new OutboxStatus("PENDING", attempts, 0L, null));
        }

        @Override
        public void acknowledge(String group, RecordId id) {
        }

        @Override
        public void deleteEntry(RecordId id) {
            entries.values().removeIf(entry -> entry.id().equals(id));
        }

        @Override
        public void ensureGroup(String group) {
        }

        @Override
        public long oldestPendingOccurredAtMs() {
            return System.currentTimeMillis();
        }

        @Override
        public void removeOnline(String hsmUid) {
            online.remove(hsmUid);
        }

        @Override
        public boolean recordExists(String eventId) {
            return entries.containsKey(eventId);
        }
    }
}
