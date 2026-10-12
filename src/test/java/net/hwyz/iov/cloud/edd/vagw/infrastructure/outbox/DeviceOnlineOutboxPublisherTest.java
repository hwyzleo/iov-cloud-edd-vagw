package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DeviceOnlineOutboxPublisher 单元测试（EDD-VAGW-DSN-CR-012 §3.5/§4/§8）。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DeviceOnlineOutboxPublisher 测试")
class DeviceOnlineOutboxPublisherTest {

    private static final String TOPIC = "vagw.device.online";
    private static final String GROUP = "vagw-device-online";
    private static final String EVENT_ID = "evt-1";

    @Mock
    private DeviceOnlineOutboxStore store;

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Mock
    private ObjectProvider<KafkaTopicProvisioningStatus> provisioningStatusProvider;

    @Mock
    private KafkaTopicProvisioningStatus provisioningStatus;

    private VagwDeviceOnlineProperties properties;
    private DeviceOnlineMetrics metrics;
    private DeviceOnlineOutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        properties = new VagwDeviceOnlineProperties();
        metrics = new DeviceOnlineMetrics();
        publisher = new DeviceOnlineOutboxPublisher(store, properties, kafkaTemplate,
                provisioningStatusProvider, metrics);
        publisher.init();
    }

    private DeviceOnlineOutboxEntry entry() {
        return new DeviceOnlineOutboxEntry(RecordId.of("1-0"), EVENT_ID, "HSM1",
                "{\"eventId\":\"evt-1\"}", "digest", "1000");
    }

    private void stubReadyBatch() {
        when(provisioningStatusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.READY);
        when(store.readNew(eq(GROUP), anyString(), anyInt())).thenReturn(List.of(entry()));
        when(store.claimStale(eq(GROUP), anyString(), any(), anyInt())).thenReturn(List.of());
        when(store.status(EVENT_ID)).thenReturn(Optional.empty());
        when(store.oldestPendingOccurredAtMs()).thenReturn(Instant.now().toEpochMilli());
    }

    @Test
    @DisplayName("READY：Key=hsmUid 发布成功后标记 PUBLISHED 并 XACK/XDEL")
    void publishPending_ready_shouldPublishAndAck() {
        stubReadyBatch();
        when(kafkaTemplate.send(eq(TOPIC), eq("HSM1"), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        publisher.publishPending();

        verify(kafkaTemplate).send(eq(TOPIC), eq("HSM1"), any(byte[].class));
        verify(store).markPublished(eq(EVENT_ID), anyLong());
        verify(store).acknowledge(eq(GROUP), any(RecordId.class));
        verify(store).deleteEntry(any(RecordId.class));
        assertEquals(1, metrics.get("vagw_device_online_publish_total{result=success}"));
    }

    @Test
    @DisplayName("NOT_READY：Producer 门禁不发送，保留 Outbox 等待恢复")
    void publishPending_notReady_shouldNotSend() {
        when(provisioningStatusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.NOT_READY);

        publisher.publishPending();

        verifyNoInteractions(kafkaTemplate);
        verify(store, never()).readNew(anyString(), anyString(), anyInt());
        assertEquals(0, metrics.topicReady());
    }

    @Test
    @DisplayName("投递失败：保持可补偿并指数退避，重试不生成新 eventId")
    void publishPending_failure_shouldScheduleRetry() {
        stubReadyBatch();
        when(kafkaTemplate.send(eq(TOPIC), eq("HSM1"), any(byte[].class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        publisher.publishPending();

        verify(store).markRetry(eq(EVENT_ID), eq(1), anyLong(), anyString());
        verify(store, never()).markPublished(anyString(), anyLong());
        assertEquals(1, metrics.get("vagw_device_online_publish_total{result=failure}"));
    }

    @Test
    @DisplayName("已 PUBLISHED 的重复投递：幂等确认清理，不再次发送")
    void publishPending_alreadyPublished_shouldAckWithoutSend() {
        stubReadyBatch();
        when(store.status(EVENT_ID))
                .thenReturn(Optional.of(new DeviceOnlineOutboxStore.OutboxStatus(
                        "PUBLISHED", 1, null, Instant.now().toEpochMilli())));

        publisher.publishPending();

        verifyNoInteractions(kafkaTemplate);
        verify(store).acknowledge(eq(GROUP), any(RecordId.class));
        verify(store).deleteEntry(any(RecordId.class));
    }

    @Test
    @DisplayName("退避未到期：跳过，不发送不确认")
    void publishPending_retryNotDue_shouldSkip() {
        stubReadyBatch();
        when(store.status(EVENT_ID))
                .thenReturn(Optional.of(new DeviceOnlineOutboxStore.OutboxStatus(
                        "PENDING", 2, System.currentTimeMillis() + 60_000, null)));

        publisher.publishPending();

        verifyNoInteractions(kafkaTemplate);
        verify(store, never()).acknowledge(anyString(), any());
    }
}
