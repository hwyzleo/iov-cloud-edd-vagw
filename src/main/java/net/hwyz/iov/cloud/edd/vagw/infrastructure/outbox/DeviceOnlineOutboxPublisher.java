package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineEventFactory;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 设备上线 Outbox 发布器（EDD-VAGW-DSN-CR-012 §3.5/§4/§6）。
 * <p>
 * 定时扫描 Redis Stream Outbox：仅在 Topic Provisioning READY 时发布；Producer 幂等 + acks=all；
 * Kafka Key=hsmUid、value 使用已固化 payload；成功标记 PUBLISHED 并 XACK/XDEL；超时或
 * outcome unknown 保持可补偿并指数退避，不删除、不改 eventId。多实例经消费组租约认领 pending，
 * 崩溃后自动接管。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceOnlineOutboxPublisher {

    private final DeviceOnlineOutboxStore store;
    private final VagwDeviceOnlineProperties properties;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectProvider<KafkaTopicProvisioningStatus> provisioningStatusProvider;
    private final DeviceOnlineMetrics metrics;

    @Value("${HOSTNAME:vagw}")
    private String hostname;

    private String consumerName;

    @PostConstruct
    void init() {
        this.consumerName = hostname + "-" + UUID.randomUUID().toString().substring(0, 8);
        // Kafka Producer 幂等/acks 由 spring.kafka.producer.* 提供；此处只负责 Outbox 编排。
    }

    @Scheduled(fixedDelayString = "${vagw.kafka.device-online.publish-interval-ms:5000}")
    public void publishPending() {
        if (!properties.isEnabled()) {
            return;
        }
        if (!producerTopicsReady()) {
            metrics.setTopicReady(false);
            metrics.publishResult("not-ready");
            return;
        }
        metrics.setTopicReady(true);

        store.ensureGroup(properties.getConsumerGroup());
        Map<String, DeviceOnlineOutboxEntry> batch = new LinkedHashMap<>();
        for (DeviceOnlineOutboxEntry entry : store.readNew(
                properties.getConsumerGroup(), consumerName, properties.getOutboxBatchSize())) {
            batch.putIfAbsent(entry.eventId(), entry);
        }
        for (DeviceOnlineOutboxEntry entry : store.claimStale(
                properties.getConsumerGroup(), consumerName, properties.getLeaseTimeout(), properties.getOutboxBatchSize())) {
            batch.putIfAbsent(entry.eventId(), entry);
        }
        if (batch.isEmpty()) {
            metrics.setOutboxLagSeconds(0);
            return;
        }

        long nowMs = System.currentTimeMillis();
        for (DeviceOnlineOutboxEntry entry : batch.values()) {
            tryPublish(entry, nowMs);
        }
        long lagSeconds = Math.max(0, (nowMs - store.oldestPendingOccurredAtMs()) / 1000);
        metrics.setOutboxLagSeconds(lagSeconds);
    }

    private void tryPublish(DeviceOnlineOutboxEntry entry, long nowMs) {
        DeviceOnlineOutboxStore.OutboxStatus status = store.status(entry.eventId()).orElse(null);
        if (status != null && status.published()) {
            // 已发布重复投递：幂等确认并清理 Stream 条目
            store.acknowledge(properties.getConsumerGroup(), entry.id());
            store.deleteEntry(entry.id());
            return;
        }
        if (status != null && !status.retryDue(nowMs)) {
            // 退避未到：保持 pending，等待下次租约认领
            return;
        }

        long startedAt = System.currentTimeMillis();
        try {
            kafkaTemplate.send(properties.getTopic(), entry.kafkaKey(),
                            entry.payload().getBytes(StandardCharsets.UTF_8))
                    .get(properties.getPublishTimeout().toMillis(), TimeUnit.MILLISECONDS);
            long latency = System.currentTimeMillis() - startedAt;
            metrics.setPublishLatencyMs(latency);
            metrics.publishResult("success");
            metrics.outboxStatus("PUBLISHED");
            store.markPublished(entry.eventId(), System.currentTimeMillis());
            store.acknowledge(properties.getConsumerGroup(), entry.id());
            store.deleteEntry(entry.id());
            log.info("DEVICE-ONLINE | published: eventId={}, hsmUidDigest={}, latencyMs={}",
                    entry.eventId(), entry.aggregateKeyHash(), latency);
        } catch (Exception e) {
            int attempts = (status == null ? 0 : status.attempts()) + 1;
            long nextRetryAtMs = nowMs + backoff(attempts).toMillis();
            store.markRetry(entry.eventId(), attempts, nextRetryAtMs, e.getMessage());
            metrics.publishResult("failure");
            metrics.outboxStatus("RETRY");
            metrics.publishFailureAlert(entry.aggregateKeyHash(), e.getMessage());
        }
    }

    private Duration backoff(int attempts) {
        long initial = Math.max(1, properties.getBackoffInitial().toMillis());
        long max = Math.max(initial, properties.getBackoffMax().toMillis());
        int shift = Math.min(Math.max(0, attempts - 1), 20);
        long delay = initial << shift;
        if (delay <= 0 || delay > max) {
            delay = max;
        }
        return Duration.ofMillis(delay);
    }

    /**
     * Producer 就绪门禁：Topic Provisioning 未 READY 时不静默发送，保留 Outbox 并后台重试。
     */
    private boolean producerTopicsReady() {
        KafkaTopicProvisioningStatus status = provisioningStatusProvider.getIfAvailable();
        return status == null || status.state() != KafkaTopicProvisioningStatus.State.NOT_READY;
    }
}
