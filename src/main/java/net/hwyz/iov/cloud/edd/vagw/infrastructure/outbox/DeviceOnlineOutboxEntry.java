package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import org.springframework.data.redis.connection.stream.RecordId;

/**
 * 设备上线 Outbox Stream 条目（EDD-VAGW-DSN-CR-012 §3.5）。
 * <p>
 * Kafka key 与 value 均来自本条目，重试时复用已固化 eventId 与 payload，不重新读取可变绑定。
 * </p>
 */
public record DeviceOnlineOutboxEntry(
        RecordId id,
        String eventId,
        String kafkaKey,
        String payload,
        String aggregateKeyHash,
        String occurredAt) {
}
