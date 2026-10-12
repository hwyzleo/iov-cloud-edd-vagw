package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineEventFactory;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 设备上线 Outbox 存储（Redis Stream，EDD-VAGW-DSN-CR-012 §3.5/§4）。
 * <p>
 * 以 Redis Stream + Consumer Group 承载至少一次投递：入队（Lua 原子去重 + XADD）→
 * 消费组读取/租约认领 → 发布成功 XACK + XDEL。event_id 去重记录作为 Outbox 唯一约束与
 * 会话对账标记；发布状态（PENDING/PUBLISHED/attempts/next_retry_at）保存在独立 Hash，
 * 重试复用已固化 eventId 与 payload，不重新读取可变绑定。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceOnlineOutboxStore {

    private static final String RECORD_KEY_PREFIX = "vagw:deviceonline:record:";
    private static final String STREAM_KEY = "vagw:deviceonline:stream";
    private static final String ONLINE_KEY = "vagw:deviceonline:online";

    private static final String FIELD_EVENT_ID = "eventId";
    private static final String FIELD_KAFKA_KEY = "kafkaKey";
    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_AGGREGATE_HASH = "aggregateKeyHash";
    private static final String FIELD_OCCURRED_AT = "occurredAt";

    private static final String STATUS_PENDING = "PENDING";
    private static final String STATUS_PUBLISHED = "PUBLISHED";

    private final StringRedisTemplate redisTemplate;
    private final VagwDeviceOnlineProperties properties;

    private final DefaultRedisScript<String> enqueueScript = new DefaultRedisScript<>();

    @PostConstruct
    void init() {
        enqueueScript.setLocation(new ClassPathResource("scripts/device_online_enqueue.lua"));
        enqueueScript.setResultType(String.class);
    }

    /**
     * 入队结果。
     */
    public enum EnqueueResult { ENQUEUED, DUPLICATE }

    /**
     * Outbox 发布状态。
     */
    public record OutboxStatus(String status, int attempts, Long nextRetryAtMs, Long publishedAtMs) {
        public boolean published() {
            return STATUS_PUBLISHED.equals(status);
        }

        public boolean retryDue(long nowMs) {
            return nextRetryAtMs == null || nextRetryAtMs <= nowMs;
        }
    }

    /**
     * 原子入队：eventId 去重（唯一约束）+ XADD + 在线会话索引；重复 eventId 直接幂等返回。
     */
    public EnqueueResult enqueue(DeviceOnlineEvent event, String hsmUid, String payloadJson) {
        long nowMs = System.currentTimeMillis();
        long occurredAtMs = event.occurredAt() == null ? nowMs : event.occurredAt().toEpochMilli();
        List<String> keys = List.of(recordKey(event.eventId()), STREAM_KEY, ONLINE_KEY);
        Object[] args = {
                event.eventId(),
                event.hsmUid(),
                payloadJson,
                aggregateKeyHash(event.hsmUid()),
                Long.toString(occurredAtMs),
                Long.toString(properties.getDedupTtl().toSeconds()),
                normalize(hsmUid),
                Long.toString(nowMs),
                Long.toString(properties.getStreamMaxLen())
        };
        String result = redisTemplate.execute(enqueueScript, keys, args);
        boolean created = "ENQUEUED".equals(result);
        if (created) {
            log.info("DEVICE-ONLINE | outbox enqueued: eventId={}, hsmUidDigest={}, generation={}",
                    event.eventId(), identityDigest(event.hsmUid()), event.connectionGeneration());
        }
        return created ? EnqueueResult.ENQUEUED : EnqueueResult.DUPLICATE;
    }

    /**
     * 创建消费者组（不存在时），幂等。
     */
    public void ensureGroup(String group) {
        try {
            redisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0"), group);
        } catch (Exception e) {
            // BUSYGROUP：已存在，忽略
            log.debug("DEVICE-ONLINE | consumer group already exists: group={}", group);
        }
    }

    /**
     * 读取新消息（&gt;，at-least-once：读取即进入 PEL，成功发布后 XACK）。
     */
    public List<DeviceOnlineOutboxEntry> readNew(String group, String consumer, int count) {
        try {
            List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                    .read(Consumer.from(group, consumer),
                            StreamReadOptions.empty().count(count),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));
            return toEntries(records);
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | read new outbox entries failed: {}", e.getMessage());
            ensureGroup(group);
            return Collections.emptyList();
        }
    }

    /**
     * 认领超出租约的 pending 消息（多实例条件抢占 / 崩溃恢复）。
     */
    public List<DeviceOnlineOutboxEntry> claimStale(String group, String consumer, Duration minIdle, int count) {
        try {
            PendingMessages pending = redisTemplate.opsForStream()
                    .pending(STREAM_KEY, group, Range.unbounded(), count);
            List<RecordId> ids = new ArrayList<>();
            for (PendingMessage message : pending) {
                if (message.getElapsedTimeSinceLastDelivery().compareTo(minIdle) >= 0) {
                    ids.add(message.getId());
                }
            }
            if (ids.isEmpty()) {
                return Collections.emptyList();
            }
            // 3.0.x 的 StreamOperations 尚未提供 claim，直接走 RedisConnection.xClaim（租约认领）
            List<ByteRecord> records = redisTemplate.execute((RedisCallback<List<ByteRecord>>) (RedisConnection connection) ->
                    connection.xClaim(STREAM_KEY.getBytes(StandardCharsets.UTF_8), group, consumer,
                            RedisStreamCommands.XClaimOptions.minIdle(minIdle).ids(ids)));
            return toEntriesFromBytes(records);
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | claim stale outbox entries failed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public void acknowledge(String group, RecordId id) {
        try {
            redisTemplate.opsForStream().acknowledge(STREAM_KEY, group, id);
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | acknowledge failed: id={}, err={}", id, e.getMessage());
        }
    }

    public void deleteEntry(RecordId id) {
        try {
            redisTemplate.opsForStream().delete(STREAM_KEY, id);
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | delete stream entry failed: id={}, err={}", id, e.getMessage());
        }
    }

    public void markPublished(String eventId, long publishedAtMs) {
        try {
            redisTemplate.opsForHash().putAll(recordKey(eventId), Map.of(
                    "status", STATUS_PUBLISHED,
                    "publishedAt", Long.toString(publishedAtMs)));
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | mark published failed: eventId={}", eventId, e);
        }
    }

    public void markRetry(String eventId, int attempts, long nextRetryAtMs, String error) {
        try {
            redisTemplate.opsForHash().putAll(recordKey(eventId), Map.of(
                    "status", STATUS_PENDING,
                    "attempts", Integer.toString(attempts),
                    "nextRetryAt", Long.toString(nextRetryAtMs),
                    "lastError", truncate(error)));
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | mark retry failed: eventId={}", eventId, e);
        }
    }

    public Optional<OutboxStatus> status(String eventId) {
        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(recordKey(eventId));
            if (entries.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new OutboxStatus(
                    str(entries.get("status")),
                    parseInt(entries.get("attempts")),
                    parseLong(entries.get("nextRetryAt")),
                    parseLong(entries.get("publishedAt"))));
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | read outbox status failed: eventId={}", eventId, e);
            return Optional.empty();
        }
    }

    public boolean recordExists(String eventId) {
        try {
            return Boolean.TRUE.equals(redisTemplate.hasKey(recordKey(eventId)));
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | check record existence failed: eventId={}", eventId, e);
            return false;
        }
    }

    public void removeRecord(String eventId) {
        try {
            redisTemplate.delete(recordKey(eventId));
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | remove record failed: eventId={}", eventId, e);
        }
    }

    public Set<String> onlineMembers(int limit) {
        try {
            Set<String> members = redisTemplate.opsForZSet().range(ONLINE_KEY, 0, limit - 1L);
            return members == null ? Collections.emptySet() : members;
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | read online sessions failed: {}", e.getMessage());
            return Collections.emptySet();
        }
    }

    public void removeOnline(String hsmUid) {
        if (hsmUid == null || hsmUid.isBlank()) {
            return;
        }
        try {
            redisTemplate.opsForZSet().remove(ONLINE_KEY, normalize(hsmUid));
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | remove online session failed: {}", e.getMessage());
        }
    }

    public long oldestPendingOccurredAtMs() {
        try {
            PendingMessages pending = redisTemplate.opsForStream()
                    .pending(STREAM_KEY, properties.getConsumerGroup(), Range.unbounded(), 1);
            if (pending.isEmpty()) {
                return System.currentTimeMillis();
            }
            Long timestamp = pending.get(0).getId().getTimestamp();
            return timestamp == null ? System.currentTimeMillis() : timestamp;
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private List<DeviceOnlineOutboxEntry> toEntries(List<MapRecord<String, Object, Object>> records) {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        List<DeviceOnlineOutboxEntry> entries = new ArrayList<>(records.size());
        for (MapRecord<String, Object, Object> record : records) {
            Map<Object, Object> value = record.getValue();
            entries.add(new DeviceOnlineOutboxEntry(
                    record.getId(),
                    str(value.get(FIELD_EVENT_ID)),
                    str(value.get(FIELD_KAFKA_KEY)),
                    str(value.get(FIELD_PAYLOAD)),
                    str(value.get(FIELD_AGGREGATE_HASH)),
                    str(value.get(FIELD_OCCURRED_AT))));
        }
        return entries;
    }

    private List<DeviceOnlineOutboxEntry> toEntriesFromBytes(List<ByteRecord> records) {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        List<DeviceOnlineOutboxEntry> entries = new ArrayList<>(records.size());
        for (ByteRecord record : records) {
            Map<byte[], byte[]> value = record.getValue();
            entries.add(new DeviceOnlineOutboxEntry(
                    record.getId(),
                    bytes(value.get(FIELD_EVENT_ID.getBytes(StandardCharsets.UTF_8))),
                    bytes(value.get(FIELD_KAFKA_KEY.getBytes(StandardCharsets.UTF_8))),
                    bytes(value.get(FIELD_PAYLOAD.getBytes(StandardCharsets.UTF_8))),
                    bytes(value.get(FIELD_AGGREGATE_HASH.getBytes(StandardCharsets.UTF_8))),
                    bytes(value.get(FIELD_OCCURRED_AT.getBytes(StandardCharsets.UTF_8)))));
        }
        return entries;
    }

    private static String bytes(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    private String recordKey(String eventId) {
        return RECORD_KEY_PREFIX + eventId;
    }

    private static String aggregateKeyHash(String hsmUid) {
        String digest = identityDigest(hsmUid);
        return digest == null ? "" : digest;
    }

    private static String identityDigest(String hsmUid) {
        return hsmUid == null ? null : DeviceOnlineEventFactory.identityDigest(hsmUid);
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private static String str(Object value) {
        return value == null ? null : value.toString();
    }

    private static int parseInt(Object value) {
        try {
            return value == null ? 0 : Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static Long parseLong(Object value) {
        try {
            return value == null ? null : Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 512 ? error : error.substring(0, 509) + "...";
    }
}
