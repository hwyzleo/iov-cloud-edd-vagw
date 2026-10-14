package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * DeviceOnlineOutboxStore 字段还原测试（EDD-VAGW-DSN-CR-012 §3.5/§4）。
 * <p>
 * {@code claimStale} 走 RedisConnection.xClaim，返回 {@link ByteRecord}，其字段名是 {@code byte[]}。
 * Java 数组按引用比较，直接 {@code map.get(field.getBytes())} 永远取不到值，会导致
 * payload/kafkaKey 全为 null，发布时抛 NPE（{@code publish failure ... payload() is null}）。
 * 本测试锁定 ByteRecord → Entry 的正确还原。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DeviceOnlineOutboxStore 测试")
class DeviceOnlineOutboxStoreTest {

    private static final String STREAM = "vagw:deviceonline:stream";
    private static final String GROUP = "vagw-device-online";
    private static final String CONSUMER = "vagw-1";

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private StreamOperations<String, Object, Object> streamOperations;

    private VagwDeviceOnlineProperties properties;
    private DeviceOnlineOutboxStore store;

    @BeforeEach
    void setUp() {
        properties = new VagwDeviceOnlineProperties();
        store = new DeviceOnlineOutboxStore(redisTemplate, properties);
        when(redisTemplate.opsForStream()).thenReturn(streamOperations);
    }

    @Test
    @DisplayName("claimStale 从 ByteRecord 正确还原 payload/kafkaKey 等全部字段（byte[] 键不可按引用查找）")
    void claimStaleRestoresAllFieldsFromByteRecord() {
        // pending 中存在一条已超出租约的消息，触发认领
        PendingMessages pending = new PendingMessages(GROUP, List.of(
                new PendingMessage(RecordId.of("1-0"), Consumer.from(GROUP, "other"),
                        Duration.ofSeconds(60), 1L)));
        when(streamOperations.pending(eq(STREAM), eq(GROUP), any(Range.class), anyLong()))
                .thenReturn(pending);

        // xClaim 返回的 ByteRecord：字段名与字段值都是 byte[]（数组按引用比较）
        Map<byte[], byte[]> fields = new LinkedHashMap<>();
        fields.put(utf8("eventId"), utf8("evt-1"));
        fields.put(utf8("kafkaKey"), utf8("DEVICE-1"));
        fields.put(utf8("payload"), utf8("{\"eventId\":\"evt-1\"}"));
        fields.put(utf8("aggregateKeyHash"), utf8("abc123"));
        fields.put(utf8("occurredAt"), utf8("1700000000000"));
        ByteRecord record = ByteRecord.of(
                MapRecord.create(utf8(STREAM), fields).withId(RecordId.of("1-0")));

        when(redisTemplate.execute(any(RedisCallback.class))).thenReturn(List.of(record));

        List<DeviceOnlineOutboxEntry> entries = store.claimStale(GROUP, CONSUMER, Duration.ofSeconds(30), 100);

        assertEquals(1, entries.size());
        DeviceOnlineOutboxEntry entry = entries.get(0);
        assertEquals("1-0", entry.id().getValue());
        assertEquals("evt-1", entry.eventId());
        assertEquals("DEVICE-1", entry.kafkaKey());
        assertEquals("{\"eventId\":\"evt-1\"}", entry.payload());
        assertEquals("abc123", entry.aggregateKeyHash());
        assertEquals("1700000000000", entry.occurredAt());
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
