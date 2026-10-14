package net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox;

import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DeviceOnlineOutboxStore 真实 Redis 集成测试（EDD-VAGW-DSN-CR-012 §3.5/§4/§8）。
 * <p>
 * 覆盖单测内存替身无法触达的两条真实 Redis Stream 路径：
 * <ul>
 *   <li>{@code enqueue → readNew}：XREADGROUP 正常发布路径（String 字段名）；</li>
 *   <li>{@code enqueue → readNew → claimStale}：XCLAIM 返回 {@code ByteRecord}
 *       （字段名为 {@code byte[]}），回归“数组按引用比较导致 payload 全为 null”的缺陷。</li>
 * </ul>
 * 无 Docker 环境自动跳过（{@code disabledWithoutDocker}），不阻塞无容器 CI。
 * </p>
 */
@Testcontainers(disabledWithoutDocker = true)
@DisplayName("DeviceOnlineOutboxStore 真实 Redis 集成测试")
class DeviceOnlineOutboxStoreRedisIntegrationTest {

    private static final String GROUP = "vagw-device-online-it";
    private static final String CONSUMER = "vagw-it-1";
    private static final String HSM_UID = "DEVICE-1";
    private static final String PAYLOAD = "{\"eventId\":\"evt-it-1\",\"eventType\":\"DEVICE_ONLINE\"}";

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;

    private StringRedisTemplate redisTemplate;

    private VagwDeviceOnlineProperties properties;
    private DeviceOnlineOutboxStore store;

    @BeforeAll
    static void createConnectionFactory() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
    }

    @AfterAll
    static void destroyConnectionFactory() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void setUp() {
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        flushAll();

        properties = new VagwDeviceOnlineProperties();
        properties.setConsumerGroup(GROUP);
        store = new DeviceOnlineOutboxStore(redisTemplate, properties);
        store.init();
    }

    @Test
    @DisplayName("enqueue → readNew 还原完整字段（正常发布路径）")
    void enqueueThenReadNewRestoresFields() {
        DeviceOnlineEvent event = event("evt-it-1", 5L);

        assertEquals(DeviceOnlineOutboxStore.EnqueueResult.ENQUEUED,
                store.enqueue(event, HSM_UID, PAYLOAD));

        store.ensureGroup(GROUP);
        List<DeviceOnlineOutboxEntry> entries = store.readNew(GROUP, CONSUMER, 10);

        assertEquals(1, entries.size());
        DeviceOnlineOutboxEntry entry = entries.get(0);
        assertEquals("evt-it-1", entry.eventId());
        assertEquals(HSM_UID, entry.kafkaKey());
        assertEquals(PAYLOAD, entry.payload());
        assertNotNull(entry.aggregateKeyHash());
        assertEquals(Long.toString(event.occurredAt().toEpochMilli()), entry.occurredAt());
    }

    @Test
    @DisplayName("重复 eventId 幂等：第二次入队返回 DUPLICATE 且 Stream 只有一条")
    void duplicateEventIsIdempotent() {
        DeviceOnlineEvent event = event("evt-it-dup", 5L);

        assertEquals(DeviceOnlineOutboxStore.EnqueueResult.ENQUEUED,
                store.enqueue(event, HSM_UID, PAYLOAD));
        assertEquals(DeviceOnlineOutboxStore.EnqueueResult.DUPLICATE,
                store.enqueue(event, HSM_UID, PAYLOAD));

        store.ensureGroup(GROUP);
        assertEquals(1, store.readNew(GROUP, CONSUMER, 10).size());
    }

    @Test
    @DisplayName("claimStale 从真实 XCLAIM ByteRecord 还原字段（回归：byte[] 键不可按引用查找）")
    void claimStaleRestoresFieldsFromRealByteRecord() {
        DeviceOnlineEvent event = event("evt-it-claim", 6L);
        assertEquals(DeviceOnlineOutboxStore.EnqueueResult.ENQUEUED,
                store.enqueue(event, HSM_UID, PAYLOAD));

        store.ensureGroup(GROUP);
        // 先读取使条目进入 PEL，再以 minIdle=0 认领，等价于租约超时/崩溃接管
        assertEquals(1, store.readNew(GROUP, CONSUMER, 10).size());

        List<DeviceOnlineOutboxEntry> claimed = store.claimStale(GROUP, "vagw-it-2", Duration.ZERO, 10);

        assertEquals(1, claimed.size());
        DeviceOnlineOutboxEntry entry = claimed.get(0);
        assertEquals("evt-it-claim", entry.eventId());
        assertEquals(HSM_UID, entry.kafkaKey());
        assertEquals(PAYLOAD, entry.payload(), "ByteRecord 字段名是 byte[]，还原失败会得到 null");
        assertNotNull(entry.aggregateKeyHash());
        assertEquals(Long.toString(event.occurredAt().toEpochMilli()), entry.occurredAt());
    }

    @Test
    @DisplayName("未超出租约的 pending 不被认领（多实例条件抢占）")
    void claimStaleSkipsWithinLease() {
        store.enqueue(event("evt-it-lease", 7L), HSM_UID, PAYLOAD);
        store.ensureGroup(GROUP);
        assertEquals(1, store.readNew(GROUP, CONSUMER, 10).size());

        assertTrue(store.claimStale(GROUP, "vagw-it-2", Duration.ofSeconds(30), 10).isEmpty(),
                "刚投递的 pending 未超过租约，不应被其他实例抢走");
    }

    @Test
    @DisplayName("发布成功：标记 PUBLISHED 且 XACK/XDEL 后 Stream 清空")
    void publishSuccessMarksPublishedAndCleansStream() {
        store.enqueue(event("evt-it-ack", 8L), HSM_UID, PAYLOAD);
        store.ensureGroup(GROUP);
        List<DeviceOnlineOutboxEntry> entries = store.readNew(GROUP, CONSUMER, 10);
        assertEquals(1, entries.size());
        DeviceOnlineOutboxEntry entry = entries.get(0);

        store.markPublished(entry.eventId(), System.currentTimeMillis());
        store.acknowledge(GROUP, entry.id());
        store.deleteEntry(entry.id());

        assertTrue(store.status(entry.eventId()).orElseThrow().published(), "应标记为 PUBLISHED");
        assertTrue(store.readNew(GROUP, CONSUMER, 10).isEmpty(), "已 XDEL 的条目不应再次读到");
    }

    private static DeviceOnlineEvent event(String eventId, long generation) {
        return new DeviceOnlineEvent(eventId, DeviceOnlineEvent.TYPE, HSM_UID, "VIN1",
                "sess-1", generation, "CERT", null,
                Instant.parse("2026-10-14T13:00:00Z"), DeviceOnlineEvent.TIME_SOURCE_EMQX);
    }

    private void flushAll() {
        redisTemplate.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }
}
