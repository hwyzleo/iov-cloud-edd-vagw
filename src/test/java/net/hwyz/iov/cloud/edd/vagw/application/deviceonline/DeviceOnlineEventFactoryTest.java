package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DeviceOnlineEventFactory 单元测试（EDD-VAGW-DSN-CR-012 §3.3/§3.4）。
 */
@DisplayName("DeviceOnlineEventFactory 测试")
class DeviceOnlineEventFactoryTest {

    private DeviceOnlineEventFactory factory;
    private AdmissionSessionContext context;

    @BeforeEach
    void setUp() {
        factory = new DeviceOnlineEventFactory(new ObjectMapper().registerModule(new JavaTimeModule()));
        context = new AdmissionSessionContext("DEVICE-1", "VIN1", "CERT", "client1",
                "10.0.0.1", 5, 12L, Instant.now(), Instant.now().plusSeconds(300));
    }

    private MqttEventRequest request(String sessionId, String node, Long ts) {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername("DEVICE-1");
        request.setClientId("client1");
        request.setSessionId(sessionId);
        request.setNode(node);
        request.setTimestamp(ts);
        return request;
    }

    @Test
    @DisplayName("eventId 由稳定连接事实派生，重试不变")
    void eventId_shouldBeStableAcrossRetries() {
        Instant receivedAt = Instant.parse("2026-10-12T12:00:00Z");
        DeviceOnlineEvent first = factory.create(context, request("sess-1", "node-a", receivedAt.toEpochMilli()), receivedAt);
        DeviceOnlineEvent second = factory.create(context, request("sess-1", "node-a", receivedAt.toEpochMilli()), receivedAt);

        assertEquals(first.eventId(), second.eventId());
        assertEquals(DeviceOnlineEvent.TYPE, first.eventType());
    }

    @Test
    @DisplayName("不同 generation 或 sessionId 派生不同 eventId")
    void eventId_shouldDifferByStableFacts() {
        Instant receivedAt = Instant.parse("2026-10-12T12:00:00Z");
        String base = factory.create(context, request("sess-1", "node-a", null), receivedAt).eventId();
        String otherSession = factory.create(context, request("sess-2", "node-a", null), receivedAt).eventId();
        AdmissionSessionContext otherGen = new AdmissionSessionContext("DEVICE-1", "VIN1", "CERT",
                "client1", null, null, 13L, Instant.now(), Instant.now().plusSeconds(300));
        String otherGeneration = factory.create(otherGen, request("sess-1", "node-a", null), receivedAt).eventId();

        assertNotEquals(base, otherSession);
        assertNotEquals(base, otherGeneration);
    }

    @Test
    @DisplayName("合法 EMQX connected 时间 → timeSource=EMQX")
    void occurredAt_usesEmqxWhenWithinSkew() {
        Instant receivedAt = Instant.parse("2026-10-12T12:00:00Z");
        Instant emqx = receivedAt.minusSeconds(10);

        DeviceOnlineEvent event = factory.create(context, request("sess-1", "node-a", emqx.toEpochMilli()), receivedAt);

        assertEquals(emqx, event.occurredAt());
        assertEquals(DeviceOnlineEvent.TIME_SOURCE_EMQX, event.timeSource());
    }

    @Test
    @DisplayName("时间缺失或明显越界 → 回退 VAGW 接收时间并标记 VAGW_RECEIVED")
    void occurredAt_fallbackWhenMissingOrOutOfRange() {
        Instant receivedAt = Instant.parse("2026-10-12T12:00:00Z");

        DeviceOnlineEvent missing = factory.create(context, request("sess-1", "node-a", null), receivedAt);
        assertEquals(receivedAt, missing.occurredAt());
        assertEquals(DeviceOnlineEvent.TIME_SOURCE_VAGW_RECEIVED, missing.timeSource());

        DeviceOnlineEvent future = factory.create(context,
                request("sess-1", "node-a", receivedAt.plusSeconds(3600).toEpochMilli()), receivedAt);
        assertEquals(receivedAt, future.occurredAt());
        assertEquals(DeviceOnlineEvent.TIME_SOURCE_VAGW_RECEIVED, future.timeSource());
    }

    @Test
    @DisplayName("契约序列化：eventType 固定，可选 tboxSn/certSerial 缺失时省略")
    void toJson_shouldRespectContract() {
        DeviceOnlineEvent event = factory.create(context, request("sess-1", "node-a", null), Instant.now());
        String json = factory.toJson(event);

        assertTrue(json.contains("\"eventType\":\"DEVICE_ONLINE\""));
        assertTrue(json.contains("\"hsmUid\":\"DEVICE-1\""));
        assertFalse(json.contains("tboxSn"), "无可靠来源时 tboxSn 应省略");
    }

    @Test
    @DisplayName("身份摘要用于脱敏日志，不暴露完整 hsmUid")
    void identityDigest_shouldNotEqualRawIdentity() {
        String digest = DeviceOnlineEventFactory.identityDigest("DEVICE-1");
        assertNotNull(digest);
        assertNotEquals("DEVICE-1", digest);
        assertEquals(12, digest.length());
    }
}
