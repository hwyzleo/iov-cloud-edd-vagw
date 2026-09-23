package net.hwyz.iov.cloud.edd.vagw.infrastructure.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 车辆桥接 Inbox 测试（EDD-VAGW-DSN-CR-006 §9）。
 */
class VehicleBridgeInboxTest {

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final VehicleBridgeInbox inbox = new VehicleBridgeInbox(redisTemplate, objectMapper);

    @SuppressWarnings("unchecked")
    private ValueOperations<String, String> mockValueOps() {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(ops);
        return ops;
    }

    @Test
    void digestOf_shouldReturnSha256() {
        String digest = VehicleBridgeInbox.digestOf(new byte[]{1, 2, 3});
        assertEquals(64, digest.length());
        assertNotEquals(VehicleBridgeInbox.digestOf(new byte[]{1, 2, 4}), digest);
    }

    @Test
    void recordFinal_shouldSetIfAbsent() {
        ValueOperations<String, String> ops = mockValueOps();
        when(ops.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true);

        boolean created = inbox.recordFinal(VehicleBridgeInbox.InboxRecord.builder()
                .direction(VehicleBridgeInbox.Direction.UPLINK.name())
                .messageId("m-1")
                .envelopeSha256("abc")
                .kafkaTopic("vagw.fota")
                .state(VehicleBridgeInbox.State.ACCEPTED.name())
                .outcome("OUTCOME_ACCEPTED")
                .updatedAt(Instant.now())
                .build());

        assertTrue(created);
        verify(ops).setIfAbsent(startsWith("vagw:bridge:inbox:uplink:"),
                anyString(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void findFinal_existing_shouldReturnRecord() throws Exception {
        ValueOperations<String, String> ops = mockValueOps();
        VehicleBridgeInbox.InboxRecord record = VehicleBridgeInbox.InboxRecord.builder()
                .direction("DOWNLINK")
                .messageId("m-2")
                .envelopeSha256("xyz")
                .state(VehicleBridgeInbox.State.ACCEPTED.name())
                .build();
        when(ops.get(startsWith("vagw:bridge:inbox:downlink:")))
                .thenReturn(objectMapper.writeValueAsString(record));

        Optional<VehicleBridgeInbox.InboxRecord> found =
                inbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "m-2");

        assertTrue(found.isPresent());
        assertEquals("xyz", found.get().getEnvelopeSha256());
    }

    @Test
    void findFinal_missing_shouldReturnEmpty() {
        ValueOperations<String, String> ops = mockValueOps();
        when(ops.get(anyString())).thenReturn(null);

        assertTrue(inbox.findFinal(VehicleBridgeInbox.Direction.UPLINK, "nope").isEmpty());
    }
}
