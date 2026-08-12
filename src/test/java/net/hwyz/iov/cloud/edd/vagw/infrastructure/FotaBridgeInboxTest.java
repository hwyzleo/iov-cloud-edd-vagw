package net.hwyz.iov.cloud.edd.vagw.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * FotaBridgeInbox 单元测试（EDD-VAGW-DSN-CR-005 §8 幂等）。
 */
@ExtendWith(MockitoExtension.class)
class FotaBridgeInboxTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private FotaBridgeInbox fotaBridgeInbox;

    @Test
    void digestOf_shouldBeStableSha256Hex() {
        String d1 = FotaBridgeInbox.digestOf(new byte[]{1, 2, 3});
        String d2 = FotaBridgeInbox.digestOf(new byte[]{1, 2, 3});
        assertEquals(d1, d2);
        assertEquals(64, d1.length());
        assertNotEquals(d1, FotaBridgeInbox.digestOf(new byte[]{1, 2, 4}));
    }

    @Test
    void findFinal_noRecord_shouldReturnEmpty() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(anyString())).thenReturn(null);
        assertTrue(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-001").isEmpty());
    }

    @Test
    void findFinal_recordExists_shouldReturnRecord() throws Exception {
        FotaBridgeInbox.InboxRecord rec = FotaBridgeInbox.InboxRecord.builder()
                .direction("DOWNLINK")
                .messageId("msg-001")
                .payloadDigest("abc")
                .status("ACCEPTED")
                .build();
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:fota:inbox:downlink:msg-001")).thenReturn("{\"json\":\"payload\"}");
        when(objectMapper.readValue(anyString(), eq(FotaBridgeInbox.InboxRecord.class))).thenReturn(rec);

        Optional<FotaBridgeInbox.InboxRecord> found =
                fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-001");

        assertTrue(found.isPresent());
        assertEquals("ACCEPTED", found.get().getStatus());
    }

    @Test
    void recordFinal_firstWrite_shouldReturnTrue() throws Exception {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(valueOperations.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(true);

        FotaBridgeInbox.InboxRecord rec = FotaBridgeInbox.InboxRecord.builder()
                .direction("UPLINK")
                .messageId("msg-001")
                .status("ACCEPTED")
                .build();

        assertTrue(fotaBridgeInbox.recordFinal(rec));
    }

    @Test
    void recordFinal_duplicate_shouldReturnFalse() throws Exception {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(valueOperations.setIfAbsent(anyString(), anyString(), anyLong(), any())).thenReturn(false);

        FotaBridgeInbox.InboxRecord rec = FotaBridgeInbox.InboxRecord.builder()
                .direction("UPLINK")
                .messageId("msg-001")
                .status("ACCEPTED")
                .build();

        assertFalse(fotaBridgeInbox.recordFinal(rec));
    }
}
