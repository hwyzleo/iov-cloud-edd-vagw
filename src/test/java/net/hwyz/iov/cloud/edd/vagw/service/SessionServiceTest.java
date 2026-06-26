package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SessionServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private ObjectMapper objectMapper;

    @InjectMocks
    private SessionService sessionService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        // Re-inject since @InjectMocks won't use our configured ObjectMapper
        sessionService = new SessionService(redisTemplate, objectMapper);
    }

    @Test
    void onConnected_shouldSaveSession() throws Exception {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        sessionService.onConnected("VIN001", "client001", "127.0.0.1", 5);

        verify(valueOperations).set(eq("vagw:session:VIN001"), anyString(), eq(24L), eq(TimeUnit.HOURS));
    }

    @Test
    void isOnline_shouldReturnTrueWhenOnline() throws Exception {
        String json = "{\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:VIN001")).thenReturn(json);

        assertTrue(sessionService.isOnline("VIN001"));
    }

    @Test
    void isOnline_shouldReturnFalseWhenNoSession() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:VIN001")).thenReturn(null);

        assertFalse(sessionService.isOnline("VIN001"));
    }

    @Test
    void getSession_shouldReturnSessionWhenExists() throws Exception {
        String json = "{\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:VIN001")).thenReturn(json);

        Optional<SessionInfo> result = sessionService.getSession("VIN001");

        assertTrue(result.isPresent());
        assertEquals("VIN001", result.get().getVin());
        assertTrue(result.get().isOnline());
    }
}
