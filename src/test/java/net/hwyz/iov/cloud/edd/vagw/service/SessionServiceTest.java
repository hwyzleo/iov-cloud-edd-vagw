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

    @Mock
    private BindingService bindingService;

    private ObjectMapper objectMapper;

    @InjectMocks
    private SessionService sessionService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        sessionService = new SessionService(redisTemplate, objectMapper, bindingService);
    }

    @Test
    void onConnected_shouldSaveSessionWithDeviceSn() throws Exception {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("VIN001"));

        sessionService.onConnected("DEVICE001", "client001", "127.0.0.1", 5);

        verify(valueOperations).set(eq("vagw:session:DEVICE001"), anyString(), eq(24L), eq(TimeUnit.HOURS));
    }

    @Test
    void onConnected_shouldSaveSessionWhenVinNotBound() throws Exception {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.empty());

        sessionService.onConnected("DEVICE001", "client001", "127.0.0.1", 5);

        verify(valueOperations).set(eq("vagw:session:DEVICE001"), anyString(), eq(24L), eq(TimeUnit.HOURS));
    }

    @Test
    void getSessionByDeviceSn_shouldReturnSessionWhenExists() throws Exception {
        String json = "{\"deviceSn\":\"DEVICE001\",\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(json);

        Optional<SessionInfo> result = sessionService.getSessionByDeviceSn("DEVICE001");

        assertTrue(result.isPresent());
        assertEquals("DEVICE001", result.get().getDeviceSn());
        assertEquals("VIN001", result.get().getVin());
        assertTrue(result.get().isOnline());
    }

    @Test
    void getSessionByDeviceSn_shouldReturnEmptyWhenNotExists() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(null);

        Optional<SessionInfo> result = sessionService.getSessionByDeviceSn("DEVICE001");

        assertTrue(result.isEmpty());
    }

    @Test
    void getSessionByVin_shouldReturnSessionWhenBound() throws Exception {
        String json = "{\"deviceSn\":\"DEVICE001\",\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(json);

        Optional<SessionInfo> result = sessionService.getSessionByVin("VIN001");

        assertTrue(result.isPresent());
        assertEquals("DEVICE001", result.get().getDeviceSn());
        assertEquals("VIN001", result.get().getVin());
    }

    @Test
    void getSessionByVin_shouldReturnEmptyWhenNotBound() {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.empty());

        Optional<SessionInfo> result = sessionService.getSessionByVin("VIN001");

        assertTrue(result.isEmpty());
    }

    @Test
    void isOnlineByDeviceSn_shouldReturnTrueWhenOnline() throws Exception {
        String json = "{\"deviceSn\":\"DEVICE001\",\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(json);

        assertTrue(sessionService.isOnlineByDeviceSn("DEVICE001"));
    }

    @Test
    void isOnlineByDeviceSn_shouldReturnFalseWhenNoSession() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(null);

        assertFalse(sessionService.isOnlineByDeviceSn("DEVICE001"));
    }

    @Test
    void isOnlineByVin_shouldReturnTrueWhenOnline() throws Exception {
        String json = "{\"deviceSn\":\"DEVICE001\",\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(json);

        assertTrue(sessionService.isOnlineByVin("VIN001"));
    }

    @Test
    void isOnlineByVin_shouldReturnFalseWhenNotBound() {
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.empty());

        assertFalse(sessionService.isOnlineByVin("VIN001"));
    }

    @Test
    void isOnline_deprecated_shouldTryDeviceSnFirst() throws Exception {
        String json = "{\"deviceSn\":\"DEVICE001\",\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(json);

        assertTrue(sessionService.isOnline("DEVICE001"));
        verify(bindingService, never()).resolveDeviceSn(anyString());
    }

    @Test
    void isOnline_deprecated_shouldFallbackToVin() throws Exception {
        String json = "{\"deviceSn\":\"DEVICE001\",\"vin\":\"VIN001\",\"clientId\":\"client001\",\"online\":true,\"connectedAt\":\"2026-01-01T00:00:00Z\",\"lastSeen\":\"2026-01-01T00:00:00Z\"}";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:session:VIN001")).thenReturn(null);
        when(bindingService.resolveDeviceSn("VIN001")).thenReturn(Optional.of("DEVICE001"));
        when(valueOperations.get("vagw:session:DEVICE001")).thenReturn(json);

        assertTrue(sessionService.isOnline("VIN001"));
    }
}
