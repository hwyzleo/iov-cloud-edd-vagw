package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * AdmissionSessionContextRegistry 单元测试（EDD-VAGW-DSN-CR-012 §3.1/§3.4）。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AdmissionSessionContextRegistry 测试")
class AdmissionSessionContextRegistryTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final VagwDeviceOnlineProperties properties = new VagwDeviceOnlineProperties();

    private AdmissionSessionContextRegistry registry;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        registry = new AdmissionSessionContextRegistry(redisTemplate, objectMapper, properties);
    }

    @Test
    @DisplayName("ALLOW 后保存候选快照：归一化 hsmUid、原子分配 generation、有界 TTL")
    void saveCandidate_shouldNormalizeAndAllocateGeneration() {
        when(valueOperations.increment("vagw:session:gen:DEVICE-1")).thenReturn(7L);

        Optional<AdmissionSessionContext> saved = registry.saveCandidate(
                " device-1 ", "VIN1", "CERT", "client1", "10.0.0.1", 5);

        assertTrue(saved.isPresent());
        assertEquals("DEVICE-1", saved.get().hsmUid());
        assertEquals("VIN1", saved.get().vin());
        assertEquals(7L, saved.get().connectionGeneration());
        assertTrue(saved.get().expiresAt().isAfter(saved.get().admittedAt()));
        verify(valueOperations).set(eq("vagw:auth:admission:client1"), anyString(),
                eq(properties.getAdmissionTtlSeconds()), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("Redis 写失败即 fail-closed（返回空，不建立可信会话）")
    void saveCandidate_redisFailure_shouldReturnEmpty() {
        when(valueOperations.increment(anyString())).thenReturn(1L);
        doThrow(new RuntimeException("redis down")).when(valueOperations)
                .set(anyString(), anyString(), anyLong(), any());

        assertTrue(registry.saveCandidate("DEVICE-1", "VIN1", "CERT", "client1", null, null).isEmpty());
    }

    @Test
    @DisplayName("client.connected 关联候选快照：hsmUid 一致时命中")
    void findCandidate_shouldMatchNormalizedIdentity() throws Exception {
        AdmissionSessionContext context = new AdmissionSessionContext(
                "DEVICE-1", "VIN1", "CERT", "client1", null, null, 3L, Instant.now(),
                Instant.now().plusSeconds(300));
        when(valueOperations.get("vagw:auth:admission:client1"))
                .thenReturn(objectMapper.writeValueAsString(context));

        Optional<AdmissionSessionContext> found = registry.findCandidate("device-1", "client1");

        assertTrue(found.isPresent());
        assertEquals(3L, found.get().connectionGeneration());
    }

    @Test
    @DisplayName("Hook 自报身份与快照不一致时拒绝")
    void findCandidate_identityMismatch_shouldReturnEmpty() throws Exception {
        AdmissionSessionContext context = new AdmissionSessionContext(
                "DEVICE-1", "VIN1", "CERT", "client1", null, null, 3L, Instant.now(),
                Instant.now().plusSeconds(300));
        when(valueOperations.get("vagw:auth:admission:client1"))
                .thenReturn(objectMapper.writeValueAsString(context));

        assertTrue(registry.findCandidate("VICTIM-9", "client1").isEmpty());
    }
}
