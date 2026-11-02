package net.hwyz.iov.cloud.edd.vagw.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * VagwSelfConnectionRegistry 单元测试。
 * <p>
 * 覆盖 bypass 放行时写 TTL 标记、Hook 命中/未命中判定与 Redis 故障 fail-closed。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("VagwSelfConnectionRegistry 测试")
class VagwSelfConnectionRegistryTest {

    private static final String CLIENT_ID = "vehicle-access-gatewaytest0001";

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private VagwSelfConnectionRegistry registry;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        registry = new VagwSelfConnectionRegistry(redisTemplate);
    }

    @Test
    @DisplayName("bypass 放行后写标记：有界 TTL，空 clientId 忽略")
    void mark_shouldPersistWithTtl() {
        registry.mark(CLIENT_ID);

        verify(valueOperations).set("vagw:auth:service:" + CLIENT_ID, "1", 300L, TimeUnit.SECONDS);

        registry.mark("  ");
        registry.mark(null);
        verify(valueOperations, times(1)).set(anyString(), anyString(), anyLong(), any());
    }

    @Test
    @DisplayName("Redis 写失败仅告警不抛出（不阻断已建立的 bypass 信任）")
    void mark_redisFailure_shouldNotThrow() {
        doThrow(new RuntimeException("redis down")).when(valueOperations)
                .set(anyString(), anyString(), anyLong(), any());

        assertDoesNotThrow(() -> registry.mark(CLIENT_ID));
    }

    @Test
    @DisplayName("Hook 命中标记即视为 VAGW 自身连接")
    void isSelfConnection_hit_shouldReturnTrue() {
        when(valueOperations.get("vagw:auth:service:" + CLIENT_ID)).thenReturn("1");

        assertTrue(registry.isSelfConnection(CLIENT_ID));
    }

    @Test
    @DisplayName("无标记或空 clientId：判定为非自身连接")
    void isSelfConnection_miss_shouldReturnFalse() {
        when(valueOperations.get("vagw:auth:service:" + CLIENT_ID)).thenReturn(null);

        assertFalse(registry.isSelfConnection(CLIENT_ID));
        assertFalse(registry.isSelfConnection(null));
        assertFalse(registry.isSelfConnection(" "));
    }

    @Test
    @DisplayName("Redis 读失败 fail-closed：不信任未经正向证据的连接")
    void isSelfConnection_redisFailure_shouldReturnFalse() {
        when(valueOperations.get(anyString())).thenThrow(new RuntimeException("redis down"));

        assertFalse(registry.isSelfConnection(CLIENT_ID));
    }
}
