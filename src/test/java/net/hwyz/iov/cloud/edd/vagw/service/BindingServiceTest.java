package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.TspResolveResult;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.TspVehicleAccessIdentityClient;
import net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 绑定解析服务测试（EDD-VAGW-DSN-CR-007 §4/§10）。
 * <p>
 * 覆盖：版本化缓存命中/未命中回源 TSP、UNBOUND/CONFLICT/DEPENDENCY 映射、
 * 上行 CONTEXT_MISSING 区分、准入 rememberAdmission、失效与 deprecated 适配器。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class BindingServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private TspVehicleAccessIdentityClient tspClient;

    private ObjectMapper objectMapper;
    private BindingServiceImpl bindingService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        bindingService = new BindingServiceImpl(redisTemplate, tspClient, objectMapper);
        ReflectionTestUtils.setField(bindingService, "cacheTtlSeconds", 300L);
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    // ------------------------------------------------------------------
    // resolveByVin（下行权威解析）
    // ------------------------------------------------------------------

    @Test
    void resolveByVin_cacheHit_shouldReturnResolvedWithoutTsp() throws Exception {
        when(valueOperations.get("vagw:binding:vin:VIN-A"))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("DEVICE001", r.binding().getHsmUid());
        verifyNoInteractions(tspClient);
    }

    @Test
    void resolveByVin_cacheMiss_shouldFallbackToTsp() {
        when(valueOperations.get("vagw:binding:vin:VIN-A")).thenReturn(null);
        when(tspClient.resolveByVin("VIN-A"))
                .thenReturn(TspResolveResult.resolved("VIN-A", "DEVICE001", "TBOX-1", 1L, Instant.now()));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("DEVICE001", r.binding().getHsmUid());
        assertEquals(1L, r.binding().getBindingVersion());
        verify(tspClient).resolveByVin("VIN-A");
    }

    @Test
    void resolveByVin_tspUnbound_shouldReturnUnbound() {
        when(valueOperations.get("vagw:binding:vin:VIN-A")).thenReturn(null);
        when(tspClient.resolveByVin("VIN-A"))
                .thenReturn(TspResolveResult.unresolved("VIN-A", "UNBOUND"));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.UNBOUND, r.status());
        assertEquals("UNBOUND", r.reason());
        assertNull(r.binding());
    }

    @Test
    void resolveByVin_tspConflict_shouldReturnConflict() {
        when(valueOperations.get("vagw:binding:vin:VIN-A")).thenReturn(null);
        when(tspClient.resolveByVin("VIN-A"))
                .thenReturn(TspResolveResult.unresolved("VIN-A", "BINDING_CONFLICT"));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.BINDING_CONFLICT, r.status());
    }

    @Test
    void resolveByVin_tspDependencyUnavailable_shouldReturnDependency() {
        when(valueOperations.get("vagw:binding:vin:VIN-A")).thenReturn(null);
        when(tspClient.resolveByVin("VIN-A"))
                .thenReturn(TspResolveResult.unresolved("VIN-A", "DEPENDENCY_UNAVAILABLE"));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.DEPENDENCY_UNAVAILABLE, r.status());
    }

    @Test
    void resolveByVin_blankInput_shouldReturnContextMissing() {
        BindingResolution r = bindingService.resolveByVin("  ");

        assertEquals(ResolutionStatus.CONTEXT_MISSING, r.status());
        verifyNoInteractions(tspClient);
    }

    // ------------------------------------------------------------------
    // resolveByHsmUid（上行会话绑定）
    // ------------------------------------------------------------------

    @Test
    void resolveByHsmUid_cacheHit_shouldReturnResolved() throws Exception {
        when(valueOperations.get("vagw:binding:hsm:DEVICE001"))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        BindingResolution r = bindingService.resolveByHsmUid("DEVICE001");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("VIN-A", r.binding().getVin());
    }

    @Test
    void resolveByHsmUid_cacheMiss_shouldReturnContextMissingNotUnbound() {
        when(valueOperations.get("vagw:binding:hsm:DEVICE001")).thenReturn(null);

        BindingResolution r = bindingService.resolveByHsmUid("DEVICE001");

        // 基础设施/上下文缺失 ≠ 真实 UNBOUND（不得静默等同）
        assertEquals(ResolutionStatus.CONTEXT_MISSING, r.status());
        verifyNoInteractions(tspClient);
    }

    // ------------------------------------------------------------------
    // rememberAdmission / invalidate
    // ------------------------------------------------------------------

    @Test
    void rememberAdmission_shouldAttemptBidirectionalWrite() {
        bindingService.rememberAdmission("DEVICE001", "VIN-A", 3L);

        verify(redisTemplate).execute(any(RedisCallback.class));
    }

    @Test
    void rememberAdmission_blankInput_shouldSkipWrite() {
        bindingService.rememberAdmission("", "VIN-A", null);
        bindingService.rememberAdmission("DEVICE001", null, null);

        verify(redisTemplate, never()).execute(any(RedisCallback.class));
    }

    @Test
    void invalidate_shouldDeleteBothKeys() {
        bindingService.invalidate("VIN-A", "DEVICE001", InvalidateReason.REBIND);

        verify(redisTemplate).delete("vagw:binding:vin:VIN-A");
        verify(redisTemplate).delete("vagw:binding:hsm:DEVICE001");
    }

    // ------------------------------------------------------------------
    // deprecated 兼容适配器（委托新实现，不再返回空桩）
    // ------------------------------------------------------------------

    @Test
    void deprecated_resolveVin_shouldDelegateToHsmCache() throws Exception {
        when(valueOperations.get("vagw:binding:hsm:DEVICE001"))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        Optional<String> vin = bindingService.resolveVin("DEVICE001");

        assertTrue(vin.isPresent());
        assertEquals("VIN-A", vin.get());
    }

    @Test
    void deprecated_resolveDeviceSn_shouldDelegateToTspFallback() {
        when(valueOperations.get("vagw:binding:vin:VIN-A")).thenReturn(null);
        when(tspClient.resolveByVin("VIN-A"))
                .thenReturn(TspResolveResult.resolved("VIN-A", "DEVICE001", "TBOX-1", 1L, Instant.now()));

        Optional<String> deviceSn = bindingService.resolveDeviceSn("VIN-A");

        assertTrue(deviceSn.isPresent());
        assertEquals("DEVICE001", deviceSn.get());
    }

    @Test
    void deprecated_resolveDeviceSn_unbound_shouldReturnEmpty() {
        when(valueOperations.get("vagw:binding:vin:VIN-A")).thenReturn(null);
        when(tspClient.resolveByVin("VIN-A"))
                .thenReturn(TspResolveResult.unresolved("VIN-A", "VIN_UNKNOWN"));

        assertTrue(bindingService.resolveDeviceSn("VIN-A").isEmpty());
    }

    @Test
    void deprecated_isValidAndBound_shouldDelegate() {
        when(valueOperations.get("vagw:binding:hsm:DEVICE001")).thenReturn(null);

        assertFalse(bindingService.isValidAndBound("DEVICE001"));
    }

    private VehicleAccessBinding binding(String vin, String hsmUid, long version) {
        return VehicleAccessBinding.builder()
                .vin(vin)
                .hsmUid(hsmUid)
                .tboxSn("TBOX-1")
                .bindingVersion(version)
                .bindingUpdatedAt(Instant.now())
                .source(BindingSource.ADMISSION)
                .build();
    }
}
