package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.config.VagwBindingProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.ViamResolveResult;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.ViamVehicleAccessIdentityClient;
import net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 绑定解析服务测试（EDD-VAGW-DSN-CR-007 §4/§10 + EDD-VAGW-DSN-CR-020 §4/§9）。
 * <p>
 * 覆盖：下行缓存命中/未命中回源、上行三级自愈解析（缓存 → VIAM 回源 → 收缩的
 * CONTEXT_MISSING）、回源身份不一致、回源治理（单飞/许可/超时）、会话续期与断连清理、
 * 准入 rememberAdmission、失效与 deprecated 适配器。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class BindingServiceTest {

    private static final String HSM_KEY = "vagw:binding:hsm:DEVICE001";
    private static final String VIN_KEY = "vagw:binding:vin:VIN-A";
    private static final String ANCHOR_KEY = "vagw:binding:hsm-anchor:DEVICE001";

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ViamVehicleAccessIdentityClient viamClient;

    private ObjectMapper objectMapper;
    private VagwBindingProperties properties;
    private BindingResolutionMetrics metrics;
    private BindingServiceImpl bindingService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        properties = new VagwBindingProperties();
        metrics = new BindingResolutionMetrics();
        bindingService = new BindingServiceImpl(redisTemplate, viamClient, objectMapper,
                properties, metrics);
        bindingService.initUplinkResolve();
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @AfterEach
    void tearDown() {
        bindingService.shutdownUplinkResolve();
    }

    // ------------------------------------------------------------------
    // resolveByVin（下行权威解析，行为不变）
    // ------------------------------------------------------------------

    @Test
    void resolveByVin_cacheHit_shouldReturnResolvedWithoutViam() throws Exception {
        when(valueOperations.get(VIN_KEY))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("DEVICE001", r.binding().getHsmUid());
        verifyNoInteractions(viamClient);
    }

    @Test
    void resolveByVin_cacheMiss_shouldFallbackToViam() {
        when(valueOperations.get(VIN_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 1L, Instant.now()));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("DEVICE001", r.binding().getHsmUid());
        assertEquals(1L, r.binding().getBindingVersion());
        verify(viamClient).resolveByVin("VIN-A");
    }

    @Test
    void resolveByVin_viamUnbound_shouldReturnUnbound() {
        when(valueOperations.get(VIN_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.unresolved("VIN-A", "UNBOUND"));

        BindingResolution r = bindingService.resolveByVin("VIN-A");

        assertEquals(ResolutionStatus.UNBOUND, r.status());
        assertEquals("UNBOUND", r.reason());
        assertNull(r.binding());
    }

    @Test
    void resolveByVin_viamConflict_shouldReturnConflict() {
        when(valueOperations.get(VIN_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.unresolved("VIN-A", "BINDING_CONFLICT"));

        assertEquals(ResolutionStatus.BINDING_CONFLICT, bindingService.resolveByVin("VIN-A").status());
    }

    @Test
    void resolveByVin_viamDependencyUnavailable_shouldReturnDependency() {
        when(valueOperations.get(VIN_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.unresolved("VIN-A", "DEPENDENCY_UNAVAILABLE"));

        assertEquals(ResolutionStatus.DEPENDENCY_UNAVAILABLE,
                bindingService.resolveByVin("VIN-A").status());
    }

    @Test
    void resolveByVin_blankInput_shouldReturnContextMissing() {
        assertEquals(ResolutionStatus.CONTEXT_MISSING, bindingService.resolveByVin("  ").status());
        verifyNoInteractions(viamClient);
    }

    // ------------------------------------------------------------------
    // resolveByHsmUid（只读缓存，不做回源）
    // ------------------------------------------------------------------

    @Test
    void resolveByHsmUid_cacheHit_shouldReturnResolvedWithoutViam() throws Exception {
        when(valueOperations.get(HSM_KEY))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        BindingResolution r = bindingService.resolveByHsmUid("DEVICE001");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("VIN-A", r.binding().getVin());
        verifyNoInteractions(viamClient);
    }

    @Test
    void resolveByHsmUid_cacheMiss_shouldReturnContextMissingWithoutViam() {
        when(valueOperations.get(HSM_KEY)).thenReturn(null);

        // 只读入口：基础设施/上下文缺失 ≠ 真实 UNBOUND，且不得为内部读路径引入回源
        assertEquals(ResolutionStatus.CONTEXT_MISSING,
                bindingService.resolveByHsmUid("DEVICE001").status());
        verifyNoInteractions(viamClient);
    }

    // ------------------------------------------------------------------
    // resolveForUplink（CR-020 三级解析 + 回源治理）
    // ------------------------------------------------------------------

    @Test
    @DisplayName("上行缓存命中：放行并按 min(cache-ttl, 会话锚点剩余) 滑动续期")
    void resolveForUplink_cacheHit_shouldResolveAndRenewWithCappedTtl() throws Exception {
        when(valueOperations.get(HSM_KEY))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));
        when(redisTemplate.getExpire(ANCHOR_KEY, TimeUnit.SECONDS)).thenReturn(100L);
        when(redisTemplate.expire(HSM_KEY, 100L, TimeUnit.SECONDS)).thenReturn(true);

        BindingResolution r = bindingService.resolveForUplink("DEVICE001", "VIN-A");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        verify(redisTemplate).expire(HSM_KEY, 100L, TimeUnit.SECONDS);
        verifyNoInteractions(viamClient);
        assertEquals(1L, metrics.get("vagw_binding_session_renew_total"));
    }

    @Test
    @DisplayName("上行缓存命中但关闭 renew-on-hit：不续期")
    void resolveForUplink_cacheHit_renewDisabled_shouldNotRenew() throws Exception {
        properties.getSessionRenew().setRenewOnHit(false);
        when(valueOperations.get(HSM_KEY))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        assertEquals(ResolutionStatus.RESOLVED,
                bindingService.resolveForUplink("DEVICE001", "VIN-A").status());
        verify(redisTemplate, never()).expire(anyString(), anyLong(), any(TimeUnit.class));
    }

    @Test
    @DisplayName("上行缓存未命中：回源 VIAM 成功并回写双向缓存（自愈）")
    void resolveForUplink_cacheMiss_shouldResolveViaViamAndWriteCache() {
        when(valueOperations.get(HSM_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 2L, Instant.now()));

        BindingResolution r = bindingService.resolveForUplink("DEVICE001", "VIN-A");

        assertEquals(ResolutionStatus.RESOLVED, r.status());
        assertEquals("VIN-A", r.binding().getVin());
        assertEquals(BindingSource.VIAM_RESOLVE, r.binding().getSource());
        verify(redisTemplate).execute(any(RedisCallback.class));
        verify(valueOperations).setIfAbsent(eq(ANCHOR_KEY), anyString(), eq(1800L), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("上行回源 hsmUid 与接入身份不一致：失效双向映射并按冲突拒绝（改绑）")
    void resolveForUplink_cacheMiss_hsmUidMismatch_shouldInvalidateAndConflict() {
        when(valueOperations.get(HSM_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.resolved("VIN-A", "DEVICE-OTHER", "PR-1", "TBOX-1", 3L, Instant.now()));

        BindingResolution r = bindingService.resolveForUplink("DEVICE001", "VIN-A");

        assertEquals(ResolutionStatus.BINDING_CONFLICT, r.status());
        verify(redisTemplate).delete(VIN_KEY);
        verify(redisTemplate).delete("vagw:binding:hsm:DEVICE-OTHER");
        verify(redisTemplate).delete(HSM_KEY);
        verify(redisTemplate, never()).execute(any(RedisCallback.class));
    }

    @Test
    @DisplayName("上行回源明确未绑定：UNBOUND（804008），不写缓存")
    void resolveForUplink_cacheMiss_unbound_shouldReturnUnbound() {
        when(valueOperations.get(HSM_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.unresolved("VIN-A", "VIN_UNKNOWN"));

        BindingResolution r = bindingService.resolveForUplink("DEVICE001", "VIN-A");

        assertEquals(ResolutionStatus.UNBOUND, r.status());
        assertEquals("VIN_UNKNOWN", r.reason());
        verify(redisTemplate, never()).execute(any(RedisCallback.class));
    }

    @Test
    @DisplayName("上行回源依赖不可用：DEPENDENCY_UNAVAILABLE（804014）")
    void resolveForUplink_cacheMiss_dependencyUnavailable_shouldReturnDependency() {
        when(valueOperations.get(HSM_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.unresolved("VIN-A", "DEPENDENCY_UNAVAILABLE"));

        BindingResolution r = bindingService.resolveForUplink("DEVICE001", "VIN-A");

        assertEquals(ResolutionStatus.DEPENDENCY_UNAVAILABLE, r.status());
        assertEquals(1L, metrics.get("vagw_binding_uplink_resolve_total{result=REJECTED,"
                + "reason=binding_dependency_unavailable}"));
    }

    @Test
    @DisplayName("Redis 读异常但回源成功：放行（缓存只是加速层）")
    void resolveForUplink_redisReadError_shouldFallbackToViam() {
        when(valueOperations.get(HSM_KEY)).thenThrow(new IllegalStateException("redis down"));
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 1L, Instant.now()));

        assertEquals(ResolutionStatus.RESOLVED,
                bindingService.resolveForUplink("DEVICE001", "VIN-A").status());
    }

    @Test
    @DisplayName("上行回源被配置关闭：退化为旧 CONTEXT_MISSING 行为且不调用 VIAM")
    void resolveForUplink_disabled_shouldReturnContextMissing() {
        properties.getUplinkResolve().setEnabled(false);
        when(valueOperations.get(HSM_KEY)).thenReturn(null);

        assertEquals(ResolutionStatus.CONTEXT_MISSING,
                bindingService.resolveForUplink("DEVICE001", "VIN-A").status());
        verifyNoInteractions(viamClient);
    }

    @Test
    @DisplayName("空 hsmUid / 空 Envelope.vin：CONTEXT_MISSING（无可信上下文）")
    void resolveForUplink_blankInputs_shouldReturnContextMissing() {
        assertEquals(ResolutionStatus.CONTEXT_MISSING,
                bindingService.resolveForUplink("  ", "VIN-A").status());
        assertEquals(ResolutionStatus.CONTEXT_MISSING,
                bindingService.resolveForUplink("DEVICE001", "  ").status());
        verifyNoInteractions(viamClient);
    }

    @Test
    @DisplayName("单飞：同 (hsmUid, vin) 并发未命中只产生一次 VIAM 调用")
    void resolveForUplink_concurrentMiss_shouldCallViamOnce() throws Exception {
        properties.getUplinkResolve().setMaxConcurrent(4);
        properties.getUplinkResolve().setDeadlineMs(2000);
        bindingService.initUplinkResolve();
        when(valueOperations.get(HSM_KEY)).thenReturn(null);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(viamClient.resolveByVin("VIN-A")).thenAnswer(inv -> {
            started.countDown();
            release.await(2, TimeUnit.SECONDS);
            return ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 1L, Instant.now());
        });

        ExecutorService callers = Executors.newFixedThreadPool(4);
        List<Future<BindingResolution>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            futures.add(callers.submit(() -> bindingService.resolveForUplink("DEVICE001", "VIN-A")));
        }
        assertTrue(started.await(2, TimeUnit.SECONDS));
        Thread.sleep(100);   // 让其余调用方进入单飞等待
        release.countDown();
        for (Future<BindingResolution> f : futures) {
            assertEquals(ResolutionStatus.RESOLVED, f.get(3, TimeUnit.SECONDS).status());
        }
        callers.shutdownNow();

        verify(viamClient, times(1)).resolveByVin("VIN-A");
    }

    @Test
    @DisplayName("回源许可耗尽：立即过载拒绝（DEPENDENCY_UNAVAILABLE）且不调用 VIAM")
    void resolveForUplink_overloaded_shouldRejectWithoutViam() throws Exception {
        properties.getUplinkResolve().setMaxConcurrent(1);
        properties.getUplinkResolve().setDeadlineMs(2000);
        bindingService.initUplinkResolve();
        when(valueOperations.get(anyString())).thenReturn(null);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(viamClient.resolveByVin("VIN-A")).thenAnswer(inv -> {
            started.countDown();
            release.await(2, TimeUnit.SECONDS);
            return ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 1L, Instant.now());
        });

        ExecutorService callers = Executors.newFixedThreadPool(2);
        Future<BindingResolution> first =
                callers.submit(() -> bindingService.resolveForUplink("DEVICE001", "VIN-A"));
        assertTrue(started.await(2, TimeUnit.SECONDS));

        BindingResolution overloaded =
                bindingService.resolveForUplink("DEVICE002", "VIN-B");

        assertEquals(ResolutionStatus.DEPENDENCY_UNAVAILABLE, overloaded.status());
        assertEquals("binding_resolve_overloaded", overloaded.reason());
        verify(viamClient, times(1)).resolveByVin(anyString());
        assertEquals(1L, metrics.get("vagw_binding_uplink_resolve_overloaded_total"));

        release.countDown();
        assertEquals(ResolutionStatus.RESOLVED, first.get(3, TimeUnit.SECONDS).status());
        callers.shutdownNow();
    }

    @Test
    @DisplayName("回源超 deadline：依赖不可用（binding_resolve_deadline_exceeded）")
    void resolveForUplink_deadlineExceeded_shouldReturnDependency() {
        properties.getUplinkResolve().setDeadlineMs(50);
        bindingService.initUplinkResolve();
        when(valueOperations.get(HSM_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A")).thenAnswer(inv -> {
            Thread.sleep(400);
            return ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 1L, Instant.now());
        });

        BindingResolution r = bindingService.resolveForUplink("DEVICE001", "VIN-A");

        assertEquals(ResolutionStatus.DEPENDENCY_UNAVAILABLE, r.status());
        assertEquals("binding_resolve_deadline_exceeded", r.reason());
        assertEquals(1L, metrics.get("vagw_binding_uplink_resolve_deadline_exceeded_total"));
    }

    // ------------------------------------------------------------------
    // 会话级续期/清理
    // ------------------------------------------------------------------

    @Test
    @DisplayName("断连清理：只删除 hsm 方向映射与会话锚点，不动 VIN 方向")
    void clearSessionMapping_shouldDeleteOnlyHsmAndAnchor() {
        bindingService.clearSessionMapping("DEVICE001");

        verify(redisTemplate).delete(HSM_KEY);
        verify(redisTemplate).delete(ANCHOR_KEY);
        verify(redisTemplate, never()).delete(VIN_KEY);
        assertEquals(1L, metrics.get("vagw_binding_session_clear_total"));
    }

    @Test
    @DisplayName("断连清理关闭：不执行删除")
    void clearSessionMapping_disabled_shouldSkip() {
        properties.getSessionRenew().setCleanOnDisconnect(false);

        bindingService.clearSessionMapping("DEVICE001");

        verify(redisTemplate, never()).delete(anyString());
    }

    @Test
    @DisplayName("续期只对已存在 key 生效：已失效（删/过期）的映射不会被复活")
    void renewSessionMapping_shouldNotResurrectDeletedKey() {
        when(redisTemplate.getExpire(ANCHOR_KEY, TimeUnit.SECONDS)).thenReturn(1800L);
        when(redisTemplate.expire(HSM_KEY, 300L, TimeUnit.SECONDS)).thenReturn(false);

        bindingService.renewSessionMapping("DEVICE001");

        verify(valueOperations, never()).set(eq(HSM_KEY), anyString(), anyLong(), any(TimeUnit.class));
        assertEquals(0L, metrics.get("vagw_binding_session_renew_total"));
    }

    // ------------------------------------------------------------------
    // rememberAdmission / invalidate
    // ------------------------------------------------------------------

    @Test
    void rememberAdmission_shouldWriteBidirectionalCacheAndAnchor() {
        bindingService.rememberAdmission("DEVICE001", "VIN-A", 3L);

        verify(redisTemplate).execute(any(RedisCallback.class));
        verify(valueOperations).setIfAbsent(eq(ANCHOR_KEY), anyString(), eq(1800L), eq(TimeUnit.SECONDS));
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

        verify(redisTemplate).delete(VIN_KEY);
        verify(redisTemplate).delete(HSM_KEY);
    }

    // ------------------------------------------------------------------
    // deprecated 兼容适配器（委托新实现，不再返回空桩）
    // ------------------------------------------------------------------

    @Test
    void deprecated_resolveVin_shouldDelegateToHsmCache() throws Exception {
        when(valueOperations.get(HSM_KEY))
                .thenReturn(objectMapper.writeValueAsString(binding("VIN-A", "DEVICE001", 1L)));

        Optional<String> vin = bindingService.resolveVin("DEVICE001");

        assertTrue(vin.isPresent());
        assertEquals("VIN-A", vin.get());
    }

    @Test
    void deprecated_resolveDeviceSn_shouldDelegateToViamFallback() {
        when(valueOperations.get(VIN_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.resolved("VIN-A", "DEVICE001", "PR-1", "TBOX-1", 1L, Instant.now()));

        Optional<String> deviceSn = bindingService.resolveDeviceSn("VIN-A");

        assertTrue(deviceSn.isPresent());
        assertEquals("DEVICE001", deviceSn.get());
    }

    @Test
    void deprecated_resolveDeviceSn_unbound_shouldReturnEmpty() {
        when(valueOperations.get(VIN_KEY)).thenReturn(null);
        when(viamClient.resolveByVin("VIN-A"))
                .thenReturn(ViamResolveResult.unresolved("VIN-A", "VIN_UNKNOWN"));

        assertTrue(bindingService.resolveDeviceSn("VIN-A").isEmpty());
    }

    @Test
    void deprecated_isValidAndBound_shouldDelegate() {
        when(valueOperations.get(HSM_KEY)).thenReturn(null);

        assertFalse(bindingService.isValidAndBound("DEVICE001"));
    }

    private VehicleAccessBinding binding(String vin, String hsmUid, long version) {
        return VehicleAccessBinding.builder()
                .vin(vin)
                .hsmUid(hsmUid)
                .tboxSn("TBOX-1")
                .bindingVersion(version)
                .bindingUpdatedAt(Instant.now())
                .source(BindingSource.VIAM_ADMISSION)
                .build();
    }
}
