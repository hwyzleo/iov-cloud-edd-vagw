package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwBindingProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.ViamResolveResult;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.ViamVehicleAccessIdentityClient;
import net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 设备绑定解析服务实现（EDD-VAGW-DSN-CR-007 §4/§5，owner 由 EDD-VAGW-DSN-CR-019 切换 VIAM，
 * 上行自愈与会话级生命周期由 EDD-VAGW-DSN-CR-020 §4 补充）。
 * <p>
 * VIN → hsmUid 解析（下行）：版本化缓存（有界 TTL）→ 未命中回源 VIAM resolveByVin →
 * 校验后原子写入双向缓存。
 * </p>
 * <p>
 * hsmUid → VIN 解析（上行，{@link #resolveForUplink(String, String)}）三级流程：
 * L1 会话级缓存命中（滑动续期，受单会话上限封顶）→ L2 以 Envelope.vin 回源 VIAM
 * resolveByVin（单飞 / 有界并发 / 独立 deadline）并做双向校验 → L3 收缩的
 * CONTEXT_MISSING（仅“无法获得且无法重建”）。Envelope.vin 只是解析输入，不是身份声明。
 * </p>
 * <p>
 * 缓存语义（CR-020 §5）：{@code cache-ttl-seconds} 只是加速层陈旧上限，<b>不再是映射业务
 * 可用期</b>；hsm 方向映射有效期与会话对齐，断连清理。Redis 不可用或写失败仅影响加速，
 * 不伪造未绑定；VIAM 绑定投影仍是权威查询入口，安全失效事件仍是唯一权威失效路径。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BindingServiceImpl implements BindingService {

    private static final String VIN_KEY_PREFIX = "vagw:binding:vin:";
    private static final String HSM_KEY_PREFIX = "vagw:binding:hsm:";
    /** 单会话映射上限锚点：首次写入时间，TTL=session-max-ttl；滑动续期不得早于该时间退出 */
    private static final String HSM_ANCHOR_PREFIX = "vagw:binding:hsm-anchor:";

    private static final String DIRECTION_VIN = "vin";
    private static final String DIRECTION_HSM = "hsm";

    /** 回源治理细分 reason（指标保留这些稳定标签，其余依赖失败归并） */
    private static final java.util.Set<String> RESOLVE_GOVERNANCE_REASONS = java.util.Set.of(
            "binding_resolve_overloaded",
            "binding_resolve_deadline_exceeded",
            "binding_resolve_interrupted",
            "binding_resolve_failed");

    private final StringRedisTemplate redisTemplate;
    private final ViamVehicleAccessIdentityClient viamClient;
    private final ObjectMapper objectMapper;
    private final VagwBindingProperties properties;
    private final BindingResolutionMetrics metrics;

    /** 上行回源许可集合（CR-020 §4.3：耗尽即过载拒绝，不调用 VIAM） */
    private Semaphore uplinkResolvePermits;
    /** 上行回源执行器（有界线程 + 有界队列，禁止 CallerRuns / 无界等待） */
    private ExecutorService uplinkResolveExecutor;
    /** 单飞合并表：同一 (hsmUid, vin) 并发未命中只产生一次 VIAM 调用 */
    private final ConcurrentHashMap<String, CompletableFuture<BindingResolution>> inFlightResolves =
            new ConcurrentHashMap<>();

    @PostConstruct
    void initUplinkResolve() {
        int maxConcurrent = properties.getUplinkResolve().getMaxConcurrent();
        this.uplinkResolvePermits = new Semaphore(maxConcurrent);
        AtomicInteger seq = new AtomicInteger();
        this.uplinkResolveExecutor = new ThreadPoolExecutor(
                maxConcurrent, maxConcurrent, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maxConcurrent),
                r -> {
                    Thread t = new Thread(r, "vagw-binding-resolve-" + seq.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    @PreDestroy
    void shutdownUplinkResolve() {
        if (uplinkResolveExecutor != null) {
            uplinkResolveExecutor.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // VIN → hsmUid（下行权威解析）
    // ------------------------------------------------------------------

    @Override
    public BindingResolution resolveByVin(String vin) {
        if (vin == null || vin.isBlank()) {
            return BindingResolution.contextMissing("blank vin");
        }
        try {
            Optional<VehicleAccessBinding> cached = readCache(VIN_KEY_PREFIX, vin);
            metrics.cache(DIRECTION_VIN, cached.isPresent());
            if (cached.isPresent()) {
                log.debug("Binding cache hit (vin): vin={}, hsmUid={}", LogMask.mask(vin),
                        LogMask.mask(cached.get().getHsmUid()));
                return BindingResolution.resolved(cached.get());
            }
            // 缓存未命中 → VIAM 回源
            log.debug("Binding cache miss (vin), fallback to VIAM: vin={}", LogMask.mask(vin));
            return mapViamResult(viamClient.resolveByVin(vin));
        } catch (Exception e) {
            log.error("Failed to resolve binding by vin={}", LogMask.mask(vin), e);
            return BindingResolution.dependencyUnavailable("cache read failed");
        }
    }

    // ------------------------------------------------------------------
    // hsmUid → VIN（只读缓存；内部会话富化等场景）
    // ------------------------------------------------------------------

    @Override
    public BindingResolution resolveByHsmUid(String hsmUid) {
        if (hsmUid == null || hsmUid.isBlank()) {
            return BindingResolution.contextMissing("blank hsmUid");
        }
        try {
            Optional<VehicleAccessBinding> cached = readCache(HSM_KEY_PREFIX, hsmUid);
            metrics.cache(DIRECTION_HSM, cached.isPresent());
            if (cached.isPresent()) {
                log.debug("Binding cache hit (hsm): hsmUid={}, vin={}", LogMask.mask(hsmUid),
                        LogMask.mask(cached.get().getVin()));
                return BindingResolution.resolved(cached.get());
            }
            // 只读入口：缺失即基础设施/上下文状态，非真实 UNBOUND
            log.debug("Binding cache miss (hsm, read-only path): hsmUid={}", LogMask.mask(hsmUid));
            return BindingResolution.contextMissing("no session/cache binding for hsmUid");
        } catch (Exception e) {
            log.error("Failed to resolve binding by hsmUid={}", LogMask.mask(hsmUid), e);
            return BindingResolution.contextMissing("cache read failed");
        }
    }

    // ------------------------------------------------------------------
    // 上行绑定解析（CR-020 §4.1：缓存 → 回源 → 收缩的 CONTEXT_MISSING）
    // ------------------------------------------------------------------

    @Override
    public BindingResolution resolveForUplink(String hsmUid, String envelopeVin) {
        if (hsmUid == null || hsmUid.isBlank()) {
            metrics.uplinkResolve("REJECTED", BindingResolutionMetrics.REASON_CONTEXT_MISSING);
            return BindingResolution.contextMissing("blank hsmUid");
        }

        // L1 会话级缓存
        Optional<VehicleAccessBinding> cached = safeReadHsmCache(hsmUid);
        metrics.cache(DIRECTION_HSM, cached.isPresent());
        if (cached.isPresent()) {
            if (properties.getSessionRenew().isEnabled()
                    && properties.getSessionRenew().isRenewOnHit()) {
                renewSessionMapping(hsmUid);
            }
            metrics.uplinkResolve("RESOLVED", "cache_hit");
            return BindingResolution.resolved(cached.get());
        }

        // L2 回源（可关闭：非生产排障回退到旧行为）
        if (!properties.getUplinkResolve().isEnabled()) {
            log.warn("Uplink binding resolve disabled by config, cache-only path: hsmUid={}",
                    LogMask.mask(hsmUid));
            metrics.uplinkResolve("REJECTED", BindingResolutionMetrics.REASON_CONTEXT_MISSING);
            return BindingResolution.contextMissing("uplink resolve disabled and cache missing");
        }
        if (envelopeVin == null || envelopeVin.isBlank()) {
            // 正常链路已由前置契约校验拦截（envelope_vin_missing），此处为防御
            metrics.uplinkResolve("REJECTED", BindingResolutionMetrics.REASON_CONTEXT_MISSING);
            return BindingResolution.contextMissing("blank envelope vin, cannot resolve");
        }

        BindingResolution resolution = properties.getUplinkResolve().isSingleFlight()
                ? resolveSingleFlight(hsmUid, envelopeVin)
                : resolveFromViam(hsmUid, envelopeVin);

        metrics.uplinkResolve(
                resolution.status() == ResolutionStatus.RESOLVED ? "RESOLVED" : "REJECTED",
                resolution.status() == ResolutionStatus.RESOLVED
                        ? "viam_resolve" : reasonTag(resolution));
        if (resolution.status() == ResolutionStatus.RESOLVED) {
            log.info("Binding resolved for uplink by VIAM fallback: hsmUid={}, vin={}, version={}",
                    LogMask.mask(hsmUid), LogMask.mask(resolution.binding().getVin()),
                    resolution.binding().getBindingVersion());
        } else {
            log.warn("Binding resolve for uplink failed: hsmUid={}, status={}, reason={}",
                    LogMask.mask(hsmUid), resolution.status(), resolution.reason());
        }
        return resolution;
    }

    /** 单飞：同一 (hsmUid, vin) 并发未命中合并为一次 VIAM 调用（有界等待）。 */
    private BindingResolution resolveSingleFlight(String hsmUid, String envelopeVin) {
        String key = hsmUid + "|" + envelopeVin;
        CompletableFuture<BindingResolution> mine = new CompletableFuture<>();
        CompletableFuture<BindingResolution> leader = inFlightResolves.putIfAbsent(key, mine);
        if (leader != null) {
            try {
                return leader.get(properties.getUplinkResolve().getDeadlineMs(),
                        TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                metrics.uplinkResolveDeadlineExceeded();
                return BindingResolution.dependencyUnavailable("binding_resolve_deadline_exceeded");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return BindingResolution.dependencyUnavailable("binding_resolve_interrupted");
            } catch (ExecutionException e) {
                return BindingResolution.dependencyUnavailable("binding_resolve_failed");
            }
        }
        try {
            BindingResolution resolution = resolveFromViam(hsmUid, envelopeVin);
            mine.complete(resolution);
            return resolution;
        } catch (RuntimeException e) {
            BindingResolution failed =
                    BindingResolution.dependencyUnavailable("binding_resolve_failed");
            mine.complete(failed);
            return failed;
        } finally {
            inFlightResolves.remove(key, mine);
        }
    }

    /** 一次有界回源调用：许可 → 执行器 → deadline → 双向校验 → 回写。 */
    private BindingResolution resolveFromViam(String hsmUid, String envelopeVin) {
        if (!uplinkResolvePermits.tryAcquire()) {
            metrics.uplinkResolveOverloaded();
            return BindingResolution.dependencyUnavailable("binding_resolve_overloaded");
        }
        try {
            Future<ViamResolveResult> future = uplinkResolveExecutor.submit(
                    () -> viamClient.resolveByVin(envelopeVin));
            ViamResolveResult result;
            try {
                result = future.get(properties.getUplinkResolve().getDeadlineMs(),
                        TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                metrics.uplinkResolveDeadlineExceeded();
                return BindingResolution.dependencyUnavailable("binding_resolve_deadline_exceeded");
            } catch (InterruptedException e) {
                future.cancel(true);
                Thread.currentThread().interrupt();
                return BindingResolution.dependencyUnavailable("binding_resolve_interrupted");
            } catch (ExecutionException e) {
                log.warn("VIAM resolve failed for uplink: vin={}, err={}",
                        LogMask.mask(envelopeVin),
                        e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
                return BindingResolution.dependencyUnavailable("binding_resolve_failed");
            }

            if (result == null) {
                return BindingResolution.dependencyUnavailable("null VIAM result");
            }
            if (result.getStatus() != ViamResolveResult.Status.RESOLVED) {
                // UNBOUND / BINDING_CONFLICT / DEPENDENCY_UNAVAILABLE（不写缓存）
                return mapUnresolved(result);
            }
            // 双向校验：回源 hsmUid 必须等于接入身份（Topic device key＝Envelope.device_id）
            if (result.getHsmUid() == null || !hsmUid.equalsIgnoreCase(result.getHsmUid())) {
                log.warn("Binding conflict: VIN already bound to another device: hsmUid={}, vin={}, "
                                + "resolvedHsmUid={}", LogMask.mask(hsmUid), LogMask.mask(envelopeVin),
                        LogMask.mask(result.getHsmUid()));
                // VIN 已改绑：原子失效双向 key，并清理请求设备可能残留的旧映射（旧设备不得继续上行）
                invalidate(result.getVin(), result.getHsmUid(), InvalidateReason.IDENTITY_MISMATCH);
                deleteHsmKeyQuietly(hsmUid);
                return BindingResolution.conflict();
            }
            VehicleAccessBinding binding = VehicleAccessBinding.builder()
                    .vin(result.getVin())
                    .hsmUid(result.getHsmUid())
                    .tboxSn(result.getTboxSn())
                    .bindingVersion(result.getBindingVersion() == null ? 0L : result.getBindingVersion())
                    .bindingUpdatedAt(result.getBindingUpdatedAt() == null
                            ? Instant.now() : result.getBindingUpdatedAt())
                    .source(BindingSource.VIAM_RESOLVE)
                    .build();
            writeBidirectional(binding);
            return BindingResolution.resolved(binding);
        } finally {
            uplinkResolvePermits.release();
        }
    }

    // ------------------------------------------------------------------
    // 会话级映射续期 / 清理（CR-020 §4.4）
    // ------------------------------------------------------------------

    @Override
    public void renewSessionMapping(String hsmUid) {
        if (hsmUid == null || hsmUid.isBlank()) {
            return;
        }
        if (!properties.getSessionRenew().isEnabled()) {
            return;
        }
        try {
            long ttl = boundedSessionTtl(hsmUid);
            // expire 仅对已存在的 key 生效（key 已失效/被 DEL 时不复活）
            Boolean renewed = redisTemplate.expire(HSM_KEY_PREFIX + hsmUid, ttl, TimeUnit.SECONDS);
            if (Boolean.TRUE.equals(renewed)) {
                metrics.sessionRenewed();
                log.debug("Session binding renewed: hsmUid={}, ttl={}s", LogMask.mask(hsmUid), ttl);
            }
        } catch (Exception e) {
            log.warn("Failed to renew session binding: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
        }
    }

    @Override
    public void clearSessionMapping(String hsmUid) {
        if (hsmUid == null || hsmUid.isBlank()) {
            return;
        }
        if (!properties.getSessionRenew().isEnabled()
                || !properties.getSessionRenew().isCleanOnDisconnect()) {
            return;
        }
        try {
            // 只清 hsm 方向会话级映射与单会话锚点；VIN 方向映射供下行解析，安全失效语义独立
            redisTemplate.delete(HSM_KEY_PREFIX + hsmUid);
            redisTemplate.delete(HSM_ANCHOR_PREFIX + hsmUid);
            metrics.sessionCleared();
            log.debug("Session binding cleared on disconnect: hsmUid={}", LogMask.mask(hsmUid));
        } catch (Exception e) {
            log.warn("Failed to clear session binding: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
        }
    }

    /**
     * 有界单会话 TTL：{@code min(cache-ttl, 锚点剩余)}；锚点缺失时先建立。
     * 返回值恒 &gt; 0，避免把 key 写成永久缓存。
     */
    private long boundedSessionTtl(String hsmUid) {
        long cacheTtl = properties.getCacheTtlSeconds();
        long sessionMax = properties.getSessionMaxTtlSeconds();
        try {
            String anchorKey = HSM_ANCHOR_PREFIX + hsmUid;
            Long remaining = redisTemplate.getExpire(anchorKey, TimeUnit.SECONDS);
            if (remaining == null || remaining < 0) {
                // 锚点缺失（旧数据/被清理）：以当前时刻为会话内首次写入
                ensureAnchor(hsmUid);
                remaining = sessionMax;
            }
            return Math.max(1L, Math.min(cacheTtl, remaining));
        } catch (Exception e) {
            log.debug("Failed to read session anchor, fallback to cache ttl: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
            return cacheTtl;
        }
    }

    /** 建立单会话上限锚点（SET NX，首次写入时间不被覆盖）。 */
    private void ensureAnchor(String hsmUid) {
        try {
            redisTemplate.opsForValue().setIfAbsent(HSM_ANCHOR_PREFIX + hsmUid,
                    String.valueOf(Instant.now().getEpochSecond()),
                    properties.getSessionMaxTtlSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.debug("Failed to write session anchor: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 准入/失效
    // ------------------------------------------------------------------

    @Override
    public void rememberAdmission(String hsmUid, String vin, Long bindingVersion) {
        if (hsmUid == null || hsmUid.isBlank() || vin == null || vin.isBlank()) {
            log.warn("rememberAdmission skipped: blank hsmUid/vin");
            return;
        }
        VehicleAccessBinding binding = VehicleAccessBinding.builder()
                .vin(vin)
                .hsmUid(hsmUid)
                .bindingVersion(bindingVersion == null ? 0L : bindingVersion)
                .bindingUpdatedAt(Instant.now())
                .source(BindingSource.VIAM_ADMISSION)
                .build();
        writeBidirectional(binding);
        log.info("Admission binding remembered: vin={}, hsmUid={}", LogMask.mask(vin), LogMask.mask(hsmUid));
    }

    @Override
    public void invalidate(String vin, String hsmUid, InvalidateReason reason) {
        try {
            if (vin != null && !vin.isBlank()) {
                redisTemplate.delete(VIN_KEY_PREFIX + vin);
            }
            if (hsmUid != null && !hsmUid.isBlank()) {
                redisTemplate.delete(HSM_KEY_PREFIX + hsmUid);
                redisTemplate.delete(HSM_ANCHOR_PREFIX + hsmUid);
            }
            log.info("Binding invalidated: vin={}, hsmUid={}, reason={}",
                    LogMask.mask(vin), LogMask.mask(hsmUid), reason);
        } catch (Exception e) {
            // 失效失败不伪造绑定；下一跳 TTL/回源自愈
            log.warn("Failed to invalidate binding: vin={}, hsmUid={}, reason={}",
                    LogMask.mask(vin), LogMask.mask(hsmUid), reason, e);
        }
    }

    // ------------------------------------------------------------------
    // 兼容适配器（deprecated，只委托新实现）
    // ------------------------------------------------------------------

    @Override
    @Deprecated
    public Optional<String> resolveVin(String hsmUid) {
        return resolveByHsmUid(hsmUid).binding() == null
                ? Optional.empty()
                : Optional.of(resolveByHsmUid(hsmUid).binding().getVin());
    }

    @Override
    @Deprecated
    public Optional<String> resolveDeviceSn(String vin) {
        return resolveByVin(vin).binding() == null
                ? Optional.empty()
                : Optional.of(resolveByVin(vin).binding().getHsmUid());
    }

    @Override
    @Deprecated
    public boolean isValidAndBound(String hsmUid) {
        return resolveByHsmUid(hsmUid).status() == ResolutionStatus.RESOLVED;
    }

    /**
     * @deprecated 使用 {@link #rememberAdmission(String, String, Long)}，不再依赖外部预热
     */
    @Deprecated
    public void cacheBinding(String deviceSn, String vin) {
        rememberAdmission(deviceSn, vin, null);
    }

    // ------------------------------------------------------------------
    // 内部实现
    // ------------------------------------------------------------------

    private BindingResolution mapViamResult(ViamResolveResult r) {
        if (r == null || r.getStatus() != ViamResolveResult.Status.RESOLVED) {
            return mapUnresolved(r);
        }
        // RESOLVED：校验必填字段（客户端已校验），原子写入双向缓存
        VehicleAccessBinding binding = VehicleAccessBinding.builder()
                .vin(r.getVin())
                .hsmUid(r.getHsmUid())
                .tboxSn(r.getTboxSn())
                .bindingVersion(r.getBindingVersion() == null ? 0L : r.getBindingVersion())
                .bindingUpdatedAt(r.getBindingUpdatedAt() == null ? Instant.now() : r.getBindingUpdatedAt())
                .source(BindingSource.VIAM_RESOLVE)
                .build();
        writeBidirectional(binding);
        return BindingResolution.resolved(binding);
    }

    private BindingResolution mapUnresolved(ViamResolveResult r) {
        if (r == null) {
            return BindingResolution.dependencyUnavailable("null VIAM result");
        }
        String reason = r.getReason();
        return switch (reason == null ? "" : reason) {
            case "BINDING_CONFLICT" -> BindingResolution.conflict();
            case "DEPENDENCY_UNAVAILABLE", "INVALID_ARGUMENT" ->
                    BindingResolution.dependencyUnavailable(reason);
            default -> BindingResolution.unbound(reason); // VIN_UNKNOWN / UNBOUND / TBOX_NOT_FOUND / HSM_UID_MISSING
        };
    }

    /** 有界 reason 标签（用于指标，不含身份信息）。 */
    private String reasonTag(BindingResolution resolution) {
        return switch (resolution.status()) {
            case UNBOUND -> "vin_unbound";
            case BINDING_CONFLICT -> "binding_conflict";
            case DEPENDENCY_UNAVAILABLE -> {
                String reason = resolution.reason();
                // 回源治理细分 reason 原样保留；VIAM 语义依赖失败统一归为
                // binding_dependency_unavailable（与 DLQ reason 对齐）
                yield RESOLVE_GOVERNANCE_REASONS.contains(reason)
                        ? reason : "binding_dependency_unavailable";
            }
            default -> BindingResolutionMetrics.REASON_CONTEXT_MISSING;
        };
    }

    private Optional<VehicleAccessBinding> safeReadHsmCache(String hsmUid) {
        try {
            return readCache(HSM_KEY_PREFIX, hsmUid);
        } catch (Exception e) {
            // Redis 读异常不阻断：继续回源（缓存只是加速层）
            log.warn("Failed to read hsm binding cache, fallback to VIAM: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<VehicleAccessBinding> readCache(String prefix, String id) {
        String json = redisTemplate.opsForValue().get(prefix + id);
        if (json == null || json.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(json, VehicleAccessBinding.class));
        } catch (Exception e) {
            log.warn("Corrupt binding cache entry, ignored: key={}", prefix + LogMask.mask(id), e);
            return Optional.empty();
        }
    }

    /**
     * 双向 key 在同一原子操作（MULTI/EXEC）中写入，携带 bindingVersion/时间/来源与有界 TTL；
     * 同时建立单会话上限锚点（SET NX，TTL=session-max-ttl，供滑动续期封顶）。
     * 写失败只影响加速，不得伪造未绑定（VIAM 仍是权威）。
     */
    private void writeBidirectional(VehicleAccessBinding binding) {
        try {
            String json = objectMapper.writeValueAsString(binding);
            byte[] vinKey = (VIN_KEY_PREFIX + binding.getVin()).getBytes(StandardCharsets.UTF_8);
            byte[] hsmKey = (HSM_KEY_PREFIX + binding.getHsmUid()).getBytes(StandardCharsets.UTF_8);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            long ttl = properties.getCacheTtlSeconds();
            redisTemplate.execute((RedisCallback<Object>) conn -> {
                conn.multi();
                conn.set(vinKey, value);
                conn.set(hsmKey, value);
                conn.expire(vinKey, ttl);
                conn.expire(hsmKey, ttl);
                conn.exec();
                return null;
            });
            ensureAnchor(binding.getHsmUid());
        } catch (Exception e) {
            // 缓存是加速层，不是 SSOT；写失败不阻断回源
            log.warn("Failed to write binding cache (accelerator only): vin={}, hsmUid={}, err={}",
                    LogMask.mask(binding.getVin()), LogMask.mask(binding.getHsmUid()), e.getMessage());
        }
    }

    private void deleteHsmKeyQuietly(String hsmUid) {
        try {
            redisTemplate.delete(HSM_KEY_PREFIX + hsmUid);
            redisTemplate.delete(HSM_ANCHOR_PREFIX + hsmUid);
        } catch (Exception e) {
            log.warn("Failed to delete stale hsm binding: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
        }
    }
}
