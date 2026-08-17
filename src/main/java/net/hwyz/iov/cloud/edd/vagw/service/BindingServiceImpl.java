package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.TspResolveResult;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.TspVehicleAccessIdentityClient;
import net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

/**
 * 设备绑定解析服务实现（EDD-VAGW-DSN-CR-007 §4/§5）。
 * <p>
 * VIN → hsmUid 解析：版本化缓存（有界 TTL）→ 未命中回源 TSP resolveByVin → 校验后原子写入双向缓存。
 * hsmUid → VIN：优先使用准入成功时写入的会话绑定缓存，不依赖外部预热 Redis；
 * 映射意外缺失返回 CONTEXT_MISSING（基础设施状态），不得误记为真实 UNBOUND。
 * Redis 不可用或写失败仅影响加速，不伪造未绑定；TSP 绑定投影是权威查询入口。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BindingServiceImpl implements BindingService {

    private static final String VIN_KEY_PREFIX = "vagw:binding:vin:";
    private static final String HSM_KEY_PREFIX = "vagw:binding:hsm:";

    private final StringRedisTemplate redisTemplate;
    private final TspVehicleAccessIdentityClient tspClient;
    private final ObjectMapper objectMapper;

    @Value("${vagw.binding.cache-ttl-seconds:300}")
    private long cacheTtlSeconds;

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
            if (cached.isPresent()) {
                log.debug("Binding cache hit (vin): vin={}, hsmUid={}", LogMask.mask(vin),
                        LogMask.mask(cached.get().getHsmUid()));
                return BindingResolution.resolved(cached.get());
            }
            // 缓存未命中 → TSP 回源
            log.debug("Binding cache miss (vin), fallback to TSP: vin={}", LogMask.mask(vin));
            return mapTspResult(tspClient.resolveByVin(vin));
        } catch (Exception e) {
            log.error("Failed to resolve binding by vin={}", LogMask.mask(vin), e);
            return BindingResolution.dependencyUnavailable("cache read failed");
        }
    }

    // ------------------------------------------------------------------
    // hsmUid → VIN（上行会话绑定，不依赖外部预热）
    // ------------------------------------------------------------------

    @Override
    public BindingResolution resolveByHsmUid(String hsmUid) {
        if (hsmUid == null || hsmUid.isBlank()) {
            return BindingResolution.contextMissing("blank hsmUid");
        }
        try {
            Optional<VehicleAccessBinding> cached = readCache(HSM_KEY_PREFIX, hsmUid);
            if (cached.isPresent()) {
                log.debug("Binding cache hit (hsm): hsmUid={}, vin={}", LogMask.mask(hsmUid),
                        LogMask.mask(cached.get().getVin()));
                return BindingResolution.resolved(cached.get());
            }
            // 映射意外缺失：基础设施/上下文状态，非真实 UNBOUND，fail-closed 进入安全隔离
            log.warn("Binding context missing for hsmUid={}", LogMask.mask(hsmUid));
            return BindingResolution.contextMissing("no session/cache binding for hsmUid");
        } catch (Exception e) {
            log.error("Failed to resolve binding by hsmUid={}", LogMask.mask(hsmUid), e);
            return BindingResolution.contextMissing("cache read failed");
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
                .source(BindingSource.ADMISSION)
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

    private BindingResolution mapTspResult(TspResolveResult r) {
        if (r == null || r.getStatus() != TspResolveResult.Status.RESOLVED) {
            String reason = r == null ? "null TSP result" : r.getReason();
            return switch (reason == null ? "" : reason) {
                case "BINDING_CONFLICT" -> BindingResolution.conflict();
                case "DEPENDENCY_UNAVAILABLE", "INVALID_ARGUMENT" ->
                        BindingResolution.dependencyUnavailable(reason);
                default -> BindingResolution.unbound(reason); // VIN_UNKNOWN / UNBOUND / TBOX_NOT_FOUND / HSM_UID_MISSING
            };
        }
        // RESOLVED：校验必填字段（客户端已校验），原子写入双向缓存
        VehicleAccessBinding binding = VehicleAccessBinding.builder()
                .vin(r.getVin())
                .hsmUid(r.getHsmUid())
                .tboxSn(r.getTboxSn())
                .bindingVersion(r.getBindingVersion() == null ? 0L : r.getBindingVersion())
                .bindingUpdatedAt(r.getBindingUpdatedAt() == null ? Instant.now() : r.getBindingUpdatedAt())
                .source(BindingSource.TSP)
                .build();
        writeBidirectional(binding);
        return BindingResolution.resolved(binding);
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
     * 双向 key 在同一原子操作（MULTI/EXEC）中写入，携带 bindingVersion/时间/来源与有界 TTL。
     * 写失败只影响加速，不得伪造未绑定（TSP 仍是权威）。
     */
    private void writeBidirectional(VehicleAccessBinding binding) {
        try {
            String json = objectMapper.writeValueAsString(binding);
            byte[] vinKey = (VIN_KEY_PREFIX + binding.getVin()).getBytes(StandardCharsets.UTF_8);
            byte[] hsmKey = (HSM_KEY_PREFIX + binding.getHsmUid()).getBytes(StandardCharsets.UTF_8);
            byte[] value = json.getBytes(StandardCharsets.UTF_8);
            redisTemplate.execute((RedisCallback<Object>) conn -> {
                conn.multi();
                conn.set(vinKey, value);
                conn.set(hsmKey, value);
                conn.expire(vinKey, cacheTtlSeconds);
                conn.expire(hsmKey, cacheTtlSeconds);
                conn.exec();
                return null;
            });
        } catch (Exception e) {
            // 缓存是加速层，不是 SSOT；写失败不阻断回源
            log.warn("Failed to write binding cache (accelerator only): vin={}, hsmUid={}, err={}",
                    LogMask.mask(binding.getVin()), LogMask.mask(binding.getHsmUid()), e.getMessage());
        }
    }
}
