package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 准入会话快照注册表（EDD-VAGW-DSN-CR-012 §3.1/§3.4）。
 * <p>
 * {@code /mqtt/auth} 获得 TSP ALLOW 后保存候选态不可变快照，并原子递增 connection generation
 * （Redis INCR，跨实例单调）。{@code client.connected} 仅允许关联本快照；候选态在提升为 active
 * 后删除，不跨重连复用。generation 在 auth 期分配、connected 期提升为 active，保证 Hook 重试
 * 与 Kafka 重试 eventId 稳定。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AdmissionSessionContextRegistry {

    static final String CONTEXT_KEY_PREFIX = "vagw:auth:admission:";
    static final String GENERATION_KEY_PREFIX = "vagw:session:gen:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final VagwDeviceOnlineProperties properties;

    /**
     * 保存准入候选快照并分配 connection generation。
     *
     * @return 已固化的候选快照；持久化失败返回 {@link Optional#empty()}（调用方 fail-closed）
     */
    public Optional<AdmissionSessionContext> saveCandidate(String hsmUid, String vin, String certSerial,
                                                           String clientId, String sourceIp, Integer protoVer) {
        if (hsmUid == null || hsmUid.isBlank() || clientId == null || clientId.isBlank()) {
            return Optional.empty();
        }
        try {
            long generation = nextGeneration(hsmUid);
            Instant now = Instant.now();
            AdmissionSessionContext context = new AdmissionSessionContext(
                    normalize(hsmUid), vin, certSerial, clientId, sourceIp, protoVer,
                    generation, now, now.plusSeconds(properties.getAdmissionTtlSeconds()));
            redisTemplate.opsForValue().set(CONTEXT_KEY_PREFIX + clientId,
                    objectMapper.writeValueAsString(context),
                    properties.getAdmissionTtlSeconds(), TimeUnit.SECONDS);
            return Optional.of(context);
        } catch (Exception e) {
            // 快照是设备上线事实的必要输入，写失败即 fail-closed（不建立可信会话）
            log.error("SEC-DEVICE-ONLINE | failed to persist admission context: hsmUid={}, clientId={}",
                    LogMask.mask(hsmUid), clientId, e);
            return Optional.empty();
        }
    }

    /**
     * 关联当前候选快照：仅当 clientId 命中、hsmUid 一致且未过期时返回。
     */
    public Optional<AdmissionSessionContext> findCandidate(String hsmUid, String clientId) {
        if (hsmUid == null || hsmUid.isBlank() || clientId == null || clientId.isBlank()) {
            return Optional.empty();
        }
        try {
            String json = redisTemplate.opsForValue().get(CONTEXT_KEY_PREFIX + clientId);
            if (json == null) {
                return Optional.empty();
            }
            AdmissionSessionContext context = objectMapper.readValue(json, AdmissionSessionContext.class);
            if (!normalize(hsmUid).equals(normalize(context.hsmUid()))) {
                log.warn("SEC-DEVICE-ONLINE | admission context identity mismatch: hookHsmUid={}, contextHsmUid={}",
                        LogMask.mask(hsmUid), LogMask.mask(context.hsmUid()));
                return Optional.empty();
            }
            return Optional.of(context);
        } catch (Exception e) {
            log.warn("Failed to read admission context: clientId={}", clientId, e);
            return Optional.empty();
        }
    }

    /**
     * 原子分配连接代际（与既有 {@code vagw:session:gen:} 命名空间一致）。
     */
    public long nextGeneration(String hsmUid) {
        Long generation = redisTemplate.opsForValue()
                .increment(GENERATION_KEY_PREFIX + normalize(hsmUid));
        return generation == null ? 0L : generation;
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
