package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 认证身份短期存储（EDD-VAGW-DSN-CR-011 §3.2/§5）。
 * <p>
 * 在 {@code /mqtt/auth} 准入成功时按 clientId 写入本次连接已认证的证书身份（hsmUid/certSerial/
 * certFingerprint），供 {@code client.connected} 事件建立会话不可变快照时并入；TTL 有界，仅承载
 * 非敏感标识元数据，不落明文密钥、不参与业务授权。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthenticatedIdentityStore {

    private static final String KEY_PREFIX = "vagw:auth:ctx:";
    private static final long TTL_SECONDS = 300;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public void save(String clientId, String hsmUid, String certSerial, String certFingerprint) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        try {
            AuthenticatedIdentity identity = new AuthenticatedIdentity(
                    hsmUid, certSerial, certFingerprint, Instant.now());
            redisTemplate.opsForValue().set(KEY_PREFIX + clientId,
                    objectMapper.writeValueAsString(identity), TTL_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Failed to persist authenticated identity for clientId={}", clientId, e);
        }
    }

    public Optional<AuthenticatedIdentity> findByClientId(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return Optional.empty();
        }
        try {
            String json = redisTemplate.opsForValue().get(KEY_PREFIX + clientId);
            if (json != null) {
                return Optional.of(objectMapper.readValue(json, AuthenticatedIdentity.class));
            }
        } catch (Exception e) {
            log.warn("Failed to read authenticated identity for clientId={}", clientId, e);
        }
        return Optional.empty();
    }

    public void remove(String clientId) {
        if (clientId != null && !clientId.isBlank()) {
            redisTemplate.delete(KEY_PREFIX + clientId);
        }
    }

    /**
     * 已认证连接身份（非敏感标识元数据）。
     */
    public record AuthenticatedIdentity(
            String hsmUid,
            String certSerial,
            String certFingerprint,
            Instant authenticatedAt) {
    }
}
