package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class SessionService {

    private static final String SESSION_KEY_PREFIX = "vagw:session:";
    private static final long SESSION_TTL_HOURS = 24;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public void onConnected(String vin, String clientId, String sourceIp, Integer protoVer) {
        try {
            SessionInfo session = SessionInfo.builder()
                    .vin(vin)
                    .clientId(clientId)
                    .online(true)
                    .connectedAt(Instant.now())
                    .sourceIp(sourceIp)
                    .protoVer(protoVer)
                    .lastSeen(Instant.now())
                    .build();

            String key = SESSION_KEY_PREFIX + vin;
            String json = objectMapper.writeValueAsString(session);
            redisTemplate.opsForValue().set(key, json, SESSION_TTL_HOURS, TimeUnit.HOURS);

            log.info("Session online: vin={}, clientId={}", vin, clientId);
        } catch (Exception e) {
            log.error("Failed to save session for vin={}", vin, e);
        }
    }

    public void onDisconnected(String vin) {
        try {
            String key = SESSION_KEY_PREFIX + vin;
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                SessionInfo session = objectMapper.readValue(json, SessionInfo.class);
                session.setOnline(false);
                session.setDisconnectedAt(Instant.now());

                String updatedJson = objectMapper.writeValueAsString(session);
                redisTemplate.opsForValue().set(key, updatedJson, SESSION_TTL_HOURS, TimeUnit.HOURS);

                log.info("Session offline: vin={}", vin);
            }
        } catch (Exception e) {
            log.error("Failed to update session disconnect for vin={}", vin, e);
        }
    }

    public Optional<SessionInfo> getSession(String vin) {
        try {
            String key = SESSION_KEY_PREFIX + vin;
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                return Optional.of(objectMapper.readValue(json, SessionInfo.class));
            }
        } catch (Exception e) {
            log.error("Failed to get session for vin={}", vin, e);
        }
        return Optional.empty();
    }

    public boolean isOnline(String vin) {
        return getSession(vin)
                .map(SessionInfo::isOnline)
                .orElse(false);
    }
}
