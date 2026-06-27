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
    private final BindingService bindingService;

    /**
     * 设备上线事件处理
     * @param deviceSn 设备序列号（主键）
     * @param clientId MQTT 客户端 ID
     * @param sourceIp 来源 IP
     * @param protoVer MQTT 协议版本
     */
    public void onConnected(String deviceSn, String clientId, String sourceIp, Integer protoVer) {
        try {
            // Resolve VIN for northbound enrichment
            String vin = bindingService.resolveVin(deviceSn).orElse(null);
            
            SessionInfo session = SessionInfo.builder()
                    .deviceSn(deviceSn)
                    .vin(vin)
                    .clientId(clientId)
                    .online(true)
                    .connectedAt(Instant.now())
                    .sourceIp(sourceIp)
                    .protoVer(protoVer)
                    .lastSeen(Instant.now())
                    .build();

            String key = SESSION_KEY_PREFIX + deviceSn;
            String json = objectMapper.writeValueAsString(session);
            redisTemplate.opsForValue().set(key, json, SESSION_TTL_HOURS, TimeUnit.HOURS);

            log.info("Session online: deviceSn={}, vin={}, clientId={}", deviceSn, vin, clientId);
        } catch (Exception e) {
            log.error("Failed to save session for deviceSn={}", deviceSn, e);
        }
    }

    /**
     * 设备下线事件处理
     * @param deviceSn 设备序列号
     */
    public void onDisconnected(String deviceSn) {
        try {
            String key = SESSION_KEY_PREFIX + deviceSn;
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                SessionInfo session = objectMapper.readValue(json, SessionInfo.class);
                session.setOnline(false);
                session.setDisconnectedAt(Instant.now());

                String updatedJson = objectMapper.writeValueAsString(session);
                redisTemplate.opsForValue().set(key, updatedJson, SESSION_TTL_HOURS, TimeUnit.HOURS);

                log.info("Session offline: deviceSn={}", deviceSn);
            }
        } catch (Exception e) {
            log.error("Failed to update session disconnect for deviceSn={}", deviceSn, e);
        }
    }

    /**
     * 获取会话信息（按 device_sn）
     */
    public Optional<SessionInfo> getSessionByDeviceSn(String deviceSn) {
        try {
            String key = SESSION_KEY_PREFIX + deviceSn;
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                return Optional.of(objectMapper.readValue(json, SessionInfo.class));
            }
        } catch (Exception e) {
            log.error("Failed to get session for deviceSn={}", deviceSn, e);
        }
        return Optional.empty();
    }

    /**
     * 获取会话信息（按 VIN，北向接口使用）
     * 先解析 VIN→device_sn，再查会话
     */
    public Optional<SessionInfo> getSessionByVin(String vin) {
        Optional<String> deviceSnOpt = bindingService.resolveDeviceSn(vin);
        if (deviceSnOpt.isEmpty()) {
            return Optional.empty();
        }
        return getSessionByDeviceSn(deviceSnOpt.get());
    }

    /**
     * 检查设备是否在线（按 device_sn）
     */
    public boolean isOnlineByDeviceSn(String deviceSn) {
        return getSessionByDeviceSn(deviceSn)
                .map(SessionInfo::isOnline)
                .orElse(false);
    }

    /**
     * 检查车辆是否在线（按 VIN，北向接口使用）
     */
    public boolean isOnlineByVin(String vin) {
        return getSessionByVin(vin)
                .map(SessionInfo::isOnline)
                .orElse(false);
    }
    
    /**
     * @deprecated Use isOnlineByDeviceSn or isOnlineByVin instead
     */
    @Deprecated
    public boolean isOnline(String identifier) {
        // Try device_sn first, then VIN
        return isOnlineByDeviceSn(identifier) || isOnlineByVin(identifier);
    }
}
