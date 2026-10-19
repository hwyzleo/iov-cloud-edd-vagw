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
    private static final String SESSION_GENERATION_KEY_PREFIX = "vagw:session:gen:";
    private static final long SESSION_TTL_HOURS = 24;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final BindingService bindingService;
    private final AuthenticatedIdentityStore authenticatedIdentityStore;

    /**
     * 设备上线事件处理（兼容入口，generation 由 {@link #nextConnectionGeneration} 分配）。
     *
     * @param deviceSn 设备序列号（主键）
     * @param clientId MQTT 客户端 ID
     * @param sourceIp 来源 IP
     * @param protoVer MQTT 协议版本
     */
    public void onConnected(String deviceSn, String clientId, String sourceIp, Integer protoVer) {
        onConnected(deviceSn, clientId, sourceIp, protoVer, null, null);
    }

    /**
     * 设备上线事件处理（EDD-VAGW-DSN-CR-012 §3.1/§3.5）。
     * <p>
     * connection generation 由 {@code /mqtt/auth} 候选态分配、此处提升为 active；传入非空时
     * 直接复用（保证 Hook 重试与 Kafka 重试 eventId 稳定），为空时兼容旧路径自行分配。
     * 同时固化本次 DEVICE_ONLINE eventId 供会话对账使用。
     * </p>
     *
     * @param deviceSn 设备序列号（主键）
     * @param clientId MQTT 客户端 ID
     * @param sourceIp 来源 IP
     * @param protoVer MQTT 协议版本
     * @param generation 准入期已分配的 connection generation（可空）
     * @param deviceOnlineEventId 本次已固化的 DEVICE_ONLINE eventId（可空）
     */
    public void onConnected(String deviceSn, String clientId, String sourceIp, Integer protoVer,
                            Long generation, String deviceOnlineEventId) {
        try {
            // Resolve VIN for northbound enrichment
            String vin = bindingService.resolveVin(deviceSn).orElse(null);

            // connection generation：auth 候选态分配后提升为 active；兼容路径自行递增
            long connectionGeneration = generation != null ? generation : nextConnectionGeneration(deviceSn);
            // 认证期证书身份（EDD-VAGW-DSN-CR-011 §3.2）：由 /mqtt/auth 写入，仅标识元数据
            AuthenticatedIdentityStore.AuthenticatedIdentity identity =
                    authenticatedIdentityStore.findByClientId(clientId).orElse(null);

            SessionInfo session = SessionInfo.builder()
                    .deviceSn(deviceSn)
                    .vin(vin)
                    .clientId(clientId)
                    .online(true)
                    .connectedAt(Instant.now())
                    .sourceIp(sourceIp)
                    .protoVer(protoVer)
                    .lastSeen(Instant.now())
                    .connectionGeneration(connectionGeneration)
                    .brokerSessionId(clientId)
                    .certSerial(identity == null ? null : identity.certSerial())
                    .certFingerprint(identity == null ? null : identity.certFingerprint())
                    .lastDeviceOnlineEventId(deviceOnlineEventId)
                    .build();

            String key = SESSION_KEY_PREFIX + deviceSn;
            String json = objectMapper.writeValueAsString(session);
            redisTemplate.opsForValue().set(key, json, SESSION_TTL_HOURS, TimeUnit.HOURS);

            log.info("Session online: deviceUid={}, vin={}, clientId={}, generation={}",
                    deviceSn, vin, clientId, connectionGeneration);
        } catch (Exception e) {
            log.error("Failed to save session for deviceSn={}", deviceSn, e);
        }
    }

    private long nextConnectionGeneration(String deviceSn) {
        try {
            Long generation = redisTemplate.opsForValue()
                    .increment(SESSION_GENERATION_KEY_PREFIX + deviceSn);
            return generation == null ? 0L : generation;
        } catch (Exception e) {
            log.warn("Failed to increment connection generation for deviceSn={}", deviceSn, e);
            return 0L;
        }
    }

    /**
     * keyprov 下行前 generation guard（EDD-VAGW-DSN-CR-011 §5）：
     * 仅当会话仍在线且 connection generation 与已认证上下文一致时返回 true。
     */
    public boolean isGenerationCurrent(String deviceSn, long generation) {
        return getSessionByDeviceSn(deviceSn)
                .map(session -> session.isOnline() && session.getConnectionGeneration() == generation)
                .orElse(false);
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

                log.info("Session offline: deviceUid={}", deviceSn);
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
