package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;

/**
 * 设备上线事件工厂（EDD-VAGW-DSN-CR-012 §3.3/§3.4）。
 * <p>
 * eventId 由稳定连接事实派生，重试时不得重生成：
 * {@code sha256(brokerNode + sessionId|clientId + normalizedHsmUid + connectionGeneration + "DEVICE_ONLINE")}。
 * occurredAt 优先使用合法 EMQX connected 时间，缺失或明显越界时回退 VAGW 接收时间并标记
 * {@code VAGW_RECEIVED}，不得以任意业务时间伪造。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceOnlineEventFactory {

    /** EMQX connected 时间与 VAGW 接收时间允许的最大偏差 */
    private static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(5);
    private static final String EVENT_SALT = "DEVICE_ONLINE";

    private final ObjectMapper objectMapper;

    public DeviceOnlineEvent create(AdmissionSessionContext context, MqttEventRequest request, Instant receivedAt) {
        String sessionId = firstNonBlank(request.getSessionId(), request.getClientId());
        String brokerNode = firstNonBlank(request.getNode(), "unknown");
        String eventId = eventId(brokerNode, sessionId, context.hsmUid(), context.connectionGeneration());

        Instant occurredAt = receivedAt;
        String timeSource = DeviceOnlineEvent.TIME_SOURCE_VAGW_RECEIVED;
        Long ts = request.getTimestamp();
        if (ts != null) {
            Instant emqx = Instant.ofEpochMilli(ts);
            if (!emqx.isBefore(receivedAt.minus(MAX_CLOCK_SKEW)) && !emqx.isAfter(receivedAt.plus(MAX_CLOCK_SKEW))) {
                occurredAt = emqx;
                timeSource = DeviceOnlineEvent.TIME_SOURCE_EMQX;
            } else {
                log.warn("DEVICE-ONLINE | EMQX connected ts out of skew, fallback to VAGW receivedAt: skewMs={}",
                        Duration.between(receivedAt, emqx).toMillis());
            }
        }
        return new DeviceOnlineEvent(
                eventId,
                DeviceOnlineEvent.TYPE,
                context.hsmUid(),
                context.vin(),
                sessionId,
                context.connectionGeneration(),
                context.certSerial(),
                null,
                occurredAt,
                timeSource);
    }

    /**
     * 稳定 eventId（重试不变）。
     */
    public static String eventId(String brokerNode, String sessionId, String hsmUid, long generation) {
        String input = String.join("|",
                nullSafe(brokerNode), nullSafe(sessionId), normalize(hsmUid), Long.toString(generation), EVENT_SALT);
        return sha256Hex(input);
    }

    /**
     * 身份摘要（用于日志/指标，禁止输出完整 hsmUid）。
     */
    public static String identityDigest(String hsmUid) {
        return normalize(hsmUid) == null ? null : sha256Hex(normalize(hsmUid)).substring(0, 12);
    }

    public String toJson(DeviceOnlineEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize DeviceOnlineEvent", e);
        }
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
