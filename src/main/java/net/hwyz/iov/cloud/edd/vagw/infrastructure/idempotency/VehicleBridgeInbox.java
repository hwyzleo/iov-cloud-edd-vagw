package net.hwyz.iov.cloud.edd.vagw.infrastructure.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 车辆桥接 Inbox（幂等／审计，EDD-VAGW-DSN-CR-006 §9）。
 * <p>
 * 字段对齐 CR §9：direction / message_id / envelope_sha256 / kafka_topic / partition / offset /
 * mqtt_route / state / outcome / updated_at（另保留 reason 与 result_message_id 供审计）。
 * </p>
 * <ul>
 *   <li>相同 message_id + 相同 envelope_sha256 → 复用已有技术结果，不重复桥接。</li>
 *   <li>相同 message_id + 不同 envelope_sha256 → 冲突隔离（调用方进 DLQ）。</li>
 *   <li>Kafka 重投、consumer rebalance、VAGW 重启、MQTT publish unknown 不生成新 message_id
 *       或新 Envelope，不得重复形成不同技术结果。</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleBridgeInbox {

    private static final String INBOX_KEY_PREFIX = "vagw:bridge:inbox:";
    private static final long INBOX_TTL_HOURS = 24;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public enum Direction { UPLINK, DOWNLINK }

    /** 技术投递状态（§9 state/outcome；ACCEPTED 仅表示技术接管，不表示 FOTA 业务成功） */
    public enum State { ACCEPTED, REJECTED, DLQED, CONFLICT }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InboxRecord {
        private String direction;
        private String messageId;
        /** 完整序列化 Envelope bytes 的 SHA-256（§9 envelope_sha256） */
        private String envelopeSha256;
        private String kafkaTopic;
        private Integer partition;
        private Long offset;
        /** MQTT 下行发布 route（topic）；上行可为空 */
        private String mqttRoute;
        private String state;
        private String outcome;
        private String reason;
        private String resultMessageId;
        private Instant updatedAt;
    }

    /**
     * 计算完整 Envelope bytes 的 SHA-256 十六进制摘要（幂等/冲突检测，§9）。
     */
    public static String digestOf(byte[] envelopeBytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(envelopeBytes == null ? new byte[0] : envelopeBytes);
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 digest unavailable", e);
        }
    }

    /**
     * 幂等查询：该 (direction, messageId) 是否已有最终处理记录。
     */
    public Optional<InboxRecord> findFinal(Direction direction, String messageId) {
        try {
            String json = redisTemplate.opsForValue().get(key(direction.name(), messageId));
            if (json == null) {
                return Optional.empty();
            }
            InboxRecord record = objectMapper.readValue(json, InboxRecord.class);
            return Optional.of(record);
        } catch (Exception e) {
            log.error("Failed to read bridge inbox: direction={}, messageId={}", direction, messageId, e);
            return Optional.empty();
        }
    }

    /**
     * 记录最终处理结果（SETNX 幂等）。
     *
     * @return true 表示本次写入成功；false 表示该 messageId 已有记录（幂等重复或冲突）
     */
    public boolean recordFinal(InboxRecord record) {
        try {
            String key = key(record.getDirection(), record.getMessageId());
            String json = objectMapper.writeValueAsString(record);
            Boolean created = redisTemplate.opsForValue().setIfAbsent(key, json, INBOX_TTL_HOURS, TimeUnit.HOURS);
            return Boolean.TRUE.equals(created);
        } catch (Exception e) {
            log.error("Failed to record bridge inbox: direction={}, messageId={}",
                    record.getDirection(), record.getMessageId(), e);
            return false;
        }
    }

    private String key(String direction, String messageId) {
        return INBOX_KEY_PREFIX + direction.toLowerCase() + ":" + messageId;
    }
}
