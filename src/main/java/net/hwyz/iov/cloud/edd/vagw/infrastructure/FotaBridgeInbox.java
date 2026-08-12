package net.hwyz.iov.cloud.edd.vagw.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
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
 * FOTA 桥接 Inbox（幂等/审计），对齐 EDD-VAGW-DSN-CR-005 §8。
 * <p>
 * 建议保存 direction / message_id / payload_digest / kafka_topic / partition / offset /
 * mqtt_topic / status / result_message_id。以 Redis 实现（与 Session/Binding 同设施）。
 * </p>
 * <ul>
 *   <li>上行物理重复：按 messageId 去重；相同摘要复用处理结果，不同摘要进入冲突隔离。</li>
 *   <li>下行 Kafka 重复：同 messageId + payloadDigest 只发布一次 MQTT；进程重启 / Consumer
 *       rebalance 不得重复形成不同技术结果。</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FotaBridgeInbox {

    private static final String INBOX_KEY_PREFIX = "vagw:fota:inbox:";
    private static final long INBOX_TTL_HOURS = 24;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public enum Direction { UPLINK, DOWNLINK }

    public enum Status { ACCEPTED, REJECTED, DLQED }

    @Data
    @Builder
    public static class InboxRecord {
        private String direction;
        private String messageId;
        private String payloadDigest;
        private String kafkaTopic;
        private Integer partition;
        private Long offset;
        private String mqttTopic;
        private String status;
        private String resultMessageId;
        private String reason;
        private Instant processedAt;
    }

    /**
     * 计算 payload 的 SHA-256 十六进制摘要（去重用）。
     */
    public static String digestOf(byte[] payload) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(payload == null ? new byte[0] : payload);
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
            log.error("Failed to read fota inbox: direction={}, messageId={}", direction, messageId, e);
            return Optional.empty();
        }
    }

    /**
     * 记录最终处理结果（SETNX 幂等）。
     *
     * @return true 表示本次调用成功写入；false 表示该 messageId 已有记录（幂等重复或冲突）。
     */
    public boolean recordFinal(InboxRecord record) {
        try {
            String key = key(record.getDirection(), record.getMessageId());
            String json = objectMapper.writeValueAsString(record);
            Boolean created = redisTemplate.opsForValue().setIfAbsent(key, json, INBOX_TTL_HOURS, TimeUnit.HOURS);
            return Boolean.TRUE.equals(created);
        } catch (Exception e) {
            log.error("Failed to record fota inbox: direction={}, messageId={}",
                    record.getDirection(), record.getMessageId(), e);
            return false;
        }
    }

    private String key(String direction, String messageId) {
        return INBOX_KEY_PREFIX + direction.toLowerCase() + ":" + messageId;
    }
}
