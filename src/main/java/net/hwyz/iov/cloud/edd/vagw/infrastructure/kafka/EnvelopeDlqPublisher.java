package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.VehicleBridgeException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 公共 Envelope 死信发布器（EDD-VAGW-DSN-CR-006 §3/§9）。
 * <p>
 * 不可恢复契约错误与超限进入对应 DLQ：value 保留原 bytes，DLQ 元数据（脱敏 reason）仅放 headers，
 * 不写回 value、不改变 Envelope 事实。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EnvelopeDlqPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * 发布原 bytes 到 DLQ（Key=VIN／脱敏 device key），reason 只进 headers。
     *
     * @throws VehicleBridgeException DLQ 发布失败
     */
    public void publish(String dlqTopic, String key, byte[] payload, String reason) {
        try {
            Message<byte[]> message = MessageBuilder
                    .withPayload(payload)
                    .setHeader(KafkaHeaders.TOPIC, dlqTopic)
                    .setHeader(KafkaHeaders.KEY, key)
                    .setHeader("service", "vehicle.fota")
                    .setHeader("dlq_reason", reason)
                    .build();
            kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.warn("Envelope moved to DLQ: topic={}, key={}, reason={}", dlqTopic, LogMask.mask(key), reason);
        } catch (Exception e) {
            log.error("Envelope DLQ publish failed: topic={}, key={}", dlqTopic, LogMask.mask(key), e);
            throw new VehicleBridgeException("Failed to publish to DLQ: " + dlqTopic, e);
        }
    }
}
