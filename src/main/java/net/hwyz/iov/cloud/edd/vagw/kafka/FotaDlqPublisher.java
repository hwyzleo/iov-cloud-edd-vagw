package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.FotaBridgeException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * FOTA 死信发布器，对齐 EDD-VAGW-DSN-CR-005 §8。
 * <p>
 * 不可恢复契约错误进对应 DLQ；上行 Kafka 生产超限进上行 DLQ。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FotaDlqPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * 发布消息到 DLQ。
     *
     * @throws FotaBridgeException DLQ 发布失败
     */
    public void publish(String dlqTopic, String key, byte[] payload, String reason) {
        try {
            Message<byte[]> message = MessageBuilder
                    .withPayload(payload)
                    .setHeader(KafkaHeaders.TOPIC, dlqTopic)
                    .setHeader(KafkaHeaders.KEY, key)
                    .setHeader("service", "fota")
                    .setHeader("dlq_reason", reason)
                    .build();
            kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.warn("FOTA message moved to DLQ: topic={}, key={}, reason={}", dlqTopic, key, reason);
        } catch (Exception e) {
            log.error("FOTA DLQ publish failed: topic={}, key={}", dlqTopic, key, e);
            throw new FotaBridgeException("Failed to publish to DLQ: " + dlqTopic, e);
        }
    }
}
