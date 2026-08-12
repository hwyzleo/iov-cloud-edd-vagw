package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * FOTA 上行 Kafka 生产者（Key=VIN），对齐 EDD-VAGW-DSN-CR-005 §5 / RD-005-5。
 * <p>
 * 只负责 Kafka 技术投递；不产生 OTA 业务回执。Kafka Headers 透传接入上下文供北向消费。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FotaKafkaProducer {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * 同步发送 FOTA 上行消息到 Kafka（Key=VIN）。
     *
     * @throws Exception 发送失败（供调用方有限重试 / 超限进 DLQ）
     */
    public SendResult sendFotaUplink(String topic, String vin, EnvelopeProto.Envelope envelope,
                                     String receivedAt, String sourceNode) throws Exception {
        Message<byte[]> message = MessageBuilder
                .withPayload(envelope.toByteArray())
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, vin)
                .setHeader("device_sn", envelope.getDeviceSn())
                .setHeader("vin", vin)
                .setHeader("service", "fota")
                .setHeader("msg_type", envelope.getMsgType().name())
                .setHeader("message_id", envelope.getMsgId())
                .setHeader("correlation_id", envelope.getCorrelationId())
                .setHeader("trace_id", envelope.getTraceId())
                .setHeader("received_at", receivedAt)
                .setHeader("source_node", sourceNode)
                .build();

        org.springframework.kafka.support.SendResult<String, byte[]> result =
                kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("FOTA uplink sent: topic={}, vin={}, messageId={}, partition={}, offset={}",
                topic, LogMask.mask(vin), envelope.getMsgId(),
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        return new SendResult(result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }

    public record SendResult(int partition, long offset) {
    }
}
