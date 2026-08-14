package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import vehicle.common.v1.Envelope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 公共 Envelope Kafka 生产者（EDD-VAGW-DSN-CR-006 §6）。
 * <p>
 * 将原序列化 Envelope bytes 作为 Kafka value 生产到主 Topic，Key=VIN；
 * Kafka Headers 从 Envelope 派生（service/payload_type/message_kind/message_id/correlation_id/
 * request_id/protocol_version/received_at/脱敏设备标识），仅用于检索，消费者必须以 value 中
 * Envelope 为事实来源，不得因 headers 缺失或篡改改变 value 事实。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EnvelopeKafkaProducer {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * 同步发送（Key=VIN，value=原 Envelope bytes）。
     *
     * @throws Exception 发送失败（供调用方有限重试/超限进 DLQ）
     */
    public SendResult sendUplink(String topic, String vin, byte[] envelopeBytes,
                                 Envelope.VehicleMessageEnvelope envelope,
                                 String receivedAt) throws Exception {
        Message<byte[]> message = MessageBuilder
                .withPayload(envelopeBytes)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, vin)
                .setHeader("service", envelope.getService())
                .setHeader("payload_type", envelope.getPayloadType())
                .setHeader("message_kind", envelope.getMessageKind().name())
                .setHeader("message_id", envelope.getMessageId())
                .setHeader("correlation_id", envelope.hasCorrelationId() ? envelope.getCorrelationId() : "")
                .setHeader("request_id", envelope.getRequestId())
                .setHeader("protocol_version", envelope.getProtocolVersion())
                .setHeader("received_at", receivedAt)
                .setHeader("device_key", LogMask.mask(envelope.getDeviceId()))
                .build();

        org.springframework.kafka.support.SendResult<String, byte[]> result =
                kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("Envelope uplink sent: topic={}, vin={}, messageId={}, partition={}, offset={}",
                topic, LogMask.mask(vin), envelope.getMessageId(),
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        return new SendResult(result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }

    public record SendResult(int partition, long offset) {
    }
}
