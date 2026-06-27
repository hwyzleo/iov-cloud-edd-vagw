package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UplinkKafkaProducer {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public void send(String topic, String deviceSn, String vin, byte[] envelopeBytes,
                     String service, String msgType, String receivedAt, String clientId) {
        try {
            var message = MessageBuilder
                    .withPayload(envelopeBytes)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.KEY, deviceSn)
                    .setHeader("vin", vin)
                    .setHeader("service", service)
                    .setHeader("msgType", msgType)
                    .setHeader("receivedAt", receivedAt)
                    .build();
            kafkaTemplate.send(message).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send to Kafka: topic={}, deviceSn={}", topic, deviceSn, ex);
                } else {
                    log.debug("Sent to Kafka: topic={}, deviceSn={}, partition={}, offset={}",
                            topic, deviceSn,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("Kafka send error: topic={}, deviceSn={}", topic, deviceSn, e);
        }
    }
}
