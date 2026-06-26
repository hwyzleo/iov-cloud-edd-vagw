package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UplinkKafkaProducer {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public void send(String topic, String vin, byte[] envelopeBytes, String service,
                     String msgType, String receivedAt, String clientId) {
        try {
            kafkaTemplate.send(topic, vin, envelopeBytes).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send to Kafka: topic={}, vin={}", topic, vin, ex);
                } else {
                    log.debug("Sent to Kafka: topic={}, vin={}, partition={}, offset={}",
                            topic, vin,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("Kafka send error: topic={}, vin={}", topic, vin, e);
        }
    }
}
