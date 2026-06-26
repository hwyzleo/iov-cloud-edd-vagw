package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.service.DownlinkService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DownlinkKafkaConsumer {

    private final DownlinkService downlinkService;

    @KafkaListener(
            topics = "${vagw.kafka.downlink-topic:tbox-vagw-cmd}",
            groupId = "${vagw.kafka.downlink-group:vagw-downlink}",
            concurrency = "${vagw.kafka.downlink-concurrency:3}"
    )
    public void consume(ConsumerRecord<String, byte[]> record) {
        try {
            String vin = record.key();
            byte[] payload = record.value();

            log.info("Downlink command from Kafka: vin={}, partition={}, offset={}",
                    vin, record.partition(), record.offset());

            // Parse service from payload or use default
            // For now, assume remotecontrol
            downlinkService.sendCommand(vin, "remotecontrol", payload, null, null);
        } catch (Exception e) {
            log.error("Error processing downlink command from Kafka", e);
        }
    }
}
