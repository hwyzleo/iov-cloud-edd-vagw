package net.hwyz.iov.cloud.edd.vagw.adapter.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.VehicleMessageBridgeService;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 车辆消息下行 Kafka 消费者（EDD-VAGW-DSN-CR-006 §7/§9）。
 * <p>
 * 消费 ota.fota（消费组 edd-vagw-fota-downlink），手动 ack：
 * Kafka offset 仅在 Inbox 与技术结果可靠收敛后提交；处理异常不提交，交由 Kafka 重投。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleMessageDownlinkConsumer {

    private final VehicleMessageBridgeService bridgeService;

    @KafkaListener(
            topics = VehicleRouteCatalog.KAFKA_DOWN_TOPIC,
            groupId = VehicleRouteCatalog.DOWNLINK_CONSUMER_GROUP,
            containerFactory = "vagwKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        try {
            boolean processed = bridgeService.processDownlink(record);
            if (processed) {
                acknowledgment.acknowledge();
            } else {
                log.warn("FOTA downlink not reliably processed (offset not committed): topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
            }
        } catch (Exception e) {
            // 不提交 offset，交由 Kafka 重投
            log.error("FOTA downlink consume error: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), e);
        }
    }
}
