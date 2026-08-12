package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.service.FotaBridgeAppService;
import net.hwyz.iov.cloud.edd.vagw.service.FotaRouteConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * FOTA 下行 Kafka 消费者，对齐 EDD-VAGW-DSN-CR-005 §6。
 * <p>
 * 消费 iov.vagw.down.fota（消费组 edd-vagw-fota-downlink），手动 ack：
 * Kafka offset 仅在本地处理结果可靠记录后提交；处理异常不提交，交由 Kafka 重投。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FotaKafkaDownlinkConsumer {

    private final FotaBridgeAppService fotaBridgeAppService;

    @KafkaListener(
            topics = FotaRouteConfig.KAFKA_DOWN_TOPIC,
            groupId = FotaRouteConfig.DOWNLINK_CONSUMER_GROUP,
            containerFactory = "fotaKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        try {
            boolean processed = fotaBridgeAppService.processFotaDownlink(record);
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
