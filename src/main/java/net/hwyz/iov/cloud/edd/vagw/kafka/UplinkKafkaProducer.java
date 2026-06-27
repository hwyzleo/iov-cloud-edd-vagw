package net.hwyz.iov.cloud.edd.vagw.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UplinkKafkaProducer {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * 发送上行消息到 Kafka
     * @param topic Kafka topic
     * @param deviceSn 设备序列号（作为 Key）
     * @param vin 车辆识别码（可为 null，用于 Headers）
     * @param envelopeBytes Envelope 字节
     * @param service 业务服务名
     * @param msgType 消息类型
     * @param receivedAt 接收时间
     * @param clientId MQTT 客户端 ID
     */
    public void send(String topic, String deviceSn, String vin, byte[] envelopeBytes, String service,
                     String msgType, String receivedAt, String clientId) {
        try {
            Message<byte[]> message = MessageBuilder
                    .withPayload(envelopeBytes)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.KEY, deviceSn)
                    .setHeader("device_sn", deviceSn)
                    .setHeader("vin", vin)
                    .setHeader("service", service)
                    .setHeader("msg_type", msgType)
                    .setHeader("received_at", receivedAt)
                    .setHeader("source_node", clientId)
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
