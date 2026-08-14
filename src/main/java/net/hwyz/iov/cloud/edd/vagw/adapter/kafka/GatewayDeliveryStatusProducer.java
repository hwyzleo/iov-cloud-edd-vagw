package net.hwyz.iov.cloud.edd.vagw.adapter.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.VehicleBridgeException;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import vagw.v1.Delivery;

import java.util.concurrent.TimeUnit;

/**
 * 技术投递状态生产者（EDD-VAGW-DSN-CR-006 §8）。
 * <p>
 * 使用 PAR-PROTO {@code iov-cloud-proto-vagw} 生成的 vagw.v1.Delivery.GatewayDeliveryStatus，
 * 序列化为 Protobuf bytes 生产到独立云内 Topic iov.vagw.delivery.fota（Key=VIN），IOV-OTA 消费。
 * 不进入 iov.vagw.up/down.fota 或车端 Topic；不注册为 FOTA PayloadType；无 JSON fallback。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GatewayDeliveryStatusProducer {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * 生产 GatewayDeliveryStatus（Key=VIN）。
     *
     * @return 原 FOTA 传输消息 message_id（与 Inbox result 关联）
     */
    public String produce(Delivery.GatewayDeliveryStatus status, String vin) {
        try {
            Message<byte[]> message = MessageBuilder
                    .withPayload(status.toByteArray())
                    .setHeader(KafkaHeaders.TOPIC, VehicleRouteCatalog.KAFKA_DELIVERY_TOPIC)
                    .setHeader(KafkaHeaders.KEY, vin)
                    .setHeader("service", "vehicle.fota")
                    .setHeader("delivery_outcome", status.getOutcome().name())
                    .setHeader("original_message_id", status.getOriginalMessageId())
                    .build();
            kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("GatewayDeliveryStatus produced: vin={}, outcome={}, originalMessageId={}, topic={}",
                    LogMask.mask(vin), status.getOutcome().name(), status.getOriginalMessageId(),
                    VehicleRouteCatalog.KAFKA_DELIVERY_TOPIC);
            return status.getOriginalMessageId();
        } catch (Exception e) {
            log.error("GatewayDeliveryStatus produce failed: vin={}, originalMessageId={}",
                    LogMask.mask(vin), status.getOriginalMessageId(), e);
            throw new VehicleBridgeException("Failed to produce GatewayDeliveryStatus: "
                    + status.getOriginalMessageId(), e);
        }
    }
}
