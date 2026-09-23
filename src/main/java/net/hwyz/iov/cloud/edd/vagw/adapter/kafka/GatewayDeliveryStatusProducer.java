package net.hwyz.iov.cloud.edd.vagw.adapter.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.VehicleBridgeException;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import vagw.v1.Delivery;

import java.util.concurrent.TimeUnit;

/**
 * 技术投递状态生产者（EDD-VAGW-DSN-CR-006 §8 + EDD-VAGW-DSN-CR-008 §2/§3.3）。
 * <p>
 * 使用 PAR-PROTO {@code iov-cloud-proto-vagw} 生成的 vagw.v1.Delivery.GatewayDeliveryStatus，
 * 序列化为 Protobuf bytes 生产到独立云内 Topic vagw.fota.delivery（Key=VIN），IOV-OTA 消费。
 * 不进入 vagw.fota 或车端 Topic；不注册为 FOTA PayloadType；无 JSON fallback。
 * </p>
 * <p>
 * 就绪门禁：Topic Provisioning NOT_READY 时不静默发送，抛异常由调用方按
 * 不提交 offset／Kafka 重投策略处理。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GatewayDeliveryStatusProducer {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final VehicleRouteCatalog routeCatalog;
    private final ObjectProvider<KafkaTopicProvisioningStatus> provisioningStatusProvider;

    /**
     * 生产 GatewayDeliveryStatus（Key=VIN）。
     *
     * @return 原 FOTA 传输消息 message_id（与 Inbox result 关联）
     */
    public String produce(Delivery.GatewayDeliveryStatus status, String vin) {
        assertProducerTopicsReady();
        try {
            Message<byte[]> message = MessageBuilder
                    .withPayload(status.toByteArray())
                    .setHeader(KafkaHeaders.TOPIC, routeCatalog.kafkaDeliveryTopic())
                    .setHeader(KafkaHeaders.KEY, vin)
                    .setHeader("service", "vehicle.fota")
                    .setHeader("delivery_outcome", status.getOutcome().name())
                    .setHeader("original_message_id", status.getOriginalMessageId())
                    .build();
            kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("GatewayDeliveryStatus produced: vin={}, outcome={}, originalMessageId={}, topic={}",
                    LogMask.mask(vin), status.getOutcome().name(), status.getOriginalMessageId(),
                    routeCatalog.kafkaDeliveryTopic());
            return status.getOriginalMessageId();
        } catch (Exception e) {
            log.error("GatewayDeliveryStatus produce failed: vin={}, originalMessageId={}",
                    LogMask.mask(vin), status.getOriginalMessageId(), e);
            throw new VehicleBridgeException("Failed to produce GatewayDeliveryStatus: "
                    + status.getOriginalMessageId(), e);
        }
    }

    /**
     * Producer 就绪门禁（EDD-VAGW-DSN-CR-008 §3.3）：
     * 依赖 Topic 未确认 READY 时不得将未就绪静默视为可发送，抛异常由调用方按
     * 不提交 offset／Kafka 重投策略处理。Provisioning 未启用（无 Status Bean）时不拦截。
     */
    private void assertProducerTopicsReady() {
        KafkaTopicProvisioningStatus status = provisioningStatusProvider.getIfAvailable();
        if (status != null && status.state() == KafkaTopicProvisioningStatus.State.NOT_READY) {
            throw new VehicleBridgeException(
                    "Kafka producer topic(s) not ready, missing: " + status.missingTopics());
        }
    }
}
