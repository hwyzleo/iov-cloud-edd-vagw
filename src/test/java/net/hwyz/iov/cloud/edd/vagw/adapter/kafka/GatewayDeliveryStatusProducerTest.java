package net.hwyz.iov.cloud.edd.vagw.adapter.kafka;

import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.VehicleBridgeException;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import vagw.v1.Delivery;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 技术投递状态生产者测试（EDD-VAGW-DSN-CR-008 §2/§3.3）。
 * <p>
 * 验证生产目标为 vagw.fota.delivery（Key=VIN）及 Provisioning 就绪门禁。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class GatewayDeliveryStatusProducerTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;
    @Mock
    private ObjectProvider<KafkaTopicProvisioningStatus> statusProvider;
    @Mock
    private KafkaTopicProvisioningStatus provisioningStatus;

    private VehicleRouteCatalog routeCatalog;

    @BeforeEach
    void setUp() {
        routeCatalog = new VehicleRouteCatalog(new VagwFotaTopicProperties());
    }

    private GatewayDeliveryStatusProducer newProducer() {
        return new GatewayDeliveryStatusProducer(kafkaTemplate, routeCatalog, statusProvider);
    }

    private Delivery.GatewayDeliveryStatus status() {
        return Delivery.GatewayDeliveryStatus.newBuilder()
                .setOriginalMessageId("msg-dn-001")
                .setVin("VIN-A")
                .setStage("DOWNLINK_RECEIVED")
                .setOutcome(Delivery.Outcome.OUTCOME_REJECTED)
                .setReason("VEHICLE_OFFLINE")
                .setRetryable(true)
                .setOccurredAtMs(System.currentTimeMillis())
                .build();
    }

    private void stubSendSuccess() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("vagw.fota.delivery", 0), 0L, 7, 0L, 0, 0);
        SendResult<String, byte[]> result = new SendResult<>(
                new ProducerRecord<>("vagw.fota.delivery", "VIN-A", new byte[]{1}), metadata);
        when(kafkaTemplate.send(any(Message.class)))
                .thenReturn(CompletableFuture.completedFuture(result));
    }

    @Test
    @DisplayName("正常生产到 vagw.fota.delivery，Key=VIN")
    void produce_shouldSendToDeliveryTopic() {
        when(statusProvider.getIfAvailable()).thenReturn(null);
        stubSendSuccess();

        GatewayDeliveryStatusProducer producer = newProducer();
        String messageId = producer.produce(status(), "VIN-A");

        assertEquals("msg-dn-001", messageId);
        verify(kafkaTemplate).send(ArgumentMatchers.<Message<byte[]>>argThat(message ->
                "vagw.fota.delivery".equals(
                        message.getHeaders().get(KafkaHeaders.TOPIC, String.class))
                        && "VIN-A".equals(message.getHeaders().get(KafkaHeaders.KEY, String.class))));
    }

    @Test
    @DisplayName("Provisioning NOT_READY 时不发送并抛异常")
    void notReady_shouldRejectSend() {
        when(statusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.NOT_READY);
        when(provisioningStatus.missingTopics()).thenReturn(Set.of("vagw.fota.delivery"));

        GatewayDeliveryStatusProducer producer = newProducer();

        VehicleBridgeException ex = assertThrows(VehicleBridgeException.class,
                () -> producer.produce(status(), "VIN-A"));
        assertTrue(ex.getMessage().contains("not ready"));
        verify(kafkaTemplate, never()).send(any(Message.class));
    }

    @Test
    @DisplayName("Provisioning READY 时正常发送")
    void ready_shouldSend() {
        when(statusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.READY);
        stubSendSuccess();

        GatewayDeliveryStatusProducer producer = newProducer();

        String messageId = producer.produce(status(), "VIN-A");

        assertEquals("msg-dn-001", messageId);
        verify(kafkaTemplate).send(any(Message.class));
    }
}
