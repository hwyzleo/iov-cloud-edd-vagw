package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import net.hwyz.iov.cloud.edd.vagw.infrastructure.VehicleBridgeException;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import vehicle.common.v1.Envelope;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 公共 Envelope Kafka 生产者测试（EDD-VAGW-DSN-CR-008 §3.3 就绪门禁）。
 */
@ExtendWith(MockitoExtension.class)
class EnvelopeKafkaProducerTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;
    @Mock
    private ObjectProvider<KafkaTopicProvisioningStatus> statusProvider;
    @Mock
    private KafkaTopicProvisioningStatus provisioningStatus;

    private EnvelopeKafkaProducer newProducer() {
        return new EnvelopeKafkaProducer(kafkaTemplate, statusProvider);
    }

    private Envelope.VehicleMessageEnvelope envelope() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId("req-1")
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion("fota-v1")
                .setDeviceId("DEVICE001")
                .setVin("VIN-A")
                .setMessageId("msg-1")
                .setPayloadType("vehicle.fota.v1.TaskCheckRequest")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setService("vehicle.fota")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
    }

    private void stubSendSuccess() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("vagw.fota", 0), 0L, 7, 0L, 0, 0);
        SendResult<String, byte[]> result = new SendResult<>(
                new ProducerRecord<>("vagw.fota", "VIN-A", new byte[]{1}), metadata);
        when(kafkaTemplate.send(any(Message.class)))
                .thenReturn(CompletableFuture.completedFuture(result));
    }

    @Test
    @DisplayName("Provisioning NOT_READY 时不发送并抛异常")
    void notReady_shouldRejectSend() {
        when(statusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.NOT_READY);
        when(provisioningStatus.missingTopics()).thenReturn(Set.of("vagw.fota"));

        EnvelopeKafkaProducer producer = newProducer();
        Envelope.VehicleMessageEnvelope e = envelope();

        VehicleBridgeException ex = assertThrows(VehicleBridgeException.class,
                () -> producer.sendUplink("vagw.fota", "VIN-A", e.toByteArray(), e, "2026-01-01T00:00:00Z"));
        assertTrue(ex.getMessage().contains("not ready"));
        verify(kafkaTemplate, never()).send(any(Message.class));
    }

    @Test
    @DisplayName("Provisioning READY 时正常发送")
    void ready_shouldSend() throws Exception {
        when(statusProvider.getIfAvailable()).thenReturn(provisioningStatus);
        when(provisioningStatus.state()).thenReturn(KafkaTopicProvisioningStatus.State.READY);
        stubSendSuccess();

        EnvelopeKafkaProducer producer = newProducer();
        Envelope.VehicleMessageEnvelope e = envelope();

        EnvelopeKafkaProducer.SendResult result =
                producer.sendUplink("vagw.fota", "VIN-A", e.toByteArray(), e, "2026-01-01T00:00:00Z");

        assertEquals(0, result.partition());
        assertEquals(7L, result.offset());
        verify(kafkaTemplate).send(any(Message.class));
    }

    @Test
    @DisplayName("Provisioning 未启用（无 Status Bean）时不拦截")
    void disabled_shouldSend() throws Exception {
        when(statusProvider.getIfAvailable()).thenReturn(null);
        stubSendSuccess();

        EnvelopeKafkaProducer producer = newProducer();
        Envelope.VehicleMessageEnvelope e = envelope();

        EnvelopeKafkaProducer.SendResult result =
                producer.sendUplink("vagw.fota", "VIN-A", e.toByteArray(), e, "2026-01-01T00:00:00Z");

        assertEquals(0, result.partition());
        verify(kafkaTemplate).send(any(Message.class));
    }
}
