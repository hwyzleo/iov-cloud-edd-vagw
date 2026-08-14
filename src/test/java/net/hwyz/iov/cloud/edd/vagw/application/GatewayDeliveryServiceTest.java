package net.hwyz.iov.cloud.edd.vagw.application;

import net.hwyz.iov.cloud.edd.vagw.adapter.kafka.GatewayDeliveryStatusProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;
import vagw.v1.Delivery;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 技术投递结果服务测试（EDD-VAGW-DSN-CR-006 §8）。
 */
@ExtendWith(MockitoExtension.class)
class GatewayDeliveryServiceTest {

    @Mock
    private GatewayDeliveryStatusProducer producer;

    @InjectMocks
    private GatewayDeliveryService service;

    private Envelope.VehicleMessageEnvelope envelope() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId("req-1")
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion("fota-v1")
                .setDeviceId("DEVICE001")
                .setVin("VIN-A")
                .setMessageId("msg-dn-001")
                .setCorrelationId("corr-1")
                .setPayloadType("vehicle.fota.v1.TaskCheckResponse")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_RESPONSE)
                .setService("vehicle.fota")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2}))
                .build();
    }

    @Test
    void produceRejected_shouldBuildStatusAndProduce() {
        when(producer.produce(any(), eq("VIN-A"))).thenReturn("msg-dn-001");

        String result = service.produceRejected(envelope(), "VIN-A", DeliveryReason.VEHICLE_OFFLINE);

        assertEquals("msg-dn-001", result);
        verify(producer).produce(argThat(status -> {
            assertEquals("msg-dn-001", status.getOriginalMessageId());
            assertEquals("corr-1", status.getCorrelationId());
            assertEquals("VIN-A", status.getVin());
            assertEquals(Delivery.Outcome.OUTCOME_REJECTED, status.getOutcome());
            assertEquals("VEHICLE_OFFLINE", status.getReason());
            assertTrue(status.getRetryable());
            assertTrue(status.hasRetryAfterMs());
            assertEquals(300_000L, status.getRetryAfterMs());
            assertTrue(status.getOccurredAtMs() > 0);
            return true;
        }), eq("VIN-A"));
    }

    @Test
    void produceRejected_nonRetryable_shouldNotSetRetryAfter() {
        when(producer.produce(any(), eq("VIN-A"))).thenReturn("msg-dn-001");

        service.produceRejected(envelope(), "VIN-A", DeliveryReason.DEVICE_MISMATCH);

        verify(producer).produce(argThat(status -> {
            assertEquals(Delivery.Outcome.OUTCOME_REJECTED, status.getOutcome());
            assertFalse(status.getRetryable());
            assertFalse(status.hasRetryAfterMs());
            return true;
        }), eq("VIN-A"));
    }
}
