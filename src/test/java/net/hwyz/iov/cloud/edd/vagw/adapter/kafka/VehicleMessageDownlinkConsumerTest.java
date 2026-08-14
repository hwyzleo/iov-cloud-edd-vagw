package net.hwyz.iov.cloud.edd.vagw.adapter.kafka;

import net.hwyz.iov.cloud.edd.vagw.application.VehicleMessageBridgeService;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 车辆消息下行消费者测试（EDD-VAGW-DSN-CR-006 §7/§9）。
 */
@ExtendWith(MockitoExtension.class)
class VehicleMessageDownlinkConsumerTest {

    @Mock
    private VehicleMessageBridgeService bridgeService;
    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private VehicleMessageDownlinkConsumer consumer;

    private ConsumerRecord<String, byte[]> record() {
        return new ConsumerRecord<>(VehicleRouteCatalog.KAFKA_DOWN_TOPIC, 0, 10L, "VIN-A",
                new byte[]{1, 2, 3});
    }

    @Test
    void consume_processed_shouldAcknowledge() {
        when(bridgeService.processDownlink(any())).thenReturn(true);

        consumer.consume(record(), acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void consume_notProcessed_shouldNotAcknowledge() {
        when(bridgeService.processDownlink(any())).thenReturn(false);

        consumer.consume(record(), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void consume_exception_shouldNotAcknowledge() {
        when(bridgeService.processDownlink(any())).thenThrow(new RuntimeException("boom"));

        consumer.consume(record(), acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }
}
