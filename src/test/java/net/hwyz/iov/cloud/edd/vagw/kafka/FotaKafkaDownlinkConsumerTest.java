package net.hwyz.iov.cloud.edd.vagw.kafka;

import net.hwyz.iov.cloud.edd.vagw.service.FotaBridgeAppService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.Mockito.*;

/**
 * FotaKafkaDownlinkConsumer 单元测试（EDD-VAGW-DSN-CR-005 §6 手动 ack）。
 */
@ExtendWith(MockitoExtension.class)
class FotaKafkaDownlinkConsumerTest {

    @Mock
    private FotaBridgeAppService fotaBridgeAppService;
    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private FotaKafkaDownlinkConsumer consumer;

    private ConsumerRecord<String, byte[]> record() {
        return new ConsumerRecord<>("iov.vagw.down.fota", 0, 10L, "VIN-A", new byte[]{1});
    }

    @Test
    void consume_processed_shouldAcknowledge() {
        ConsumerRecord<String, byte[]> rec = record();
        when(fotaBridgeAppService.processFotaDownlink(rec)).thenReturn(true);

        consumer.consume(rec, acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    @Test
    void consume_notProcessed_shouldNotAcknowledge() {
        ConsumerRecord<String, byte[]> rec = record();
        when(fotaBridgeAppService.processFotaDownlink(rec)).thenReturn(false);

        consumer.consume(rec, acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void consume_exception_shouldNotAcknowledge() {
        ConsumerRecord<String, byte[]> rec = record();
        when(fotaBridgeAppService.processFotaDownlink(rec)).thenThrow(new RuntimeException("boom"));

        consumer.consume(rec, acknowledgment);

        verify(acknowledgment, never()).acknowledge();
    }
}
