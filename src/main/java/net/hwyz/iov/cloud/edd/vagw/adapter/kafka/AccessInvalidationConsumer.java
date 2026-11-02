package net.hwyz.iov.cloud.edd.vagw.adapter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.accessinvalidation.AccessInvalidationHandler;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAccessInvalidationProperties;
import net.hwyz.iov.cloud.sec.viam.api.event.AccessInvalidatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 访问失效事件 Kafka 消费者（SEC-VIAM-DSN-CR-001 / FR-VIAM-008 / G-3，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * 消费 {@code viam.access-invalidated}（VIAM 生产），手动 ack：处理与执行回执可靠收敛后才提交
 * offset；处理异常不提交，交由 Kafka 重投（事件幂等/版本判定防重复执行）。
 * 事件 payload 不自报即授信：身份/generation/版本判定由 {@link AccessInvalidationHandler} 本地执行。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccessInvalidationConsumer {

    private final AccessInvalidationHandler handler;
    private final ObjectMapper objectMapper;
    private final VagwAccessInvalidationProperties properties;

    @KafkaListener(
            topics = "#{@vagwAccessInvalidationProperties.topic}",
            groupId = "#{@vagwAccessInvalidationProperties.groupId}",
            containerFactory = "vagwKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        try {
            AccessInvalidatedEvent event = objectMapper.readValue(
                    record.value() == null ? new byte[0] : record.value(), AccessInvalidatedEvent.class);
            handler.handle(event);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            // 不提交 offset，交由 Kafka 重投（handler 内幂等/版本判定防重复执行）
            log.error("access-invalidated consume error: topic={}, partition={}, offset={}, err={}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
        }
    }
}
