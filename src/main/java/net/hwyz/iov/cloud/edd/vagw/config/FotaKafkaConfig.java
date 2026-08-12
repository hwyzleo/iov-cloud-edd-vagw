package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * FOTA 下行 Kafka 手动 ack 容器工厂配置，对齐 EDD-VAGW-DSN-CR-005 §6。
 * <p>
 * offset 仅在本地处理结果可靠记录（FotaBridgeInbox）后提交。
 * </p>
 */
@Slf4j
@Configuration
public class FotaKafkaConfig {

    @Value("${vagw.kafka.fota.downlink-concurrency:3}")
    private int downlinkConcurrency;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, byte[]>>
    fotaKafkaListenerContainerFactory(ConsumerFactory<String, byte[]> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(downlinkConcurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
