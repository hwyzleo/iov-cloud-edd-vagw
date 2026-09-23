package net.hwyz.iov.cloud.edd.vagw.test.kafka;

import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwKafkaTopicProvisioningProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.VagwFotaTopicDefinitionProvider;
import net.hwyz.iov.cloud.framework.kafka.autoconfigure.KafkaTopicProvisioningAutoConfiguration;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * VAGW Kafka Topic Provisioning 装配测试（EDD-VAGW-DSN-CR-008 §3）。
 * <p>
 * 仅装配 FW-KAFKA Topic Provisioning 自动配置 + VAGW 相关 Bean，
 * 不加载 Redis / MySQL / MQTT / Web 等业务自动配置，聚焦 Kafka 行为。
 * 不依赖真实 Kafka Broker（Admin 连接失败时验证 NOT_READY 与后台重试）。
 * </p>
 */
@SpringBootConfiguration
@EnableConfigurationProperties({
        KafkaProperties.class,
        VagwFotaTopicProperties.class,
        VagwKafkaTopicProvisioningProperties.class
})
@ImportAutoConfiguration({
        KafkaAutoConfiguration.class,
        KafkaTopicProvisioningAutoConfiguration.class
})
public class VagwKafkaProvisioningTestConfig {

    /**
     * 复现真实装配：显式提供 Admin Bean，避免框架 kafkaAdmin 与
     * Spring Boot KafkaAutoConfiguration.kafkaAdmin 同名冲突（EDD-VAGW-DSN-CR-008 §3）。
     */
    @Bean(destroyMethod = "close")
    Admin kafkaAdminClient(KafkaProperties properties) {
        return AdminClient.create(properties.buildAdminProperties());
    }

    @Bean
    VagwFotaTopicDefinitionProvider vagwFotaTopicDefinitionProvider(
            VagwFotaTopicProperties fotaTopicProperties,
            VagwKafkaTopicProvisioningProperties provisioningProperties) {
        return new VagwFotaTopicDefinitionProvider(fotaTopicProperties, provisioningProperties);
    }
}
