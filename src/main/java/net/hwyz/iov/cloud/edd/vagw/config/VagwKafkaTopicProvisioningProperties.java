package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * VAGW Kafka Topic Provisioning 环境参数（EDD-VAGW-DSN-CR-008 §3）。
 * <p>
 * 由 {@code VagwFotaTopicDefinitionProvider} 读取并构造 KafkaTopicDefinition；
 * 分区数、副本数等环境参数通过 Nacos 配置绑定覆盖。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.kafka.topic-provisioning")
public class VagwKafkaTopicProvisioningProperties {

    /** Topic 分区数 */
    private int partitions = 3;

    /** Topic 副本数（默认 1，适配单节点 broker；生产多副本环境经 Nacos 覆盖） */
    private short replicationFactor = 1;
}
