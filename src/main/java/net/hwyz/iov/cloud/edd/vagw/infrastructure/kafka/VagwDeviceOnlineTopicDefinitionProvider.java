package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwKafkaTopicProvisioningProperties;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinition;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinitionProvider;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

/**
 * 设备上线 Topic 定义提供者（EDD-VAGW-DSN-CR-012 §2.2）。
 * <p>
 * 向 FW-KAFKA 声明 VAGW 作为 Producer 的 {@code vagw.device.online}，由框架统一完成
 * Catalog 合并、存在性检查、幂等创建、后台重试与 READY 状态传播；已存在 Topic 不修改配置，
 * 非法或同名冲突定义启动 fail-fast。分区/副本数默认继承项目级配置，可按 Topic 覆盖。
 * </p>
 */
@Component
@RequiredArgsConstructor
public class VagwDeviceOnlineTopicDefinitionProvider implements KafkaTopicDefinitionProvider {

    private final VagwDeviceOnlineProperties properties;
    private final VagwKafkaTopicProvisioningProperties provisioningProperties;

    @Override
    public Collection<KafkaTopicDefinition> topicDefinitions() {
        int partitions = properties.getPartitions() > 0
                ? properties.getPartitions() : provisioningProperties.getPartitions();
        short replicationFactor = properties.getReplicationFactor() > 0
                ? properties.getReplicationFactor() : provisioningProperties.getReplicationFactor();
        return List.of(new KafkaTopicDefinition(properties.getTopic(), partitions, replicationFactor));
    }
}
