package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwKafkaTopicProvisioningProperties;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinition;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinitionProvider;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

/**
 * VAGW Producer FOTA Topic 定义提供者（EDD-VAGW-DSN-CR-008 §3）。
 * <p>
 * 向 FW-KAFKA 声明 VAGW 作为 Producer 的 4 个 FOTA Topic
 * （vagw.fota / vagw.fota.delivery / vagw.fota.dlq.up / vagw.fota.dlq.down），
 * 由框架统一完成 Catalog 合并、存在性检查、幂等创建、后台重试与状态传播。
 * </p>
 * <p>
 * Topic 名称与 {@code VehicleRouteCatalog} 共用 {@link VagwFotaTopicProperties} 配置源；
 * 分区数、副本数通过 {@link VagwKafkaTopicProvisioningProperties} 环境参数注入。
 * 不声明 FOTA 下行业务 Topic（iov.vagw.down.fota）：其 Producer 为 IOV-OTA，不在本 CR 范围。
 * </p>
 */
@Component
@RequiredArgsConstructor
public class VagwFotaTopicDefinitionProvider implements KafkaTopicDefinitionProvider {

    private final VagwFotaTopicProperties fotaTopicProperties;
    private final VagwKafkaTopicProvisioningProperties provisioningProperties;

    @Override
    public Collection<KafkaTopicDefinition> topicDefinitions() {
        List<String> names = List.of(
                fotaTopicProperties.getUplinkTopic(),
                fotaTopicProperties.getDeliveryTopic(),
                fotaTopicProperties.getUplinkDlqTopic(),
                fotaTopicProperties.getDownlinkDlqTopic());
        return names.stream()
                .map(name -> new KafkaTopicDefinition(
                        name,
                        provisioningProperties.getPartitions(),
                        provisioningProperties.getReplicationFactor()))
                .toList();
    }
}
