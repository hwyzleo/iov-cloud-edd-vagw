package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwKafkaTopicProvisioningProperties;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VagwDeviceOnlineTopicDefinitionProvider 单元测试（EDD-VAGW-DSN-CR-012 §2/§8）。
 */
@DisplayName("VagwDeviceOnlineTopicDefinitionProvider 测试")
class VagwDeviceOnlineTopicDefinitionProviderTest {

    private VagwDeviceOnlineProperties properties;
    private VagwKafkaTopicProvisioningProperties provisioningProperties;
    private VagwDeviceOnlineTopicDefinitionProvider provider;

    @BeforeEach
    void setUp() {
        properties = new VagwDeviceOnlineProperties();
        provisioningProperties = new VagwKafkaTopicProvisioningProperties();
        provisioningProperties.setPartitions(3);
        provisioningProperties.setReplicationFactor((short) 2);
        provider = new VagwDeviceOnlineTopicDefinitionProvider(properties, provisioningProperties);
    }

    @Test
    @DisplayName("声明固定 Topic vagw.device.online 并继承项目级分区/副本")
    void declaresFixedTopicWithInheritedParams() {
        Collection<KafkaTopicDefinition> definitions = provider.topicDefinitions();
        assertEquals(1, definitions.size());
        KafkaTopicDefinition definition = definitions.iterator().next();
        assertEquals("vagw.device.online", definition.name());
        assertEquals(3, definition.partitions());
        assertEquals((short) 2, definition.replicationFactor());
    }

    @Test
    @DisplayName("环境覆盖：Topic 名称与分区/副本可注入")
    void reflectsOverrides() {
        properties.setTopic("vagw.device.online.staging");
        properties.setPartitions(6);
        properties.setReplicationFactor((short) 3);

        KafkaTopicDefinition definition = provider.topicDefinitions().iterator().next();
        assertEquals("vagw.device.online.staging", definition.name());
        assertEquals(6, definition.partitions());
        assertEquals((short) 3, definition.replicationFactor());
    }
}
