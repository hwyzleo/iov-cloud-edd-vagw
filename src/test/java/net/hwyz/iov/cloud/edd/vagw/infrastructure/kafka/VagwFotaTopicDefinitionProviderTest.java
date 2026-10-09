package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwKafkaTopicProvisioningProperties;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VAGW Producer FOTA Topic 定义提供者测试（EDD-VAGW-DSN-CR-008 §3）。
 * <p>
 * 验证 VAGW 作为 Producer 的 4 个 FOTA Topic 声明：vagw.fota / vagw.fota.delivery /
 * vagw.fota.dlq.up / vagw.fota.dlq.down 全覆盖，无遗漏、无多余、无重复；
 * 不声明 IOV-OTA 生产的 FOTA 下行业务 Topic（ota.fota）。
 * </p>
 */
@DisplayName("VagwFotaTopicDefinitionProvider 测试")
class VagwFotaTopicDefinitionProviderTest {

    private VagwFotaTopicDefinitionProvider provider;
    private VagwFotaTopicProperties topicProperties;

    @BeforeEach
    void setUp() {
        topicProperties = new VagwFotaTopicProperties();
        VagwKafkaTopicProvisioningProperties provisioningProperties =
                new VagwKafkaTopicProvisioningProperties();
        provisioningProperties.setPartitions(3);
        provisioningProperties.setReplicationFactor((short) 3);
        provider = new VagwFotaTopicDefinitionProvider(topicProperties, provisioningProperties);
    }

    private Set<String> declaredTopics() {
        Collection<KafkaTopicDefinition> definitions = provider.topicDefinitions();
        return definitions.stream().map(KafkaTopicDefinition::name).collect(Collectors.toSet());
    }

    @Nested
    @DisplayName("Producer Topic 声明")
    class ProducerTopicTests {

        @Test
        @DisplayName("声明 VAGW 生产的 4 个 FOTA Topic")
        void declaresAllProducerTopics() {
            Set<String> topics = declaredTopics();
            List<String> expected = List.of(
                    "vagw.fota",
                    "vagw.fota.delivery",
                    "vagw.fota.dlq.up",
                    "vagw.fota.dlq.down");
            for (String topic : expected) {
                assertTrue(topics.contains(topic), "缺少 Producer Topic: " + topic);
            }
        }

        @Test
        @DisplayName("不声明 IOV-OTA 生产的 FOTA 下行业务 Topic")
        void doesNotDeclareDownstreamTopic() {
            Set<String> topics = declaredTopics();
            assertFalse(topics.contains("ota.fota"),
                    "不应声明 FOTA 下行业务 Topic（Producer 为 IOV-OTA）");
        }

        @Test
        @DisplayName("只声明 vagw.fota 域 Topic，无多余")
        void onlyDeclaresVagwFotaTopics() {
            Set<String> topics = declaredTopics();
            assertTrue(topics.stream().allMatch(t -> t.startsWith("vagw.fota")),
                    "只应声明 vagw.fota 域 Topic");
            assertEquals(4, topics.size(), "应恰好声明 4 个 Producer Topic");
        }

        @Test
        @DisplayName("声明无重复")
        void noDuplicateTopics() {
            Collection<KafkaTopicDefinition> definitions = provider.topicDefinitions();
            assertEquals(declaredTopics().size(), definitions.size(), "声明不应包含重复 topic");
        }
    }

    @Nested
    @DisplayName("配置覆盖与参数注入")
    class ConfigurationTests {

        @Test
        @DisplayName("环境配置覆盖后声明对应 Topic 名称")
        void reflectsConfiguredTopicNames() {
            topicProperties.setUplinkTopic("vagw.fota.staging");
            topicProperties.setDeliveryTopic("vagw.fota.delivery.staging");
            topicProperties.setUplinkDlqTopic("vagw.fota.dlq.up.staging");
            topicProperties.setDownlinkDlqTopic("vagw.fota.dlq.down.staging");

            Set<String> topics = declaredTopics();
            assertTrue(topics.contains("vagw.fota.staging"));
            assertTrue(topics.contains("vagw.fota.delivery.staging"));
            assertTrue(topics.contains("vagw.fota.dlq.up.staging"));
            assertTrue(topics.contains("vagw.fota.dlq.down.staging"));
            assertFalse(topics.contains("vagw.fota"));
        }

        @Test
        @DisplayName("全部 Definition 携带环境注入的分区与副本数")
        void definitionsCarryConfiguredPartitionsAndReplication() {
            for (KafkaTopicDefinition def : provider.topicDefinitions()) {
                assertEquals(3, def.partitions());
                assertEquals((short) 3, def.replicationFactor());
            }
        }
    }
}
