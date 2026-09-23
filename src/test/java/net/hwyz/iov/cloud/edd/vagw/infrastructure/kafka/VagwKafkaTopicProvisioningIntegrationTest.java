package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwKafkaTopicProvisioningProperties;
import net.hwyz.iov.cloud.edd.vagw.test.kafka.VagwKafkaProvisioningTestConfig;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinition;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VAGW Kafka Topic Provisioning 装配集成测试（EDD-VAGW-DSN-CR-008 §3/§8）。
 * <p>
 * 不依赖真实 Kafka Broker：
 * - Catalog 合并 VAGW 作为 Producer 声明的 4 个 FOTA Topic；
 * - 环境配置（vagw.kafka.fota.*）绑定为规范名称；
 * - Broker 不可用时 Provisioning 保持 NOT_READY、缺失集合为声明 Topic，不崩溃且后台可重试。
 * </p>
 */
@SpringBootTest(
        classes = VagwKafkaProvisioningTestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "iov.kafka.topic-provisioning.enabled=true",
                "iov.kafka.topic-provisioning.initial-delay=0s",
                "iov.kafka.topic-provisioning.retry.initial-interval=500ms",
                "iov.kafka.topic-provisioning.retry.max-interval=2s",
                "iov.kafka.topic-provisioning.retry.multiplier=2.0",
                "vagw.kafka.topic-provisioning.partitions=3",
                "vagw.kafka.topic-provisioning.replication-factor=1",
                "spring.kafka.admin.properties.request.timeout.ms=1000",
                "spring.kafka.admin.properties.retries=0",
                "spring.kafka.admin.properties.default.api.timeout.ms=1000",
                "spring.cloud.nacos.config.enabled=false",
                "spring.cloud.nacos.discovery.enabled=false"
        })
@DisplayName("VAGW Kafka Topic Provisioning 装配测试")
class VagwKafkaTopicProvisioningIntegrationTest {

    @Autowired
    private KafkaTopicCatalog kafkaTopicCatalog;
    @Autowired
    private KafkaTopicProvisioningStatus provisioningStatus;
    @Autowired
    private VagwFotaTopicProperties fotaTopicProperties;
    @Autowired
    private VagwKafkaTopicProvisioningProperties provisioningProperties;
    @Autowired
    private VagwFotaTopicDefinitionProvider provider;

    @Test
    @DisplayName("Catalog 合并 VAGW 生产的 4 个 FOTA Topic，不含 IOV-OTA 下行业务 Topic")
    void catalogContainsProducerTopics() {
        assertTrue(kafkaTopicCatalog.contains("vagw.fota"), "缺少 vagw.fota");
        assertTrue(kafkaTopicCatalog.contains("vagw.fota.delivery"), "缺少 vagw.fota.delivery");
        assertTrue(kafkaTopicCatalog.contains("vagw.fota.dlq.up"), "缺少 vagw.fota.dlq.up");
        assertTrue(kafkaTopicCatalog.contains("vagw.fota.dlq.down"), "缺少 vagw.fota.dlq.down");
        assertFalse(kafkaTopicCatalog.contains("iov.vagw.down.fota"),
                "不应声明 IOV-OTA 生产的 FOTA 下行业务 Topic");
    }

    @Test
    @DisplayName("Topic 名称环境配置（vagw.kafka.fota.*）绑定为规范名称")
    void topicNamesBoundFromConfig() {
        assertEquals("vagw.fota", fotaTopicProperties.getUplinkTopic());
        assertEquals("vagw.fota.delivery", fotaTopicProperties.getDeliveryTopic());
        assertEquals("vagw.fota.dlq.up", fotaTopicProperties.getUplinkDlqTopic());
        assertEquals("vagw.fota.dlq.down", fotaTopicProperties.getDownlinkDlqTopic());
    }

    @Test
    @DisplayName("Definition 携带环境注入的分区与副本数")
    void definitionsCarryConfiguredParams() {
        for (KafkaTopicDefinition def : provider.topicDefinitions()) {
            assertEquals(3, def.partitions());
            assertEquals((short) 1, def.replicationFactor());
        }
    }

    @Test
    @DisplayName("Provisioning 状态机推进：Topic 就绪→READY 且无缺失；Broker 不可用→NOT_READY 且记录失败与重试")
    void provisioningStateMachineAdvances() throws Exception {
        // 环境无关断言：等待状态从初始 NOT_READY 推进（本机 Nacos 提供真实 broker 时→READY；
        // 无 broker 时→NOT_READY 并记录失败与后台重试，进程不退出）。
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (provisioningStatus.state() == KafkaTopicProvisioningStatus.State.READY) {
                break;
            }
            if (provisioningStatus.state() == KafkaTopicProvisioningStatus.State.NOT_READY
                    && provisioningStatus.lastFailure().isPresent()) {
                break;
            }
            Thread.sleep(300);
        }

        if (provisioningStatus.state() == KafkaTopicProvisioningStatus.State.READY) {
            assertTrue(provisioningStatus.missingTopics().isEmpty(),
                    "READY 时不应有缺失 Topic");
        } else {
            assertEquals(KafkaTopicProvisioningStatus.State.NOT_READY, provisioningStatus.state());
            assertEquals(4, provisioningStatus.missingTopics().size(),
                    "describe 不可靠时缺失集合应为全部声明 Topic");
            assertTrue(provisioningStatus.missingTopics().contains("vagw.fota"));
            assertTrue(provisioningStatus.lastFailure().isPresent(), "应记录最近一次失败异常");
            assertTrue(provisioningStatus.nextRetryAt().isPresent(), "应安排后台自动重试");
        }
    }
}
