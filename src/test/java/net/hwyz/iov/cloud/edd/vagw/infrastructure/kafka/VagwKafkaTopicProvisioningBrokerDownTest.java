package net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka;

import net.hwyz.iov.cloud.edd.vagw.test.kafka.VagwKafkaProvisioningTestConfig;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VAGW Kafka Topic Provisioning Broker 不可用场景测试（EDD-VAGW-DSN-CR-008 §3.2/§8）。
 * <p>
 * Broker 指向不可达地址且禁用 Nacos 配置源时：服务保持正常启动（不因 Broker 不可用退出），
 * Provisioning 状态为 NOT_READY，缺失集合为全部声明 Topic，记录失败并安排后台自动重试。
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
                "spring.kafka.bootstrap-servers=127.0.0.1:1",
                "spring.kafka.admin.properties.request.timeout.ms=1000",
                "spring.kafka.admin.properties.retries=0",
                "spring.kafka.admin.properties.default.api.timeout.ms=1000",
                "spring.cloud.nacos.config.enabled=false",
                "spring.cloud.nacos.discovery.enabled=false"
        })
@DisplayName("VAGW Kafka Topic Provisioning Broker 不可用测试")
class VagwKafkaTopicProvisioningBrokerDownTest {

    @Autowired
    private KafkaTopicProvisioningStatus provisioningStatus;

    @Test
    @DisplayName("Broker 不可用时保持 NOT_READY、缺失集合为声明 Topic、记录失败并安排后台重试")
    void brokerUnavailableKeepsNotReady() throws Exception {
        // 初始状态即为 NOT_READY（缺失空），需等后台完成一轮检查：lastFailure 出现即证明
        // describe 已失败并进入 NOT_READY（缺失=全部声明）+ 指数退避重试调度。
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && provisioningStatus.lastFailure().isEmpty()) {
            Thread.sleep(300);
        }

        assertEquals(KafkaTopicProvisioningStatus.State.NOT_READY, provisioningStatus.state(),
                "Broker 不可用时 Provisioning 应保持 NOT_READY");
        assertTrue(provisioningStatus.lastFailure().isPresent(), "应记录最近一次失败异常");
        assertEquals(4, provisioningStatus.missingTopics().size(),
                "describe 不可靠时缺失集合应为全部声明 Topic");
        assertTrue(provisioningStatus.missingTopics().contains("vagw.fota"));
        assertTrue(provisioningStatus.missingTopics().contains("vagw.fota.delivery"));
        assertTrue(provisioningStatus.nextRetryAt().isPresent(), "应安排后台自动重试");
    }
}
