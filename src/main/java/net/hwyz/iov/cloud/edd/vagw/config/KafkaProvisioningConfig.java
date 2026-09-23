package net.hwyz.iov.cloud.edd.vagw.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * VAGW Kafka 基础设施配置（EDD-VAGW-DSN-CR-008 §3）。
 * <p>
 * 提供 Kafka Admin Bean（类型 {@link Admin}）供 FW-KAFKA Topic Provisioning 使用：
 * 框架对 Admin 缺失时才自建（@ConditionalOnMissingBean(Admin.class)），此处显式提供，
 * 避免与 Spring Boot KafkaAutoConfiguration 的 kafkaAdmin Bean（类型 KafkaAdmin，
 * 不实现 Kafka Admin 接口）同名冲突。
 * </p>
 * <p>
 * Admin 连接参数复用 {@link KafkaProperties}（Nacos kafka.yaml 提供 bootstrap-servers），
 * 运行 Principal 需具备 4 个 Producer Topic 的 DescribeTopics / CreateTopics / Write 权限。
 * </p>
 */
@Configuration
public class KafkaProvisioningConfig {

    /**
     * Kafka Admin 客户端，应用关闭时释放资源。
     */
    @Bean(destroyMethod = "close")
    public Admin kafkaAdminClient(KafkaProperties properties) {
        return AdminClient.create(properties.buildAdminProperties());
    }
}
