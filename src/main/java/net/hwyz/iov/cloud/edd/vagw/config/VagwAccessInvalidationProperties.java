package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 访问失效消费配置（SEC-VIAM-DSN-CR-001 / FR-VIAM-008 / G-3，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * VAGW 消费 VIAM 发布的 {@code viam.access-invalidated} 事件（双语义：CONNECTION_DISPOSAL /
 * PRINCIPAL_REVOCATION），按安全版本/generation 判定后执行缓存/ACL 失效与实际断连，并以
 * {@code ViamAccessInvalidationService.receipt()} 回执区分状态。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.access-invalidation")
public class VagwAccessInvalidationProperties {

    /** VIAM 访问失效事件 Topic（VIAM 生产，VAGW 消费；生产经 Nacos/环境变量覆盖） */
    private String topic = "viam.access-invalidated";

    /** VAGW 消费组（多实例共享消费） */
    private String groupId = "edd-vagw-access-invalidation";

    /** 事件幂等去重保留时长（防 Kafka 重投重复执行） */
    private Duration dedupTtl = Duration.ofHours(24);

    /** 乱序版本追踪记录保留时长 */
    private Duration versionTtl = Duration.ofDays(7);

    /** 执行回执提交重试次数（VIAM 超时对账兜底） */
    private int receiptRetryAttempts = 3;

    /** 执行回执提交重试退避 */
    private Duration receiptRetryBackoff = Duration.ofMillis(200);
}
