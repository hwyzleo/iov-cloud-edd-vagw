package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * VAGW 设备上线事件（{@code vagw.device.online}）配置（EDD-VAGW-DSN-CR-012 §2/§3.5）。
 * <p>
 * 承载 Topic 名称、Outbox（Redis Stream）批量/超时/退避、准入快照 TTL、会话对账与
 * （可选）EMQX 管理面断连参数。分区数与副本数默认继承
 * {@code vagw.kafka.topic-provisioning.*}，仅当显式配置 &gt; 0 时覆盖。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.kafka.device-online")
public class VagwDeviceOnlineProperties {

    /** 设备上线事件 Topic（VAGW Producer 唯一出口，EDD-VAGW-DSN-CR-012 §2.1） */
    private String topic = "vagw.device.online";

    /** 是否允许生产 DEVICE_ONLINE；关闭时不写 Outbox、不发布（Topic 仍进入 Provisioning Catalog） */
    private boolean enabled = true;

    /** 单轮从 Stream 拉取/认领的最大条数 */
    private int outboxBatchSize = 100;

    /** Kafka produce 同步等待超时 */
    private Duration publishTimeout = Duration.ofSeconds(3);

    /** eventId 去重记录保留时长（Hook 重试幂等 / 会话对账标记） */
    private Duration dedupTtl = Duration.ofDays(7);

    /** Stream 消费者租约：pending 超过该时长视为可被其他实例认领（多实例条件抢占） */
    private Duration leaseTimeout = Duration.ofSeconds(30);

    /** 发布失败初始退避 */
    private Duration backoffInitial = Duration.ofSeconds(5);

    /** 发布失败最大退避 */
    private Duration backoffMax = Duration.ofMinutes(5);

    /** Hook 持久化（会话 + Outbox）有界恢复次数 */
    private int persistRetryAttempts = 3;

    /** Hook 持久化重试间隔 */
    private Duration persistRetryBackoff = Duration.ofMillis(50);

    /** 会话对账单轮扫描上限 */
    private int reconcileBatchSize = 200;

    /** 会话对账扫描间隔（毫秒） */
    private long reconcileIntervalMs = 60_000L;

    /** Stream 近似最大长度（内存有界） */
    private long streamMaxLen = 100_000L;

    /** 发布轮询间隔（毫秒） */
    private long publishIntervalMs = 5_000L;

    /** 消费者组名 */
    private String consumerGroup = "vagw-device-online";

    /** Topic 分区数覆盖；0 表示继承 vagw.kafka.topic-provisioning.partitions */
    private int partitions = 0;

    /** Topic 副本数覆盖；0 表示继承 vagw.kafka.topic-provisioning.replication-factor */
    private short replicationFactor = 0;

    /** 准入快照候选态 TTL（秒），供给 connected 关联 */
    private long admissionTtlSeconds = 300;

    /** EMQX 管理面（可选）：fail-closed 后主动断连已建立连接 */
    private EmqxApi emqxApi = new EmqxApi();

    @Data
    public static class EmqxApi {
        /** 是否启用管理面断连；未启用时仅做会话下线与告警 */
        private boolean enabled = false;
        /** EMQX Dashboard/API Base URL，例如 http://emqx:18083 */
        private String baseUrl = "";
        /** API Key（经 Secret 注入，不落日志） */
        private String apiKey = "";
        /** API Secret（经 Secret 注入，不落日志） */
        private String apiSecret = "";
    }
}
