package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * keyprov 能力、容量与门禁配置（EDD-VAGW-DSN-CR-011，由 EDD-VAGW-DSN-CR-016 增量修订）。
 * <p>
 * 量产默认 {@code enabled=true}：Keyprov 作为正式基础能力默认可用；仅显式关闭时才不注册
 * Keyprov ACL、上行 filter、执行器与 Gateway 调用链。
 * </p>
 * <p>
 * CR-016 新增容量／线程／超时／熔断配置：{@code max-in-flight}、{@code queue-capacity}、
 * {@code queue-wait}、{@code worker-count}、{@code vmd.deadline} 与 {@code circuit-breaker.*}。
 * Feign 连接／读取超时使用官方 Client 配置前缀（{@code vmdKeyProvisioningService}）并受统一
 * deadline 约束；不新增 {@code vmd.max-connections} 或 per-client 物理连接池配置。这些字段本身不设方法级默认值（{@code 0}/{@code null}），
 * 由 {@code KeyprovStartupGuard} 在 {@code enabled=true} 时统一校验正数、范围与字段关系并
 * fail-fast；{@code application.yml} 提供保守的 dev/CI 默认值，生产环境经环境变量覆盖。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.keyprov")
public class KeyprovProperties {

    /** 能力总开关。默认启用；显式 false 时不注册 ACL／filter／executor／Gateway 调用链。 */
    private boolean enabled = true;

    /** Envelope.protocol_version 精确 allowlist 值（不透明字符串，禁止前缀/别名/fallback 匹配）。 */
    private String protocolVersion = "keyprov-v1";

    /** Envelope.service 固定值。 */
    private String service = "security.keyprov";

    /** keyprov payload（Envelope.payload）上限：16 KiB。超限整体拒绝，不截断、不分片。 */
    private int maxPayloadBytes = 16384;

    /** 上行原始 Envelope bytes 上限（payload 上限 + Envelope 结构开销）。 */
    private int maxEnvelopeBytes = 17408;

    /** 下行发布 QoS（固定 1，ACL 约束 qos&lt;=1）。 */
    private int qos = 1;

    /** message_id / request_id 等标识长度上限（UTF-8 bytes），有界防滥用。 */
    private int identifierMaxLength = 128;

    /** 传输层短期去重窗口（秒）；窗口内重复 messageId 不重复调用 VMD。 */
    private long inboxTtlSeconds = 300;

    /** 传输层短期去重表最大条目数（有界内存）。 */
    private int inboxMaxEntries = 10000;

    // ---------------------------------------------------------------------
    // CR-016：Keyprov 专属并发与超时策略（不要求与底层物理连接池一一对应）
    // ---------------------------------------------------------------------

    /** 单实例最大 in-flight（并发许可数）。{@code 0} 表示未绑定，由启动门禁 fail-fast。 */
    private int maxInFlight;

    /** 专属执行器 worker 数；{@code 0} 表示跟随 {@link #maxInFlight}。 */
    private int workerCount;

    /** 专属执行器等待队列容量；默认 {@code 0} 为零等待（直接移交）。 */
    private int queueCapacity;

    /** 排队最大等待时间；默认 {@code 0} 表示不等待，许可耗尽立即拒绝。 */
    private Duration queueWait = Duration.ZERO;

    /** VMD 调用与并发参数（{@code vagw.keyprov.vmd.*}）。 */
    private Vmd vmd = new Vmd();

    /** 专属熔断器配置。 */
    private CircuitBreaker circuitBreaker = new CircuitBreaker();

    /**
     * VMD 调用与并发参数。
     */
    @Data
    public static class Vmd {

        /** VMD 单次调用的统一 absolute deadline；{@code null} 表示未绑定，由启动门禁 fail-fast。 */
        private Duration deadline;
    }

    /**
     * Keyprov 专属熔断器配置（仅统计 VMD 连接失败/超时/依赖不可用）。
     */
    @Data
    public static class CircuitBreaker {

        /** 是否启用 Keyprov 专属熔断器（默认启用）。 */
        private boolean enabled = true;

        /** 统计失败率的滑动窗口；{@code null} 表示未绑定，由启动门禁 fail-fast。 */
        private Duration failureWindow;

        /** OPEN 持续时长，到期后进入 HALF_OPEN；{@code null} 表示未绑定，由启动门禁 fail-fast。 */
        private Duration openDuration;

        /** HALF_OPEN 允许的有界探测数量；{@code 0} 表示未绑定，由启动门禁 fail-fast。 */
        private int halfOpenProbes;

        /** 触发熔断所需的最小样本数（有安全默认值）。 */
        private int minimumCalls = 5;

        /** 失败率阈值（0, 1]，达到即 OPEN（有安全默认值）。 */
        private double failureRateThreshold = 0.5d;
    }

    /**
     * 实际 worker 数：未显式配置时跟随 max-in-flight。
     */
    public int effectiveWorkerCount() {
        return workerCount > 0 ? workerCount : maxInFlight;
    }
}
