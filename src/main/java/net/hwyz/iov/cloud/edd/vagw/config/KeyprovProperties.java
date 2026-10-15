package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * keyprov 能力与门禁配置（EDD-VAGW-DSN-CR-011 §4/§10）。
 * <p>
 * 量产默认 {@code enabled=false}：关闭时不返回任何 keyprov ACL，也不受理 keyprov 上行；
 * 仅当 PAR-PROTO/VAGW/VMD/EMQX/TBOX release-set 全部兼容后才开启。开关变化必须使旧 ACL
 * 失效并要求重授权/重连（capability/config generation 纳入 ACL cache key）。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.keyprov")
public class KeyprovProperties {

    /** 能力总开关。默认关闭（最安全）；生产经 release-set 门禁后显式开启。 */
    private boolean enabled = false;

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
}
