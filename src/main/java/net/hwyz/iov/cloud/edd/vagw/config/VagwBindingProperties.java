package net.hwyz.iov.cloud.edd.vagw.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 接入身份绑定解析配置（EDD-VAGW-DSN-CR-020 §6）。
 * <p>
 * TTL 语义已变更：{@code cache-ttl-seconds} 只表示<b>加速层陈旧上限</b>，不再是映射业务可用期；
 * hsmUid→VIN 映射的有效期与会话生命周期对齐（命中滑动续期，受
 * {@code session-max-ttl-seconds} 单会话上限约束）。上行未命中必须回源 VIAM，
 * 因此 TTL 到期不再导致上行被拒。
 * </p>
 * <p>
 * 启动自检（fail-fast）：session max 不得小于 cache ttl，也不得超过会话记录 TTL；
 * 上行回源并发、deadline 必须有界。生产环境不允许静默关闭上行回源。
 * </p>
 */
@Slf4j
@Data
@Component
@ConfigurationProperties(prefix = "vagw.binding")
public class VagwBindingProperties {

    /** 会话记录 TTL（与 SessionService 的 SESSION_TTL_HOURS=24 保持一致） */
    public static final long SESSION_RECORD_TTL_SECONDS = 24 * 60 * 60L;

    /** 加速层陈旧上限（秒）；语义见类注释，不再是业务可用期 */
    private long cacheTtlSeconds = 300L;

    /** 单会话内 hsmUid→VIN 映射最长有效期（秒），滑动续期上限 */
    private long sessionMaxTtlSeconds = 1800L;

    /** 上行未命中回源（A 方案） */
    private UplinkResolve uplinkResolve = new UplinkResolve();

    /** 会话期续期与断连清理（B 方案） */
    private SessionRenew sessionRenew = new SessionRenew();

    @Data
    public static class UplinkResolve {
        /** 是否启用上行未命中回源；false 仅用于非生产排障，生产应恒为 true */
        private boolean enabled = true;
        /** 单次回源 budget（毫秒），须覆盖 VIAM identity Feign connect+read 的最坏情况 */
        private long deadlineMs = 500L;
        /** 上行回源并发许可上限，耗尽即过载拒绝且不调用 VIAM */
        private int maxConcurrent = 8;
        /** 同一 (hsmUid, vin) 并发未命中是否合并为一次 VIAM 调用 */
        private boolean singleFlight = true;
    }

    @Data
    public static class SessionRenew {
        /** 是否启用会话期续期与断连清理 */
        private boolean enabled = true;
        /** 上行缓存命中时滑动续期（受 sessionMaxTtlSeconds 封顶） */
        private boolean renewOnHit = true;
        /** 可信 client.disconnected 时清理 hsm 方向会话级映射 */
        private boolean cleanOnDisconnect = true;
    }

    @PostConstruct
    public void validate() {
        if (cacheTtlSeconds <= 0) {
            throw new IllegalStateException(
                    "vagw.binding.cache-ttl-seconds must be positive: " + cacheTtlSeconds);
        }
        if (sessionMaxTtlSeconds < cacheTtlSeconds) {
            throw new IllegalStateException(
                    "vagw.binding.session-max-ttl-seconds must be >= cache-ttl-seconds ("
                            + sessionMaxTtlSeconds + " < " + cacheTtlSeconds + ")");
        }
        if (sessionMaxTtlSeconds > SESSION_RECORD_TTL_SECONDS) {
            throw new IllegalStateException(
                    "vagw.binding.session-max-ttl-seconds must be <= session record TTL "
                            + SESSION_RECORD_TTL_SECONDS + ": " + sessionMaxTtlSeconds);
        }
        if (uplinkResolve.getDeadlineMs() < 50 || uplinkResolve.getDeadlineMs() > 3000) {
            throw new IllegalStateException(
                    "vagw.binding.uplink-resolve.deadline-ms out of range [50,3000]: "
                            + uplinkResolve.getDeadlineMs());
        }
        if (uplinkResolve.getMaxConcurrent() < 1 || uplinkResolve.getMaxConcurrent() > 64) {
            throw new IllegalStateException(
                    "vagw.binding.uplink-resolve.max-concurrent out of range [1,64]: "
                            + uplinkResolve.getMaxConcurrent());
        }
        if (!uplinkResolve.isEnabled()) {
            // 非生产排障开关：允许启动但必须显式告警，避免生产静默退化为“纯缓存”上行
            log.warn("SEC-BINDING | vagw.binding.uplink-resolve.enabled=false: uplink binding "
                    + "resolution falls back to cache-only and TTL expiry will reject uplink "
                    + "(non-production troubleshooting only)");
        }
    }
}
