package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 设备准入配置（EDD-VAGW-DSN-CR-013 §7）。
 * <p>
 * 超时预算基线：framework PKI total-timeout=1s，VAGW→TSP read-timeout=1.5s，
 * EMQX→VAGW HTTP auth timeout 至少 2s；必须满足
 * {@code PKI total-timeout ≤ VAGW read-timeout - 250ms} 且
 * {@code VAGW read-timeout ≤ EMQX auth timeout - 500ms}。
 * </p>
 * <p>
 * {@code peer-certificate.required} 仅用于发布编排：生产固定 true；false 时设备准入能力
 * NOT_READY/拒绝（fail-closed），不得恢复旧式仅凭 CN 的 ALLOW 路径。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.admission")
public class VagwAdmissionProperties {

    private PeerCertificate peerCertificate = new PeerCertificate();

    private Tsp tsp = new Tsp();

    private EmqxAuth emqxAuth = new EmqxAuth();

    /**
     * 叶子证书上下文配置。
     */
    @Data
    public static class PeerCertificate {
        /** 发布编排门禁：false 时准入 NOT_READY/拒绝，绝不回退到仅凭 CN 的允许路径 */
        private boolean required = true;
        /** 解析前后 DER 上限（字节），超限 fail-closed，不截断、不解析 */
        private int maxDerBytes = 8192;
    }

    /**
     * VAGW → TSP 准入裁决调用配置。
     */
    @Data
    public static class Tsp {
        /** canonical 裁决路径（供部署登记与漂移核对） */
        private String path = "/api/service/deviceAdmission/v1/decide";
        private Duration connectTimeout = Duration.ofMillis(250);
        private Duration readTimeout = Duration.ofMillis(1500);
        /** 读超时代表结果未知，禁止在同次调用内重试 */
        private boolean retryOnReadTimeout = false;
    }

    /**
     * EMQX → VAGW HTTP authn 回调预算（部署前置，用于发布前核对）。
     */
    @Data
    public static class EmqxAuth {
        private Duration requiredTimeout = Duration.ofSeconds(2);
    }
}
