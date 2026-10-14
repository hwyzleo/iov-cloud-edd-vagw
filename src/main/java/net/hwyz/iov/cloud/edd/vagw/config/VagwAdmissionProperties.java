package net.hwyz.iov.cloud.edd.vagw.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
        /**
         * 入站证书编码能力声明（EDD-VAGW-DSN-CR-014 §8）：固定 {@code [PEM, BASE64_DER]}。
         * 启动校验必须恰好包含两项；不作为灰度/降级开关，运行时始终同时启用两种解析能力，
         * 不支持动态刷新。
         */
        private List<String> acceptedEncodings = new ArrayList<>(List.of("PEM", "BASE64_DER"));
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

    /**
     * 启动校验入站证书编码能力声明（EDD-VAGW-DSN-CR-014 §8）：必须恰好为 {@code {PEM, BASE64_DER}}，
     * 缺失、未知值或重复配置均启动失败（fail-fast）。该配置不控制解析分支：运行时始终同时启用
     * PEM 与 Base64(DER)，也不会因配置变化关闭其中一种。
     */
    @PostConstruct
    void validateAcceptedEncodings() {
        Set<String> expected = Set.of("PEM", "BASE64_DER");
        List<String> encodings = peerCertificate.getAcceptedEncodings();
        if (encodings == null) {
            throw new IllegalStateException(
                    "vagw.admission.peer-certificate.accepted-encodings must be exactly [PEM, BASE64_DER]");
        }
        Set<String> seen = new HashSet<>();
        for (String encoding : encodings) {
            if (encoding == null || encoding.isBlank()) {
                throw new IllegalStateException(
                        "vagw.admission.peer-certificate.accepted-encodings contains blank value");
            }
            String normalized = encoding.trim().toUpperCase(Locale.ROOT);
            if (!expected.contains(normalized)) {
                throw new IllegalStateException(
                        "vagw.admission.peer-certificate.accepted-encodings contains unknown value: " + encoding);
            }
            if (!seen.add(normalized)) {
                throw new IllegalStateException(
                        "vagw.admission.peer-certificate.accepted-encodings contains duplicate value: " + encoding);
            }
        }
        if (!seen.equals(expected)) {
            throw new IllegalStateException(
                    "vagw.admission.peer-certificate.accepted-encodings must be exactly [PEM, BASE64_DER]");
        }
    }
}
