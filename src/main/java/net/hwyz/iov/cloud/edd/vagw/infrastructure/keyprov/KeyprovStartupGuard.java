package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Keyprov 启动就绪门禁（EDD-VAGW-DSN-CR-016 §4）。
 * <p>
 * {@code enabled=true} 时在启动阶段统一校验：
 * <ul>
 *   <li>edd-vmd-api 官方 Keyprov Service Feign Bean 必须存在——缺失即部署错误 fail-fast，
 *       不得创建 Disabled Client 或第二套自定义 Feign 契约；</li>
 *   <li>容量／deadline／连接／熔断配置必须有安全边界且相互不矛盾。</li>
 * </ul>
 * 既没有显式配置、也没有安全默认值的字段（{@code max-in-flight}、{@code vmd.deadline}、
 * 熔断窗口等绑定为 {@code 0}/{@code null}）视为缺失；绑定为 0、负数或组合矛盾同样 fail-fast。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KeyprovStartupGuard {

    private final KeyprovProperties properties;
    private final ObjectProvider<VmdKeyProvisioningService> vmdService;

    public KeyprovStartupGuard(KeyprovProperties properties,
                               ObjectProvider<VmdKeyProvisioningService> vmdService) {
        this.properties = properties;
        this.vmdService = vmdService;
    }

    @PostConstruct
    public void validate() {
        requireOfficialVmdContract();
        validateCapacity();
        log.info("Keyprov startup guard passed: maxInFlight={}, workers={}, queueCapacity={}, queueWait={}, "
                        + "vmdDeadline={}, circuitBreaker={}",
                properties.getMaxInFlight(), properties.effectiveWorkerCount(), properties.getQueueCapacity(),
                properties.getQueueWait(), properties.getVmd().getDeadline(),
                properties.getCircuitBreaker() == null ? null : properties.getCircuitBreaker().isEnabled());
    }

    private void requireOfficialVmdContract() {
        if (vmdService.getIfAvailable() == null) {
            throw new IllegalStateException(
                    "vagw.keyprov.enabled=true but the official edd-vmd-api Keyprov Service Feign bean "
                            + "(VmdKeyProvisioningService) is missing; VAGW must not create a Disabled Client "
                            + "or a second custom @FeignClient/URL/DTO/fallback");
        }
    }

    private void validateCapacity() {
        require(properties.getMaxInFlight() > 0,
                "vagw.keyprov.max-in-flight must be > 0 when keyprov is enabled");
        if (properties.getWorkerCount() < 0) {
            fail("vagw.keyprov.worker-count must be >= 0 (0 = follow max-in-flight)");
        }
        if (properties.getQueueCapacity() < 0) {
            fail("vagw.keyprov.queue-capacity must be >= 0");
        }
        if (properties.getQueueWait() != null && properties.getQueueWait().isNegative()) {
            fail("vagw.keyprov.queue-wait must be >= 0");
        }
        Duration deadline = properties.getVmd().getDeadline();
        if (deadline == null || deadline.isZero() || deadline.isNegative()) {
            fail("vagw.keyprov.vmd.deadline must be > 0 when keyprov is enabled");
        }
        validateCircuitBreaker();
    }

    private void validateCircuitBreaker() {
        KeyprovProperties.CircuitBreaker cb = properties.getCircuitBreaker();
        if (cb == null || !cb.isEnabled()) {
            return;
        }
        if (cb.getFailureWindow() == null || cb.getFailureWindow().isZero() || cb.getFailureWindow().isNegative()) {
            fail("vagw.keyprov.circuit-breaker.failure-window must be > 0 when enabled");
        }
        if (cb.getOpenDuration() == null || cb.getOpenDuration().isZero() || cb.getOpenDuration().isNegative()) {
            fail("vagw.keyprov.circuit-breaker.open-duration must be > 0 when enabled");
        }
        if (cb.getHalfOpenProbes() <= 0) {
            fail("vagw.keyprov.circuit-breaker.half-open-probes must be > 0 when enabled");
        }
        if (cb.getMinimumCalls() <= 0) {
            fail("vagw.keyprov.circuit-breaker.minimum-calls must be > 0 when enabled");
        }
        if (cb.getFailureRateThreshold() <= 0d || cb.getFailureRateThreshold() > 1d) {
            fail("vagw.keyprov.circuit-breaker.failure-rate-threshold must be in (0, 1]");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            fail(message);
        }
    }

    private static void fail(String message) {
        throw new IllegalStateException("Keyprov configuration invalid: " + message);
    }
}
