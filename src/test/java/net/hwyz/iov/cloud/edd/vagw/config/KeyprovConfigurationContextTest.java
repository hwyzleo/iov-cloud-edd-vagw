package net.hwyz.iov.cloud.edd.vagw.config;

import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovAdmissionBulkhead;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovCircuitBreaker;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovExecutor;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovStartupGuard;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Keyprov 条件装配与启动门禁上下文测试（EDD-VAGW-DSN-CR-016 §4/§7-1/§7-2）。
 * <p>
 * 默认 {@code enabled=true} 创建 Keyprov 专属组件；显式 false 时不创建；enabled=true 缺少官方
 * Feign Bean 或缺省/非法容量配置时上下文启动失败（fail-fast）。
 * </p>
 */
class KeyprovConfigurationContextTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(KeyprovComponentConfig.class);

    @Configuration
    @EnableConfigurationProperties(KeyprovProperties.class)
    @Import({KeyprovMetrics.class, KeyprovAdmissionBulkhead.class, KeyprovExecutor.class,
            KeyprovCircuitBreaker.class, KeyprovStartupGuard.class})
    static class KeyprovComponentConfig {
    }

    private static String[] validProperties() {
        return new String[]{
                "vagw.keyprov.max-in-flight=2",
                "vagw.keyprov.vmd.deadline=2s",
                "vagw.keyprov.circuit-breaker.failure-window=30s",
                "vagw.keyprov.circuit-breaker.open-duration=15s",
                "vagw.keyprov.circuit-breaker.half-open-probes=2"
        };
    }

    @Test
    void defaultEnabled_isTrue() {
        assertTrue(new KeyprovProperties().isEnabled(), "keyprov 量产默认必须为 true");
    }

    @Test
    void defaultEnabled_shouldCreateKeyprovComponents() {
        runner.withBean(VmdKeyProvisioningService.class, () -> mock(VmdKeyProvisioningService.class))
                .withPropertyValues(validProperties())
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    assertEquals(1, context.getBeanNamesForType(KeyprovExecutor.class).length);
                    assertEquals(1, context.getBeanNamesForType(KeyprovAdmissionBulkhead.class).length);
                    assertEquals(1, context.getBeanNamesForType(KeyprovCircuitBreaker.class).length);
                    assertEquals(1, context.getBeanNamesForType(KeyprovStartupGuard.class).length);
                });
    }

    @Test
    void explicitDisabled_shouldNotCreateKeyprovComponents() {
        runner.withPropertyValues("vagw.keyprov.enabled=false")
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    assertEquals(0, context.getBeanNamesForType(KeyprovExecutor.class).length);
                    assertEquals(0, context.getBeanNamesForType(KeyprovAdmissionBulkhead.class).length);
                    assertEquals(0, context.getBeanNamesForType(KeyprovCircuitBreaker.class).length);
                    assertEquals(0, context.getBeanNamesForType(KeyprovStartupGuard.class).length);
                });
    }

    @Test
    void enabledWithoutOfficialFeignBean_shouldFailStartup() {
        runner.withPropertyValues(validProperties())
                .run(context -> assertNotNull(context.getStartupFailure(),
                        "缺少官方 VmdKeyProvisioningService Bean 必须 fail-fast"));
    }

    @Test
    void invalidCapacity_shouldFailStartup() {
        runner.withBean(VmdKeyProvisioningService.class, () -> mock(VmdKeyProvisioningService.class))
                .withPropertyValues("vagw.keyprov.max-in-flight=0", "vagw.keyprov.vmd.deadline=2s")
                .run(context -> assertNotNull(context.getStartupFailure(),
                        "非法容量配置必须 fail-fast"));
    }

    private static String causeChain(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            sb.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage()).append(" | ");
            current = current.getCause();
        }
        return sb.toString();
    }

    @Test
    void missingDeadline_shouldFailStartup() {
        runner.withBean(VmdKeyProvisioningService.class, () -> mock(VmdKeyProvisioningService.class))
                .withPropertyValues("vagw.keyprov.max-in-flight=2")
                .run(context -> assertNotNull(context.getStartupFailure(),
                        "缺少 vmd.deadline 必须 fail-fast"));
    }
}
