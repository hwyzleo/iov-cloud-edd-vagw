package net.hwyz.iov.cloud.edd.vagw.mqtt;

import net.hwyz.iov.cloud.edd.vagw.config.VagwMqttProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 配置 → 注册表集成测试（EDD-VAGW-DSN-CR-015 §3/§5/§7）。
 * <p>
 * 验证 {@code vagw.mqtt.*} 真实属性绑定后，启动自检与统一 {@code $share/vagw/...} 生成一致；
 * 旧前缀或宽泛 filter 会导致上下文启动失败（fail-fast）。
 * </p>
 */
class VagwUplinkSubscriptionIntegrationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RegistryTestConfig.class);

    @Configuration
    @EnableConfigurationProperties(VagwMqttProperties.class)
    static class RegistryTestConfig {
        @Bean
        KeyprovContractGuard keyprovContractGuard() {
            KeyprovContractGuard guard = new KeyprovContractGuard();
            guard.verify();
            return guard;
        }

        @Bean
        VagwUplinkSubscriptionRegistry registry(VagwMqttProperties properties, KeyprovContractGuard guard) {
            return new VagwUplinkSubscriptionRegistry(properties, guard);
        }
    }

    @Test
    void defaultConfig_shouldResolveFotaOnlyUnderUnifiedGroup() {
        runner.run(context -> {
            VagwUplinkSubscriptionRegistry registry = context.getBean(VagwUplinkSubscriptionRegistry.class);
            List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> resolved = registry.resolveActive();

            assertEquals(1, resolved.size());
            assertEquals("vagw", resolved.get(0).group());
            assertEquals("$share/vagw/vehicle/+/up/fota", resolved.get(0).sharedTopic());
        });
    }

    @Test
    void keyprovEnabledAndContractCompatible_shouldResolveBothFilters() {
        runner.withPropertyValues(
                        "vagw.mqtt.uplink-subscriptions[0].service=fota",
                        "vagw.mqtt.uplink-subscriptions[0].filter=vehicle/+/up/fota",
                        "vagw.mqtt.uplink-subscriptions[0].qos=1",
                        "vagw.mqtt.uplink-subscriptions[0].enabled=true",
                        "vagw.mqtt.uplink-subscriptions[1].service=security.keyprov",
                        "vagw.mqtt.uplink-subscriptions[1].filter=vehicle/+/up/keyprov",
                        "vagw.mqtt.uplink-subscriptions[1].qos=1",
                        "vagw.mqtt.uplink-subscriptions[1].enabled=true")
                .run(context -> {
                    VagwUplinkSubscriptionRegistry registry = context.getBean(VagwUplinkSubscriptionRegistry.class);
                    List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> resolved = registry.resolveActive();

                    assertEquals(2, resolved.size());
                    assertEquals("$share/vagw/vehicle/+/up/fota", resolved.get(0).sharedTopic());
                    assertEquals("$share/vagw/vehicle/+/up/keyprov", resolved.get(1).sharedTopic());
                });
    }

    @Test
    void legacyQueueFilter_shouldFailContextStartup() {
        runner.withPropertyValues("vagw.mqtt.uplink-subscriptions[0].filter=$queue/vehicle/+/up/fota")
                .run(context -> assertNotNull(context.getStartupFailure()));
    }

    @Test
    void broadWildcardFilter_shouldFailContextStartup() {
        runner.withPropertyValues("vagw.mqtt.uplink-subscriptions[0].filter=vehicle/+/up/+")
                .run(context -> assertNotNull(context.getStartupFailure()));
    }
}
