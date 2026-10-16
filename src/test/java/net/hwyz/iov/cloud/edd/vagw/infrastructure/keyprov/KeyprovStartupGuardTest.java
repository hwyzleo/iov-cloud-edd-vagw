package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Keyprov 启动门禁测试（EDD-VAGW-DSN-CR-016 §4/§7-2）。
 */
class KeyprovStartupGuardTest {

    private static KeyprovProperties validProperties() {
        KeyprovProperties properties = new KeyprovProperties();
        properties.setMaxInFlight(4);
        properties.setQueueCapacity(0);
        properties.getVmd().setDeadline(Duration.ofSeconds(2));
        properties.getCircuitBreaker().setFailureWindow(Duration.ofSeconds(30));
        properties.getCircuitBreaker().setOpenDuration(Duration.ofSeconds(15));
        properties.getCircuitBreaker().setHalfOpenProbes(2);
        return properties;
    }

    @SuppressWarnings("unchecked")
    private static KeyprovStartupGuard guard(KeyprovProperties properties, boolean withOfficialBean) {
        ObjectProvider<VmdKeyProvisioningService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(
                withOfficialBean ? mock(VmdKeyProvisioningService.class) : null);
        return new KeyprovStartupGuard(properties, provider);
    }

    @Test
    void validConfiguration_shouldPass() {
        assertDoesNotThrow(() -> guard(validProperties(), true).validate());
    }

    @Test
    void missingOfficialFeignBean_shouldFailFast() {
        assertThrows(IllegalStateException.class, () -> guard(validProperties(), false).validate());
    }

    @Test
    void missingMaxInFlight_shouldFailFast() {
        KeyprovProperties properties = validProperties();
        properties.setMaxInFlight(0);
        assertThrows(IllegalStateException.class, () -> guard(properties, true).validate());
    }

    @Test
    void missingDeadline_shouldFailFast() {
        KeyprovProperties properties = validProperties();
        properties.getVmd().setDeadline(null);
        assertThrows(IllegalStateException.class, () -> guard(properties, true).validate());
    }

    @Test
    void negativeQueueCapacity_shouldFailFast() {
        KeyprovProperties properties = validProperties();
        properties.setQueueCapacity(-1);
        assertThrows(IllegalStateException.class, () -> guard(properties, true).validate());
    }

    @Test
    void missingCircuitBreakerWindow_shouldFailFast() {
        KeyprovProperties properties = validProperties();
        properties.getCircuitBreaker().setFailureWindow(null);
        assertThrows(IllegalStateException.class, () -> guard(properties, true).validate());
    }

    @Test
    void circuitBreakerDisabled_shouldSkipCircuitBreakerValidation() {
        KeyprovProperties properties = validProperties();
        properties.getCircuitBreaker().setEnabled(false);
        properties.getCircuitBreaker().setFailureWindow(null);
        properties.getCircuitBreaker().setOpenDuration(null);
        properties.getCircuitBreaker().setHalfOpenProbes(0);
        assertDoesNotThrow(() -> guard(properties, true).validate());
    }
}
