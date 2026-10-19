package net.hwyz.iov.cloud.edd.vagw.mqtt;

import net.hwyz.iov.cloud.edd.vagw.config.VagwMqttProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * 统一上行共享订阅注册表测试（EDD-VAGW-DSN-CR-015 §3/§5/§7）。
 */
@ExtendWith(MockitoExtension.class)
class VagwUplinkSubscriptionRegistryTest {

    @Mock
    private KeyprovContractGuard contractGuard;

    private VagwMqttProperties properties;

    @BeforeEach
    void setUp() {
        properties = new VagwMqttProperties();
    }

    private VagwUplinkSubscriptionRegistry registry() {
        return new VagwUplinkSubscriptionRegistry(properties, contractGuard);
    }

    private static VagwMqttProperties.Subscription sub(String service, String filter, int qos, boolean enabled) {
        return new VagwMqttProperties.Subscription(service, filter, qos, enabled);
    }

    // ---------- validate ----------

    @Test
    void defaultConfig_shouldValidate() {
        registry().validate();
    }

    @Test
    void nonVagwSharedGroup_shouldFailFast() {
        properties.setSharedGroup("vagw-keyprov");
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void queueCompatibleFilter_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "$queue/vehicle/+/up/fota", 1, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void legacyKeyprovGroupFilter_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("security.keyprov", "$share/vagw-keyprov/vehicle/+/up/keyprov", 1, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void broadWildcardFilter_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/+", 1, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void hashWildcardFilter_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/#", 1, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void unknownService_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("remotecontrol", "vehicle/+/up/remotecontrol", 1, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void duplicateService_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/fota", 1, true),
                sub("fota", "vehicle/+/up/fota", 1, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void nonQos1_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/fota", 0, true)));
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    @Test
    void emptySubscriptions_shouldFailFast() {
        properties.setUplinkSubscriptions(List.of());
        assertThrows(IllegalStateException.class, () -> registry().validate());
    }

    // ---------- resolve ----------

    @Test
    void resolveActive_default_shouldReturnSharedVagwFotaOnly() {
        List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> resolved = registry().resolveActive();

        assertEquals(1, resolved.size());
        VagwUplinkSubscriptionRegistry.ResolvedSubscription fota = resolved.get(0);
        assertEquals("vagw", fota.group());
        assertEquals("fota", fota.service());
        assertEquals("vehicle/+/up/fota", fota.filter());
        assertEquals("$share/vagw/vehicle/+/up/fota", fota.sharedTopic());
        assertEquals(1, fota.qos());
    }

    @Test
    void resolveActive_keyprovEnabledAndCompatible_shouldReturnBothFilters() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/fota", 1, true),
                sub("security.keyprov", "vehicle/+/up/keyprov", 1, true)));
        when(contractGuard.statusPathAvailable()).thenReturn(true);

        List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> resolved = registry().resolveActive();

        assertEquals(2, resolved.size());
        assertTrue(resolved.stream().anyMatch(s -> "$share/vagw/vehicle/+/up/fota".equals(s.sharedTopic())));
        assertTrue(resolved.stream().anyMatch(s -> "$share/vagw/vehicle/+/up/keyprov".equals(s.sharedTopic())));
    }

    @Test
    void resolveActive_keyprovEnabledButContractIncompatible_shouldExcludeKeyprovKeepFota() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/fota", 1, true),
                sub("security.keyprov", "vehicle/+/up/keyprov", 1, true)));
        when(contractGuard.statusPathAvailable()).thenReturn(false);

        List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> resolved = registry().resolveActive();

        assertEquals(1, resolved.size());
        assertEquals("fota", resolved.get(0).service());
    }

    @Test
    void resolveActive_keyprovDisabled_shouldNotSubscribeKeyprov() {
        properties.setUplinkSubscriptions(List.of(
                sub("fota", "vehicle/+/up/fota", 1, true),
                sub("security.keyprov", "vehicle/+/up/keyprov", 1, false)));

        List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> resolved = registry().resolveActive();

        assertEquals(1, resolved.size());
        assertEquals("fota", resolved.get(0).service());
    }
}
