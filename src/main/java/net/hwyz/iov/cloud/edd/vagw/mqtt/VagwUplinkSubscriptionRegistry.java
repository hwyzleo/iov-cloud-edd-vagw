package net.hwyz.iov.cloud.edd.vagw.mqtt;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwMqttProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * VAGW 统一上行共享订阅注册表（EDD-VAGW-DSN-CR-015 §3）。
 * <p>
 * 单一职责：把配置中的原始业务 filter 规范化为 {@code $share/vagw/{filter}}，并在启动时按
 * 静态 allowlist fail-fast 校验，阻止 {@code $queue/}、非 {@code vagw} 共享组、按业务拆分的旧组
 * （如 {@code vagw-keyprov}）以及宽泛通配 filter（{@code vehicle/+/up/+}、{@code vehicle/+/up/#}）。
 * </p>
 * <p>
 * 只负责订阅目录与命名，不承载业务处理；入口 Adapter 仍由 MQTT 客户端按 topic segment 分发，
 * 各 service 保留独立 Adapter、协议门禁、容量与下游路由（US-015）。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VagwUplinkSubscriptionRegistry {

    /** 固定共享组名：部署单元级，禁止按业务派生。 */
    static final String REQUIRED_SHARED_GROUP = "vagw";

    /** service → topic segment 静态 allowlist。 */
    private static final Map<String, String> APPROVED_SERVICE_SEGMENTS = Map.of(
            "fota", "fota",
            "security.keyprov", "keyprov");

    /** 需要 capability 就绪（协议契约兼容）才可订阅的 service。 */
    private static final Set<String> CAPABILITY_GATED_SERVICES = Set.of("security.keyprov");

    private final VagwMqttProperties properties;
    private final KeyprovContractGuard keyprovContractGuard;

    /**
     * 启动自检（fail-fast）：共享组固定、filter/allowlist/QoS/重复性任一非法即终止启动。
     * 不提供 legacy 兼容开关；回退通过恢复上一版本及其完整配置集完成。
     */
    @PostConstruct
    void validate() {
        String group = properties.getSharedGroup();
        if (group == null || !REQUIRED_SHARED_GROUP.equals(group.trim())) {
            throw new IllegalStateException("vagw.mqtt.shared-group must be exactly '"
                    + REQUIRED_SHARED_GROUP + "' (got: " + group + ")");
        }

        List<VagwMqttProperties.Subscription> subscriptions = properties.getUplinkSubscriptions();
        if (subscriptions == null || subscriptions.isEmpty()) {
            throw new IllegalStateException("vagw.mqtt.uplink-subscriptions must not be empty");
        }

        Set<String> services = new HashSet<>();
        Set<String> filters = new HashSet<>();
        for (VagwMqttProperties.Subscription subscription : subscriptions) {
            String service = normalize(subscription.getService());
            String filter = normalize(subscription.getFilter());
            if (service == null) {
                throw new IllegalStateException("vagw.mqtt.uplink-subscriptions contains blank service");
            }
            if (filter == null) {
                throw new IllegalStateException(
                        "vagw.mqtt.uplink-subscriptions contains blank filter for service=" + service);
            }

            String segment = APPROVED_SERVICE_SEGMENTS.get(service);
            if (segment == null) {
                throw new IllegalStateException(
                        "vagw.mqtt.uplink-subscriptions contains service outside allowlist: " + service);
            }
            String approvedFilter = "vehicle/+/up/" + segment;
            if (!approvedFilter.equals(filter)) {
                throw new IllegalStateException("vagw.mqtt.uplink-subscriptions filter rejected for service="
                        + service + " (expected '" + approvedFilter + "', got '" + filter
                        + "'); $queue/, $share/ groups, broad wildcards and unknown services are forbidden");
            }
            if (subscription.getQos() != 1) {
                throw new IllegalStateException("vagw.mqtt.uplink-subscriptions qos must be 1 for service=" + service);
            }
            if (!services.add(service)) {
                throw new IllegalStateException(
                        "vagw.mqtt.uplink-subscriptions contains duplicate service: " + service);
            }
            if (!filters.add(filter)) {
                throw new IllegalStateException(
                        "vagw.mqtt.uplink-subscriptions contains duplicate filter: " + filter);
            }
        }

        log.info("VAGW uplink subscription registry validated: group={}, filters=[{}]",
                REQUIRED_SHARED_GROUP, String.join(", ", filters));
    }

    /**
     * 解析当前应生效的共享订阅集合：跳过 disabled 与 capability 未就绪的可选业务，
     * 统一生成 {@code $share/${shared-group}/${filter}}。
     */
    public List<ResolvedSubscription> resolveActive() {
        String group = properties.getSharedGroup().trim();
        List<ResolvedSubscription> resolved = new ArrayList<>();
        for (VagwMqttProperties.Subscription subscription : properties.getUplinkSubscriptions()) {
            String service = subscription.getService().trim();
            String filter = subscription.getFilter().trim();
            if (!subscription.isEnabled()) {
                log.info("VAGW uplink subscription disabled: group={}, service={}, filter={}, qos={}",
                        group, service, filter, subscription.getQos());
                continue;
            }
            if (CAPABILITY_GATED_SERVICES.contains(service) && !keyprovContractGuard.statusPathAvailable()) {
                log.warn("VAGW uplink subscription capability NOT_READY: group={}, service={}, filter={}, reason={}",
                        group, service, filter, keyprovContractGuard.incompatibilityReason());
                continue;
            }
            String segment = APPROVED_SERVICE_SEGMENTS.get(service);
            String sharedTopic = "$share/" + group + "/" + filter;
            resolved.add(new ResolvedSubscription(group, service, segment, filter, sharedTopic, subscription.getQos()));
            log.info("VAGW uplink subscription enabled: group={}, service={}, filter={}, qos={}",
                    group, service, filter, subscription.getQos());
        }
        return List.copyOf(resolved);
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * 已规范化的上行订阅：共享 Topic 由 registry 统一生成，Adapter 不得自行拼接 group 前缀。
     */
    public record ResolvedSubscription(
            String group,
            String service,
            String topicSegment,
            String filter,
            String sharedTopic,
            int qos) {
    }
}
