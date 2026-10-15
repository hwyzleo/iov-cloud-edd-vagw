package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.VehicleMessageBridgeService;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.edd.vagw.mqtt.UplinkSubscriptionHandler;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import org.springframework.stereotype.Component;

/**
 * FOTA 上行入口 Adapter（EDD-VAGW-DSN-CR-006 §6 + EDD-VAGW-DSN-CR-015 §2/§4）。
 * <p>
 * 仅受理 {@code vehicle/{device-key}/up/fota}（{@code $share/vagw/vehicle/+/up/fota} 共享订阅）；
 * 其余 service segment 按 allowlist 拒绝，不形成任意 MQTT Topic 隧道。与 keyprov 入口 Adapter
 * 保持独立链路：并发池、in-flight、timeout、协议校验与下游路由互不合并（US-015）。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleMessageUplinkBridge implements UplinkSubscriptionHandler {

    /** 业务 service 名（metrics 标签）。 */
    public static final String SERVICE = "fota";
    /** MQTT Topic 中的 service segment。 */
    public static final String TOPIC_SEGMENT = VehicleRouteCatalog.FOTA_TOPIC_SEGMENT;

    private final VehicleMessageBridgeService bridgeService;
    private final VagwMqttSubscriptionMetrics metrics;

    @Override
    public String service() {
        return SERVICE;
    }

    @Override
    public String topicSegment() {
        return TOPIC_SEGMENT;
    }

    /**
     * 处理 MQTT 上行消息。
     *
     * @param topic   原始 MQTT Topic（vehicle/{device-key}/up/{segment}）
     * @param payload Envelope bytes
     */
    @Override
    public void handleUplink(String topic, byte[] payload) {
        String[] parts = topic.split("/");
        if (parts.length < 4 || !"vehicle".equals(parts[0]) || !"up".equals(parts[2])) {
            metrics.uplinkRejected(SERVICE, "invalid_topic_format");
            log.warn("Invalid uplink topic format, rejected");
            return;
        }
        String deviceKey = parts[1];
        String segment = parts[3];

        // allowlist：仅 fota 方向可桥接；vehicle.ota / vehicle.ota.v1 / 未知 service 拒绝
        if (!VehicleRouteCatalog.FOTA_TOPIC_SEGMENT.equals(segment)) {
            metrics.uplinkRejected(SERVICE, "service_not_allowlisted");
            log.warn("FOTA uplink service segment rejected (allowlist): deviceKey={}",
                    LogMask.mask(deviceKey));
            return;
        }

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(payload, deviceKey);
        if (!result.ok()) {
            metrics.uplinkRejected(SERVICE, result.reason());
            log.warn("FOTA uplink processing failed: deviceKey={}, reason={}",
                    LogMask.mask(deviceKey), result.reason());
        }
    }
}
