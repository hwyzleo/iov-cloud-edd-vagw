package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.VehicleMessageBridgeService;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import org.springframework.stereotype.Component;

/**
 * 车辆消息上行桥（EDD-VAGW-DSN-CR-006 §6）。
 * <p>
 * 仅受理 vehicle/{device-key}/up/fota；其余 service segment 按 allowlist 拒绝，不形成任意
 * MQTT Topic 隧道。Topic device key 作为接入身份（EMQX ACL 约束）传给桥接服务。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleMessageUplinkBridge {

    private final VehicleMessageBridgeService bridgeService;

    /**
     * 处理 MQTT 上行消息。
     *
     * @param topic   原始 MQTT Topic（vehicle/{device-key}/up/{segment}）
     * @param payload Envelope bytes
     */
    public void handleUplink(String topic, byte[] payload) {
        String[] parts = topic.split("/");
        if (parts.length < 4 || !"vehicle".equals(parts[0]) || !"up".equals(parts[2])) {
            log.warn("Invalid uplink topic format, rejected: topic={}", topic);
            return;
        }
        String deviceKey = parts[1];
        String segment = parts[3];

        // allowlist：仅 fota 方向可桥接；vehicle.ota / vehicle.ota.v1 / 未知 service 拒绝
        if (!VehicleRouteCatalog.FOTA_TOPIC_SEGMENT.equals(segment)) {
            log.warn("FOTA uplink service segment rejected (allowlist): segment={}, deviceKey={}",
                    segment, LogMask.mask(deviceKey));
            return;
        }

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(payload, deviceKey);
        if (!result.ok()) {
            log.warn("FOTA uplink processing failed: deviceKey={}, reason={}",
                    LogMask.mask(deviceKey), result.reason());
        }
    }
}
