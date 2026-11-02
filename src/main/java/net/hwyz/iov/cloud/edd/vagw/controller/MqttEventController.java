package net.hwyz.iov.cloud.edd.vagw.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.MqttConnectionEventHandler;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import net.hwyz.iov.cloud.edd.vagw.service.VagwSelfConnectionRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * EMQX 会话事件 Hook（EDD-VAGW-DSN-CR-012 §3.2）。
 * <p>
 * {@code client.connected} 委派 {@link MqttConnectionEventHandler}：仅关联到本次已成功准入的
 * 会话快照时建立可信会话并固化 DEVICE_ONLINE 待发布事件；无法关联即 fail-closed 并断连。
 * {@code client.disconnected} 仅更新会话状态，不发布 DEVICE_ONLINE。
 * </p>
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class MqttEventController {

    private final SessionService sessionService;
    private final MqttConnectionEventHandler deviceOnlineHandler;
    private final VagwSelfConnectionRegistry vagwSelfConnectionRegistry;

    @PostMapping("/mqtt/events")
    public ResponseEntity<Void> handleEvent(@RequestBody MqttEventRequest request) {
        String deviceSn = request.getUsername();
        String event = request.getEvent();

        log.info("MQTT event: event={}, deviceSn={}, clientId={}",
                event, LogMask.mask(deviceSn), request.getClientId());

        if (deviceSn == null || deviceSn.isBlank()) {
            log.warn("Received event with blank device_sn, ignoring");
            return ResponseEntity.ok().build();
        }

        switch (event) {
            case "client.connected" -> deviceOnlineHandler.handleConnected(request);
            case "client.disconnected" -> {
                // VAGW 自身受信服务连接（superuser bypass）非设备：不更新设备会话，避免误标服务账号下线
                if (!vagwSelfConnectionRegistry.isSelfConnection(request.getClientId())) {
                    sessionService.onDisconnected(deviceSn);
                }
            }
            default -> log.warn("Unknown MQTT event: {}", event);
        }

        return ResponseEntity.ok().build();
    }
}
