package net.hwyz.iov.cloud.edd.vagw.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class MqttEventController {

    private final SessionService sessionService;
    private final BindingService bindingService;

    @PostMapping("/mqtt/events")
    public ResponseEntity<Void> handleEvent(@RequestBody MqttEventRequest request) {
        // 会话身份与认证结果一致（EDD-VAGW-DSN-CR-009 §5）：auth 层已强制 username==证书 CN，
        // 事件侧无 peer_cert_cn，仅依赖该已校验的 username。
        String deviceSn = request.getUsername();
        String event = request.getEvent();

        log.info("MQTT event: event={}, deviceSn={}, clientId={}", event, LogMask.mask(deviceSn), request.getClientId());

        if (deviceSn == null || deviceSn.isBlank()) {
            log.warn("Received event with blank device_sn, ignoring");
            return ResponseEntity.ok().build();
        }

        switch (event) {
            case "client.connected" -> {
                // fail-closed：仅对已准入/已绑定设备建立会话；
                // 未知、未认证或无法关联到已准入会话的连接事件不创建会话（需求 US-002 + CR-009 §5）
                if (!bindingService.isValidAndBound(deviceSn)) {
                    log.warn("Ignore connected event for unauthenticated/unbound device_sn={}", LogMask.mask(deviceSn));
                    return ResponseEntity.ok().build();
                }
                sessionService.onConnected(
                        deviceSn, request.getClientId(), request.getPeerHost(), request.getProtoVer());
            }
            case "client.disconnected" -> sessionService.onDisconnected(deviceSn);
            default -> log.warn("Unknown MQTT event: {}", event);
        }

        return ResponseEntity.ok().build();
    }
}
