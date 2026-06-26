package net.hwyz.iov.cloud.edd.vagw.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
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

    @PostMapping("/mqtt/events")
    public ResponseEntity<Void> handleEvent(@RequestBody MqttEventRequest request) {
        String vin = request.getUsername();
        String event = request.getEvent();

        log.info("MQTT event: event={}, vin={}, clientId={}", event, vin, request.getClientId());

        if (vin == null || vin.isBlank()) {
            log.warn("Received event with blank VIN, ignoring");
            return ResponseEntity.ok().build();
        }

        switch (event) {
            case "client.connected" -> sessionService.onConnected(
                    vin, request.getClientId(), request.getPeerHost(), request.getProtoVer());
            case "client.disconnected" -> sessionService.onDisconnected(vin);
            default -> log.warn("Unknown MQTT event: {}", event);
        }

        return ResponseEntity.ok().build();
    }
}
