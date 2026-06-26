package net.hwyz.iov.cloud.edd.vagw.controller;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.model.dto.VehicleStatusResponse;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class VehicleStatusController {

    private final SessionService sessionService;

    @GetMapping("/v1/vehicles/{vin}/status")
    public ResponseEntity<VehicleStatusResponse> getStatus(@PathVariable String vin) {
        return sessionService.getSession(vin.toUpperCase())
                .map(session -> ResponseEntity.ok(VehicleStatusResponse.builder()
                        .online(session.isOnline())
                        .lastOnlineAt(session.getConnectedAt() != null ? session.getConnectedAt().toString() : null)
                        .lastOfflineAt(session.getDisconnectedAt() != null ? session.getDisconnectedAt().toString() : null)
                        .build()))
                .orElse(ResponseEntity.ok(VehicleStatusResponse.builder()
                        .online(false)
                        .build()));
    }
}
