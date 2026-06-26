package net.hwyz.iov.cloud.edd.vagw.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DownlinkCommandRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DownlinkCommandResponse;
import net.hwyz.iov.cloud.edd.vagw.service.DownlinkService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequiredArgsConstructor
public class VehicleCommandController {

    private final DownlinkService downlinkService;

    @PostMapping("/v1/vehicles/{vin}/commands")
    public ResponseEntity<DownlinkCommandResponse> sendCommand(
            @PathVariable String vin,
            @Valid @RequestBody DownlinkCommandRequest request) {

        log.info("Downlink command request: vin={}, service={}, msgId={}",
                vin, request.getService(), request.getMsgId());

        DownlinkCommandResponse response = downlinkService.sendCommand(
                vin.toUpperCase(),
                request.getService(),
                request.getPayload(),
                request.getMsgId(),
                request.getTtl()
        );

        return ResponseEntity.ok(response);
    }
}
