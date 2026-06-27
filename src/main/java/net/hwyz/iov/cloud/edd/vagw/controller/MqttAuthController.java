package net.hwyz.iov.cloud.edd.vagw.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.service.AuthAclService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class MqttAuthController {

    private final AuthAclService authAclService;

    @PostMapping("/mqtt/auth")
    public ResponseEntity<MqttAuthResponse> authenticate(@RequestBody MqttAuthRequest request) {
        // username = CN(device_sn), clientid = device_sn
        String deviceSn = request.getUsername();
        log.info("MQTT auth request: deviceSn={}, clientId={}", deviceSn, request.getClientId());

        AuthAclService.AuthResult result = authAclService.authenticate(deviceSn, request.getClientId());

        if (result.allowed()) {
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("allow")
                    .isSuperuser(false)
                    .acl(result.acl())
                    .build();
            log.info("Auth allowed: deviceSn={}, vin={}", result.deviceSn(), result.vin());
            return ResponseEntity.ok(response);
        } else {
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("deny")
                    .reason(result.errorCode().getCode() + ": " + result.errorCode().getMessage())
                    .build();
            log.warn("Auth denied: deviceSn={}, reason={}", deviceSn, result.reason());
            return ResponseEntity.ok(response);
        }
    }
}
