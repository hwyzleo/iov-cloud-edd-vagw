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
        log.info("MQTT auth request: username={}, clientId={}", request.getUsername(), request.getClientId());

        String vin = request.getUsername();
        AuthAclService.AuthResult result = authAclService.authenticate(vin, request.getClientId());

        if (result.allowed()) {
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("allow")
                    .isSuperuser(false)
                    .acl(result.acl())
                    .build();
            return ResponseEntity.ok(response);
        } else {
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("deny")
                    .reason(result.errorCode().getCode() + ": " + result.errorCode().getMessage())
                    .build();
            return ResponseEntity.ok(response);
        }
    }
}
