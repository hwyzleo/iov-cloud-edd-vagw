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

    private static final String VAGW_CLIENT_ID_PREFIX = "vehicle-access-gateway";

    private final AuthAclService authAclService;

    @PostMapping("/mqtt/auth")
    public ResponseEntity<MqttAuthResponse> authenticate(@RequestBody MqttAuthRequest request) {
        String clientId = request.getClientId();

        // VAGW自身连接，直接放行（超级用户权限）
        if (clientId != null && clientId.startsWith(VAGW_CLIENT_ID_PREFIX)) {
            log.info("VAGW self connection allowed: clientId={}", clientId);
            MqttAuthResponse response = MqttAuthResponse.builder()
                    .result("allow")
                    .isSuperuser(true)
                    .build();
            return ResponseEntity.ok(response);
        }

        // TBOX设备认证
        String deviceSn = request.getUsername();
        String certSerial = request.getPeerCertSerial();

        log.info("MQTT auth request: deviceSn={}, clientId={}, certSerial={}",
                deviceSn, clientId, certSerial);

        AuthAclService.AuthResult result = authAclService.authenticate(
                deviceSn, clientId, certSerial);

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
