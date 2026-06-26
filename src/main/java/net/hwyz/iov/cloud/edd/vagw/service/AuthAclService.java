package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthAclService {

    private static final Pattern VIN_PATTERN = Pattern.compile("^[A-HJ-NPR-Z0-9]{17}$");

    public AuthResult authenticate(String vin, String clientId) {
        if (vin == null || vin.isBlank()) {
            log.warn("Auth failed: empty VIN");
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Missing VIN");
        }

        String normalizedVin = vin.toUpperCase();
        if (!VIN_PATTERN.matcher(normalizedVin).matches()) {
            log.warn("Auth failed: invalid VIN format: {}", vin);
            return AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Invalid VIN format");
        }

        // TODO: Check VIN against VMD service for validity/status
        // For now, accept all valid-format VINs
        log.info("Auth success: vin={}", normalizedVin);

        List<MqttAuthResponse.AclRule> acl = buildAcl(normalizedVin);
        return AuthResult.allow(acl);
    }

    private List<MqttAuthResponse.AclRule> buildAcl(String vin) {
        String topicNamespace = "vehicle/" + vin + "/#";
        return List.of(
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("publish")
                        .topic(topicNamespace)
                        .build(),
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("subscribe")
                        .topic(topicNamespace)
                        .build()
        );
    }

    public record AuthResult(boolean allowed, ErrorCode errorCode, String reason,
                              List<MqttAuthResponse.AclRule> acl) {
        public static AuthResult allow(List<MqttAuthResponse.AclRule> acl) {
            return new AuthResult(true, null, null, acl);
        }

        public static AuthResult deny(ErrorCode code, String reason) {
            return new AuthResult(false, code, reason, null);
        }
    }
}
