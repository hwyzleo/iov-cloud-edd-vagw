package net.hwyz.iov.cloud.edd.vagw.controller;

import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.service.AuthAclService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttAuthControllerTest {

    @Mock
    private AuthAclService authAclService;

    @InjectMocks
    private MqttAuthController controller;

    @Test
    void authenticate_allowed_shouldReturnAllowWithAcl() {
        MqttAuthRequest request = new MqttAuthRequest();
        request.setUsername("VIN001");
        request.setClientId("client001");

        List<MqttAuthResponse.AclRule> acl = List.of(
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("publish")
                        .topic("vehicle/VIN001/#")
                        .build()
        );
        when(authAclService.authenticate("VIN001", "client001"))
                .thenReturn(AuthAclService.AuthResult.allow(acl));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("allow", response.getBody().getResult());
        assertFalse(response.getBody().getIsSuperuser());
        assertEquals(1, response.getBody().getAcl().size());
    }

    @Test
    void authenticate_denied_shouldReturnDenyWithReason() {
        MqttAuthRequest request = new MqttAuthRequest();
        request.setUsername("INVALID");
        request.setClientId("client001");

        when(authAclService.authenticate("INVALID", "client001"))
                .thenReturn(AuthAclService.AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Invalid VIN"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("Invalid VIN"));
    }
}
