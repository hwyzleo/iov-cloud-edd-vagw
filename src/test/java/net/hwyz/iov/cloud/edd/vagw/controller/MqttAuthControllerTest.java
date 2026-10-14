package net.hwyz.iov.cloud.edd.vagw.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.AdmissionSessionContextRegistry;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.service.AuthAclService;
import net.hwyz.iov.cloud.edd.vagw.service.AuthSecurityMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.AuthenticatedIdentityStore;
import net.hwyz.iov.cloud.edd.vagw.service.SuperuserBypassGuard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * MqttAuthController 单元测试（EDD-VAGW-DSN-CR-009 / EDD-VAGW-DSN-CR-013）。
 * <p>
 * 覆盖证书身份锚定、username==CN 一致性校验、叶子证书上下文透传与 VAGW bypass 行为。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class MqttAuthControllerTest {

    private static final String PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----";
    private static final String SERIAL = "35c7a12acb912925e2ef0a4a1f1666086c4633e8";

    @Mock
    private AuthAclService authAclService;
    @Mock
    private SuperuserBypassGuard bypassGuard;
    @Mock
    private AuthSecurityMetrics metrics;
    @Mock
    private AuthenticatedIdentityStore authenticatedIdentityStore;
    @Mock
    private AdmissionSessionContextRegistry admissionSessionContextRegistry;

    @InjectMocks
    private MqttAuthController controller;

    @BeforeEach
    void allowAdmissionContext() {
        AdmissionSessionContext context = new AdmissionSessionContext(
                "DEVICE-SN-001", "VIN001", SERIAL, "client001", "10.0.0.8", 4, 1L,
                Instant.now(), Instant.now().plusSeconds(300));
        lenient().when(admissionSessionContextRegistry
                .saveCandidate(any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.of(context));
    }

    private MqttAuthRequest deviceRequest(String username, String certCn) {
        return MqttAuthRequest.builder()
                .username(username)
                .clientId("client001")
                .peerCertCn(certCn)
                .peerCert(PEM)
                .peerHost("10.0.0.8")
                .protoVer(4)
                .build();
    }

    @Test
    void deserialize_authRequestWithExtraEmqxFields_shouldIgnoreUnknown() throws Exception {
        String json = "{\"username\":\"DEVICE-SN-001\",\"clientid\":\"client001\","
                + "\"peer_cert_cn\":\"DEVICE-SN-001\",\"peer_cert\":\"" + PEM.replace("\n", "\\n") + "\","
                + "\"peer_host\":\"10.0.0.8\",\"proto_ver\":5,\"receive_maximum\":32,\"keepalive\":60,"
                + "\"expiry_interval\":300,\"password\":null}";

        MqttAuthRequest request = new ObjectMapper().readValue(json, MqttAuthRequest.class);

        assertEquals("DEVICE-SN-001", request.getUsername());
        assertEquals("client001", request.getClientId());
        assertEquals("DEVICE-SN-001", request.getPeerCertCn());
        assertEquals("10.0.0.8", request.getPeerHost());
        assertEquals(5, request.getProtoVer());
    }

    @Test
    void authenticate_allowed_shouldReturnAllowAndPersistCertSerial() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("DEVICE-SN-001", "DEVICE-SN-001");

        List<MqttAuthResponse.AclRule> acl = List.of(
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("publish")
                        .topic("vehicle/DEVICE-SN-001/up/fota")
                        .build()
        );
        when(authAclService.authenticate("DEVICE-SN-001", "client001", PEM, "10.0.0.8"))
                .thenReturn(AuthAclService.AuthResult.allow(acl, "DEVICE-SN-001", "VIN001", SERIAL));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("allow", response.getBody().getResult());
        assertFalse(response.getBody().getIsSuperuser());
        assertEquals(1, response.getBody().getAcl().size());
        // canonical serial 由叶子证书解析后固化，供会话快照/上线事件沿用（CR-013 §4.2）
        verify(authenticatedIdentityStore).save("client001", "DEVICE-SN-001", SERIAL, null);
        verify(admissionSessionContextRegistry).saveCandidate(
                "DEVICE-SN-001", "VIN001", SERIAL, "client001", "10.0.0.8", 4);
    }

    @Test
    void authenticate_certPemMissing_shouldDenyWithoutTspCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);
        when(authAclService.authenticate("DEVICE-SN-001", "client001", null, "10.0.0.8"))
                .thenReturn(AuthAclService.AuthResult.deny(ErrorCode.AUTH_IDENTITY_MISMATCH, "PEM_MISSING"));

        MqttAuthRequest request = MqttAuthRequest.builder()
                .username("DEVICE-SN-001")
                .clientId("client001")
                .peerCertCn("DEVICE-SN-001")
                .peerCert(null)
                .peerHost("10.0.0.8")
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("PEM_MISSING"));
        verify(authenticatedIdentityStore, never()).save(any(), any(), any(), any());
        verify(admissionSessionContextRegistry, never()).saveCandidate(any(), any(), any(), any(), any(), any());
    }

    @Test
    void authenticate_denied_shouldReturnDenyWithReason() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("INVALID", "INVALID");

        when(authAclService.authenticate("INVALID", "client001", PEM, "10.0.0.8"))
                .thenReturn(AuthAclService.AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "UID_UNKNOWN"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals(200, response.getStatusCode().value());
        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804001"));
    }

    @Test
    void authenticate_peerCertCnMissing_shouldDenyWithoutServiceCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("DEVICE-SN-001", null);

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("PEER_CERT_CN_MISSING"));
        verify(authAclService, never()).authenticate(any(), any(), any(), any());
        verify(metrics).incPeerCertMissing();
    }

    @Test
    void authenticate_usernameMissing_shouldDenyWithoutServiceCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest(null, "DEVICE-SN-001");

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("USERNAME_MISSING"));
        verify(authAclService, never()).authenticate(any(), any(), any(), any());
    }

    @Test
    void authenticate_usernameMismatch_shouldDenyWithAudit() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        // 攻击场景：持合法证书（CN=DEVICE-SN-001）但冒充他人 username
        MqttAuthRequest request = deviceRequest("VICTIM-SN-999", "DEVICE-SN-001");

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("IDENTITY_MISMATCH"));
        verify(authAclService, never()).authenticate(any(), any(), any(), any());
        verify(metrics).incIdentityMismatch();
    }

    @Test
    void authenticate_invalidCertIdentityFormat_shouldDenyWithoutServiceCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("BAD@CN", "BAD@CN");

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("INVALID_CERT_IDENTITY"));
        verify(authAclService, never()).authenticate(any(), any(), any(), any());
        verify(metrics).incInvalidCertIdentity();
    }

    @Test
    void authenticate_caseInsensitiveMatch_shouldNormalizeAndForwardPem() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("DEVICE-SN-001", "device-sn-001");

        when(authAclService.authenticate("DEVICE-SN-001", "client001", PEM, "10.0.0.8"))
                .thenReturn(AuthAclService.AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "dummy"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        // 进入 service 层说明一致性校验通过，且以归一化 CN + 原始 PEM/peerhost 传参
        verify(authAclService).authenticate("DEVICE-SN-001", "client001", PEM, "10.0.0.8");
        assertEquals("deny", response.getBody().getResult());
    }

    @Test
    void authenticate_admissionContextUnavailable_shouldFailClosed() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);
        when(admissionSessionContextRegistry.saveCandidate(any(), any(), any(), any(), any(), any()))
                .thenReturn(Optional.empty());
        when(authAclService.authenticate("DEVICE-SN-001", "client001", PEM, "10.0.0.8"))
                .thenReturn(AuthAclService.AuthResult.allow(List.of(), "DEVICE-SN-001", "VIN001", SERIAL));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(
                deviceRequest("DEVICE-SN-001", "DEVICE-SN-001"));

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804010"));
        assertTrue(response.getBody().getReason().contains("ADMISSION_CONTEXT_UNAVAILABLE"));
    }

    @Test
    void authenticate_fakeClientIdPrefix_withoutBypass_shouldNotBeSuperuser() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = MqttAuthRequest.builder()
                .username("")
                .clientId("vehicle-access-gateway-evil")
                .peerCertCn(null)
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertNotNull(response.getBody());
        assertEquals("deny", response.getBody().getResult());
        assertFalse(response.getBody().getIsSuperuser());
        verify(authAclService, never()).authenticate(any(), any(), any(), any());
    }

    @Test
    void authenticate_bypassAllowed_shouldReturnSuperuserWithoutServiceCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(true);

        MqttAuthRequest request = MqttAuthRequest.builder()
                .clientId("vehicle-access-gateway" + "a1b2c3d4")
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("allow", response.getBody().getResult());
        assertTrue(response.getBody().getIsSuperuser());
        verify(authAclService, never()).authenticate(any(), any(), any(), any());
        verify(metrics).incBypassAllowed();
    }
}
