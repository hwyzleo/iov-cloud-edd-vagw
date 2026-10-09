package net.hwyz.iov.cloud.edd.vagw.controller;

import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.service.AuthAclService;
import net.hwyz.iov.cloud.edd.vagw.service.AuthSecurityMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SuperuserBypassGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * MqttAuthController 单元测试（EDD-VAGW-DSN-CR-009 §7）
 * 覆盖证书身份锚定、username==CN 一致性校验的 fail-closed 与 VAGW bypass 行为。
 */
@ExtendWith(MockitoExtension.class)
class MqttAuthControllerTest {

    @Mock
    private AuthAclService authAclService;

    @Mock
    private SuperuserBypassGuard bypassGuard;

    @Mock
    private AuthSecurityMetrics metrics;

    @InjectMocks
    private MqttAuthController controller;

    private MqttAuthRequest deviceRequest(String username, String certCn) {
        return MqttAuthRequest.builder()
                .username(username)
                .clientId("client001")
                .peerCertCn(certCn)
                .peerCertSerial("CERT-SERIAL-001")
                .build();
    }

    @Test
    void authenticate_allowed_shouldReturnAllowWithAcl() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("DEVICE-SN-001", "DEVICE-SN-001");

        List<MqttAuthResponse.AclRule> acl = List.of(
                MqttAuthResponse.AclRule.builder()
                        .permission("allow")
                        .action("publish")
                        .topic("vehicle/DEVICE-SN-001/#")
                        .build()
        );
        when(authAclService.authenticate("DEVICE-SN-001", "client001", "CERT-SERIAL-001"))
                .thenReturn(AuthAclService.AuthResult.allow(acl, "DEVICE-SN-001", "VIN001"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("allow", response.getBody().getResult());
        assertFalse(response.getBody().getIsSuperuser());
        assertEquals(1, response.getBody().getAcl().size());
    }

    @Test
    void authenticate_denied_shouldReturnDenyWithReason() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("INVALID", "INVALID");

        when(authAclService.authenticate("INVALID", "client001", "CERT-SERIAL-001"))
                .thenReturn(AuthAclService.AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "Invalid device_sn"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals(200, response.getStatusCode().value());
        assertNotNull(response.getBody());
        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804001"));
    }

    @Test
    void authenticate_peerCertCnMissing_shouldDenyWithoutTspCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest("DEVICE-SN-001", null);

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("PEER_CERT_CN_MISSING"));
        verify(authAclService, never()).authenticate(any(), any(), any());
        verify(metrics).incPeerCertMissing();
    }

    @Test
    void authenticate_usernameMissing_shouldDenyWithoutTspCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        MqttAuthRequest request = deviceRequest(null, "DEVICE-SN-001");

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("USERNAME_MISSING"));
        verify(authAclService, never()).authenticate(any(), any(), any());
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
        verify(authAclService, never()).authenticate(any(), any(), any());
        verify(metrics).incIdentityMismatch();
    }

    @Test
    void authenticate_invalidCertIdentityFormat_shouldDenyWithoutTspCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        // 证书 CN 归一化后不匹配 ^[A-Z0-9-]{1,64}$ → 拒绝
        MqttAuthRequest request = deviceRequest("BAD@CN", "BAD@CN");

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("INVALID_CERT_IDENTITY"));
        verify(authAclService, never()).authenticate(any(), any(), any());
        verify(metrics).incInvalidCertIdentity();
    }

    @Test
    void authenticate_caseInsensitiveMatch_shouldNormalizeAndAllowPath() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        // 证书 CN 含小写，username 全大写 → 归一化后一致，进入 TSP 路径
        MqttAuthRequest request = deviceRequest("DEVICE-SN-001", "device-sn-001");

        when(authAclService.authenticate("DEVICE-SN-001", "client001", "CERT-SERIAL-001"))
                .thenReturn(AuthAclService.AuthResult.deny(ErrorCode.DEVICE_UNKNOWN, "dummy"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        // 已进入 service 层（由 TSP 结果决定 allow/deny），说明一致性校验通过且以归一化 CN 传参
        verify(authAclService).authenticate("DEVICE-SN-001", "client001", "CERT-SERIAL-001");
        assertEquals("deny", response.getBody().getResult());
    }

    @Test
    void authenticate_emqxPlaceholderCertSerial_shouldSanitizeToNullAndAudit() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        // EMQX 不支持的占位符 ${ssl_cert_serial_number} 原样透传 → 置空 + SEC-AUTH 审计，不阻断认证
        MqttAuthRequest request = MqttAuthRequest.builder()
                .username("DEVICE-SN-001")
                .clientId("client001")
                .peerCertCn("DEVICE-SN-001")
                .peerCertSerial("${ssl_cert_serial_number}")
                .build();

        when(authAclService.authenticate("DEVICE-SN-001", "client001", null))
                .thenReturn(AuthAclService.AuthResult.allow(List.of(), "DEVICE-SN-001", "VIN001"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("allow", response.getBody().getResult());
        verify(authAclService).authenticate("DEVICE-SN-001", "client001", null);
        verify(metrics).incPlaceholderLeak("peer_cert_serial");
    }

    @Test
    void authenticate_nullCertSerial_shouldPassThroughWithoutAudit() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);

        // certSerial 为空（EMQX body 已删除该字段）→ 正常透传 null，不触发占位符审计
        MqttAuthRequest request = MqttAuthRequest.builder()
                .username("DEVICE-SN-001")
                .clientId("client001")
                .peerCertCn("DEVICE-SN-001")
                .peerCertSerial(null)
                .build();

        when(authAclService.authenticate("DEVICE-SN-001", "client001", null))
                .thenReturn(AuthAclService.AuthResult.allow(List.of(), "DEVICE-SN-001", "VIN001"));

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("allow", response.getBody().getResult());
        verify(authAclService).authenticate("DEVICE-SN-001", "client001", null);
        verify(metrics, never()).incPlaceholderLeak(any());
    }

    @Test
    void authenticate_fakeClientIdPrefix_withoutBypass_shouldNotBeSuperuser() {
        // 伪造 VAGW 前缀的 clientId，但 bypass 守卫拒绝（如 peerhost 不在白名单）
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
        verify(authAclService, never()).authenticate(any(), any(), any());
    }

    @Test
    void authenticate_bypassAllowed_shouldReturnSuperuserWithoutTspCall() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(true);

        MqttAuthRequest request = MqttAuthRequest.builder()
                .clientId("vehicle-access-gateway" + "a1b2c3d4")
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("allow", response.getBody().getResult());
        assertTrue(response.getBody().getIsSuperuser());
        verify(authAclService, never()).authenticate(any(), any(), any());
        verify(metrics).incBypassAllowed();
    }
}
