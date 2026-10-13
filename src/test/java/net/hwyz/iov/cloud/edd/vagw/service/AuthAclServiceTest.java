package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAdmissionProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.security.PeerCertificateContextFactory;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.iov.tsp.api.service.TspDeviceAdmissionService;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionCheckVo;
import net.hwyz.iov.cloud.iov.tsp.api.vo.DeviceAdmissionResultVo;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.AdmissionDecision;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.AdmissionReason;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.BindStatus;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.CertificateStatus;
import net.hwyz.iov.cloud.iov.tsp.api.vo.enums.DeviceStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * AuthAclService 契约测试（EDD-VAGW-DSN-CR-013 §8）。
 * <p>
 * 与 {@link AuthAclServiceImplTest} 互补：聚焦服务契约边界（ACL/绑定/错误码）而非证书矩阵。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class AuthAclServiceTest {

    @Mock
    private TspDeviceAdmissionService tspDeviceAdmissionService;
    @Mock
    private BindingService bindingService;
    @Mock
    private KeyprovProperties keyprovProperties;
    @Mock
    private KeyprovContractGuard keyprovContractGuard;
    @Mock
    private AuthSecurityMetrics metrics;

    private AuthAclServiceImpl authAclService;
    private String validPem;

    @BeforeEach
    void setUp() {
        VagwAdmissionProperties admissionProperties = new VagwAdmissionProperties();
        PeerCertificateContextFactory factory = new PeerCertificateContextFactory(admissionProperties);
        authAclService = new AuthAclServiceImpl(tspDeviceAdmissionService, bindingService, keyprovProperties,
                keyprovContractGuard, factory, admissionProperties, metrics);
        validPem = resource("certs/leaf-valid.pem");
    }

    private DeviceAdmissionResultVo allowResult() {
        return DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.ALLOW)
                .reason(AdmissionReason.NONE)
                .vin("LSGJA52U7YA000001")
                .bindStatus(BindStatus.BOUND)
                .certStatus(CertificateStatus.VALID)
                .deviceStatus(DeviceStatus.ACTIVATING)
                .bindVersion(3L)
                .updatedAt(Instant.parse("2026-10-12T00:00:00Z"))
                .build();
    }

    @Test
    void authenticate_validDevice_shouldAllowAndRememberBinding() {
        when(tspDeviceAdmissionService.decide(any(DeviceAdmissionCheckVo.class))).thenReturn(allowResult());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertTrue(result.allowed());
        assertNotNull(result.acl());
        assertEquals(6, result.acl().size());
        assertEquals("DEVICE-001", result.deviceSn());
        assertEquals("LSGJA52U7YA000001", result.vin());
        assertTrue(result.acl().get(0).getTopic().contains("DEVICE-001"));
        assertTrue(result.acl().stream().noneMatch(r -> r.getTopic().contains("keyprov")));
        verify(bindingService).rememberAdmission("DEVICE-001", "LSGJA52U7YA000001", 3L);
    }

    @Test
    void authenticate_unboundDevice_shouldDenyWith804001() {
        when(tspDeviceAdmissionService.decide(any(DeviceAdmissionCheckVo.class)))
                .thenReturn(DeviceAdmissionResultVo.builder()
                        .decision(AdmissionDecision.DENY)
                        .reason(AdmissionReason.UNBOUND)
                        .build());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_certRevoked_shouldDenyWith804002() {
        when(tspDeviceAdmissionService.decide(any(DeviceAdmissionCheckVo.class)))
                .thenReturn(DeviceAdmissionResultVo.builder()
                        .decision(AdmissionDecision.DENY)
                        .reason(AdmissionReason.CERT_REVOKED)
                        .build());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
    }

    @Test
    void authenticate_tspError_shouldDenyWith804002() {
        when(tspDeviceAdmissionService.decide(any(DeviceAdmissionCheckVo.class)))
                .thenThrow(new RuntimeException("Service unavailable"));

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
    }

    @Test
    void authenticate_emptyDeviceSn_shouldDeny() {
        assertFalse(authAclService.authenticate("", "client001", validPem, "10.0.0.8").allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN,
                authAclService.authenticate("", "client001", validPem, "10.0.0.8").errorCode());
    }

    @Test
    void authenticate_invalidFormat_shouldDeny() {
        AuthAclService.AuthResult result = authAclService.authenticate(
                "invalid@sn", "client001", validPem, "10.0.0.8");
        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode());
    }

    @Test
    void authenticate_lowercaseDeviceSn_shouldNormalize() {
        when(tspDeviceAdmissionService.decide(any(DeviceAdmissionCheckVo.class))).thenReturn(allowResult());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "device-001", "client001", validPem, "10.0.0.8");

        assertTrue(result.allowed());
        assertTrue(result.acl().get(0).getTopic().contains("DEVICE-001"));
    }

    private static String resource(String path) {
        try (InputStream in = AuthAclServiceTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in, "missing test resource: " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
