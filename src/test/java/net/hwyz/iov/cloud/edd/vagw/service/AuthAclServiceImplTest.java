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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * AuthAclServiceImpl 单元测试（EDD-VAGW-DSN-CR-013 §8）。
 * <p>
 * 真实 {@link PeerCertificateContextFactory} + 真实 PEM 夹具，覆盖证书上下文构造与 TSP 裁决映射。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class AuthAclServiceImplTest {

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

    private VagwAdmissionProperties admissionProperties;
    private AuthAclServiceImpl authAclService;
    private String validPem;

    @BeforeEach
    void setUp() {
        admissionProperties = new VagwAdmissionProperties();
        PeerCertificateContextFactory factory = new PeerCertificateContextFactory(admissionProperties);
        authAclService = new AuthAclServiceImpl(tspDeviceAdmissionService, bindingService, keyprovProperties,
                keyprovContractGuard, factory, admissionProperties, metrics);
        validPem = resource("certs/leaf-valid.pem");
    }

    private DeviceAdmissionResultVo allowResult() {
        return DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.ALLOW)
                .reason(AdmissionReason.NONE)
                .vin("VIN001")
                .bindStatus(BindStatus.BOUND)
                .certStatus(CertificateStatus.VALID)
                .deviceStatus(DeviceStatus.ACTIVE)
                .bindVersion(7L)
                .updatedAt(Instant.parse("2026-10-12T00:00:00Z"))
                .build();
    }

    private DeviceAdmissionResultVo denyResult(AdmissionReason reason) {
        return DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.DENY)
                .reason(reason)
                .build();
    }

    @Test
    void authenticate_allowWithCompleteFields_shouldAllowAndForwardCertificateContext() throws Exception {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(allowResult());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertTrue(result.allowed());
        assertEquals("DEVICE-001", result.deviceSn());
        assertEquals("VIN001", result.vin());
        assertEquals("35c7a12acb912925e2ef0a4a1f1666086c4633e8", result.certSerial());
        // 方向敏感显式白名单：3 个 service × (publish+subscribe) = 6；keyprov 默认关闭不授予
        assertEquals(6, result.acl().size());
        assertTrue(result.acl().stream().noneMatch(r -> r.getTopic().contains("keyprov")));
        verify(bindingService).rememberAdmission("DEVICE-001", "VIN001", 7L);

        ArgumentCaptor<DeviceAdmissionCheckVo> captor = ArgumentCaptor.forClass(DeviceAdmissionCheckVo.class);
        verify(tspDeviceAdmissionService).decide(captor.capture());
        DeviceAdmissionCheckVo vo = captor.getValue();
        assertEquals("DEVICE-001", vo.getUid());
        assertEquals("35c7a12acb912925e2ef0a4a1f1666086c4633e8", vo.getCertSerial());
        assertTrue(vo.getCertFingerprint().matches("[0-9a-f]{64}"));
        // leafCertificateDer 为标准 Base64 DER：可解析为 X.509 且 SHA-256 与 certFingerprint 一致
        byte[] forwardedDer = Base64.getDecoder().decode(vo.getLeafCertificateDer());
        X509Certificate forwarded = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(forwardedDer));
        assertEquals(vo.getCertSerial(), new BigInteger(1, forwarded.getSerialNumber().toByteArray()).toString(16));
        assertEquals(vo.getCertFingerprint(), sha256Hex(forwardedDer));
        assertEquals("client001", vo.getClientId());
        assertEquals("10.0.0.8", vo.getSourceIp());
        assertNotNull(vo.getTs());
    }

    @Test
    void authenticate_allowWithEmqxBase64Der_shouldForwardStandardBase64Der() throws Exception {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(allowResult());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", base64Der(validPem), "10.0.0.8");

        assertTrue(result.allowed());
        assertEquals("35c7a12acb912925e2ef0a4a1f1666086c4633e8", result.certSerial());

        ArgumentCaptor<DeviceAdmissionCheckVo> captor = ArgumentCaptor.forClass(DeviceAdmissionCheckVo.class);
        verify(tspDeviceAdmissionService).decide(captor.capture());
        DeviceAdmissionCheckVo vo = captor.getValue();
        byte[] forwardedDer = Base64.getDecoder().decode(vo.getLeafCertificateDer());
        assertArrayEquals(derBytes(validPem), forwardedDer);
        assertEquals(vo.getCertFingerprint(), sha256Hex(forwardedDer));
    }

    @Test
    void authenticate_placeholderLeak_shouldDenyWithoutTspCall() {
        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", "${cert_pem}", "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.AUTH_IDENTITY_MISMATCH, result.errorCode());
        assertEquals("PEM_PLACEHOLDER_LEAK", result.reason());
        verifyNoInteractions(tspDeviceAdmissionService);
        verify(metrics).incPeerCertificateValidation("PEM_PLACEHOLDER_LEAK");
    }

    @Test
    void authenticate_keyprovEnabledAndCompatible_shouldGrantKeyprovAcl() {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(allowResult());
        when(keyprovProperties.isEnabled()).thenReturn(true);
        when(keyprovContractGuard.statusPathAvailable()).thenReturn(true);

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertTrue(result.allowed());
        assertEquals(8, result.acl().size());
        assertTrue(result.acl().stream().anyMatch(r ->
                "publish".equals(r.getAction()) && r.getTopic().equals("vehicle/DEVICE-001/up/keyprov")));
    }

    @Test
    void authenticate_keyprovEnabledButIncompatible_shouldNotGrantKeyprovAcl() {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(allowResult());
        when(keyprovProperties.isEnabled()).thenReturn(true);
        when(keyprovContractGuard.statusPathAvailable()).thenReturn(false);

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertTrue(result.allowed());
        assertEquals(6, result.acl().size());
        assertTrue(result.acl().stream().noneMatch(r -> r.getTopic().contains("keyprov")));
    }

    @Test
    void authenticate_missingPeerCert_shouldDenyWithoutTspCall() {
        AuthAclService.AuthResult result = authAclService.authenticate("DEVICE-001", "client001", null, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.AUTH_IDENTITY_MISMATCH, result.errorCode());
        assertEquals("PEM_MISSING", result.reason());
        verifyNoInteractions(tspDeviceAdmissionService);
        verify(metrics).incPeerCertificateValidation("PEM_MISSING");
    }

    @Test
    void authenticate_cnMismatch_shouldDenyWithoutTspCall() {
        AuthAclService.AuthResult result = authAclService.authenticate(
                "OTHER-DEVICE", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.AUTH_IDENTITY_MISMATCH, result.errorCode());
        assertEquals("CN_MISMATCH", result.reason());
        verifyNoInteractions(tspDeviceAdmissionService);
    }

    @Test
    void authenticate_allowButIncompleteFields_shouldDenyAsDependencyUnavailable() {
        DeviceAdmissionResultVo incomplete = DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.ALLOW)
                .reason(AdmissionReason.NONE)
                .vin(null)
                .bindStatus(BindStatus.BOUND)
                .certStatus(CertificateStatus.VALID)
                .deviceStatus(DeviceStatus.ACTIVE)
                .bindVersion(7L)
                .updatedAt(Instant.now())
                .build();
        when(tspDeviceAdmissionService.decide(any())).thenReturn(incomplete);

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
        assertTrue(result.reason().contains("INCOMPLETE_ADMISSION_ALLOW"));
        verify(bindingService, never()).rememberAdmission(any(), any(), any());
    }

    @Test
    void authenticate_allowButCertStatusUnknown_shouldDeny() {
        DeviceAdmissionResultVo incomplete = allowResult();
        incomplete.setCertStatus(CertificateStatus.UNKNOWN);
        when(tspDeviceAdmissionService.decide(any())).thenReturn(incomplete);

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
    }

    @Test
    void authenticate_uidUnknownOrUnboundOrConflict_shouldMapToDeviceUnknown() {
        for (AdmissionReason reason : new AdmissionReason[]{
                AdmissionReason.UID_UNKNOWN, AdmissionReason.UNBOUND, AdmissionReason.BINDING_CONFLICT}) {
            when(tspDeviceAdmissionService.decide(any())).thenReturn(denyResult(reason));

            AuthAclService.AuthResult result = authAclService.authenticate(
                    "DEVICE-001", "client001", validPem, "10.0.0.8");

            assertFalse(result.allowed(), reason + " should deny");
            assertEquals(ErrorCode.DEVICE_UNKNOWN, result.errorCode(), reason + " → 804001");
        }
    }

    @Test
    void authenticate_identityMismatch_shouldMapTo804016() {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(denyResult(AdmissionReason.IDENTITY_MISMATCH));

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.AUTH_IDENTITY_MISMATCH, result.errorCode(), "IDENTITY_MISMATCH → 804016");
    }

    @Test
    void authenticate_revokedExpiredBlockedRetiredOrDependency_shouldMapToDeviceBlocked() {
        for (AdmissionReason reason : new AdmissionReason[]{
                AdmissionReason.CERT_REVOKED, AdmissionReason.CERT_EXPIRED,
                AdmissionReason.DEVICE_BLOCKED, AdmissionReason.DEVICE_RETIRED,
                AdmissionReason.DEPENDENCY_UNAVAILABLE}) {
            when(tspDeviceAdmissionService.decide(any())).thenReturn(denyResult(reason));

            AuthAclService.AuthResult result = authAclService.authenticate(
                    "DEVICE-001", "client001", validPem, "10.0.0.8");

            assertFalse(result.allowed(), reason + " should deny");
            assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode(), reason + " → 804002");
        }
    }

    @Test
    void authenticate_tspThrows_shouldFailClosedWith804002() {
        when(tspDeviceAdmissionService.decide(any())).thenThrow(new RuntimeException("TSP unavailable"));

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
        assertEquals("DEPENDENCY_UNAVAILABLE", result.reason());
        verify(metrics).incTspAdmission("DENY", "DEPENDENCY_UNAVAILABLE");
    }

    @Test
    void authenticate_nullTspResponse_shouldFailClosedWith804002() {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(null);

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
        assertTrue(result.reason().contains("INVALID_ADMISSION_RESPONSE"));
    }

    @Test
    void authenticate_nullDecision_shouldFailClosedWith804002() {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(DeviceAdmissionResultVo.builder().build());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.DEVICE_BLOCKED, result.errorCode());
    }

    @Test
    void authenticate_requiredDisabled_shouldDenyWithoutTspCall() {
        admissionProperties.getPeerCertificate().setRequired(false);

        AuthAclService.AuthResult result = authAclService.authenticate(
                "DEVICE-001", "client001", validPem, "10.0.0.8");

        assertFalse(result.allowed());
        assertEquals(ErrorCode.AUTH_IDENTITY_MISMATCH, result.errorCode());
        assertEquals("ADMISSION_NOT_READY", result.reason());
        verifyNoInteractions(tspDeviceAdmissionService);
    }

    @Test
    void authenticate_emptyOrInvalidDeviceSn_shouldDeny() {
        assertEquals(ErrorCode.DEVICE_UNKNOWN,
                authAclService.authenticate("", "client001", validPem, "10.0.0.8").errorCode());
        assertEquals(ErrorCode.DEVICE_UNKNOWN,
                authAclService.authenticate(null, "client001", validPem, "10.0.0.8").errorCode());
        assertEquals(ErrorCode.DEVICE_UNKNOWN,
                authAclService.authenticate("invalid@sn", "client001", validPem, "10.0.0.8").errorCode());
        verifyNoInteractions(tspDeviceAdmissionService);
    }

    @Test
    void authenticate_lowercaseDeviceSn_shouldNormalize() {
        when(tspDeviceAdmissionService.decide(any())).thenReturn(allowResult());

        AuthAclService.AuthResult result = authAclService.authenticate(
                "device-001", "client001", validPem, "10.0.0.8");

        assertTrue(result.allowed());
        assertEquals("DEVICE-001", result.deviceSn());
        assertTrue(result.acl().get(0).getTopic().contains("DEVICE-001"));
    }

    private static byte[] derBytes(String pem) throws Exception {
        X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
        return certificate.getEncoded();
    }

    private static String base64Der(String pem) throws Exception {
        return Base64.getEncoder().encodeToString(derBytes(pem));
    }

    private static String resource(String path) {
        try (InputStream in = AuthAclServiceImplTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in, "missing test resource: " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256Hex(byte[] data) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
