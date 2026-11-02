package net.hwyz.iov.cloud.edd.vagw.controller;

import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.AdmissionSessionContextRegistry;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAdmissionProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.security.PeerCertificateContextFactory;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthResponse;
import net.hwyz.iov.cloud.edd.vagw.service.AuthAclServiceImpl;
import net.hwyz.iov.cloud.edd.vagw.service.AuthSecurityMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.AuthenticatedIdentityStore;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.SuperuserBypassGuard;
import net.hwyz.iov.cloud.edd.vagw.service.VagwSelfConnectionRegistry;
import net.hwyz.iov.cloud.sec.viam.api.service.ViamDeviceAdmissionService;
import net.hwyz.iov.cloud.sec.viam.api.vo.DeviceAdmissionCheckVo;
import net.hwyz.iov.cloud.sec.viam.api.vo.DeviceAdmissionResultVo;
import net.hwyz.iov.cloud.sec.viam.api.vo.enums.AdmissionDecision;
import net.hwyz.iov.cloud.sec.viam.api.vo.enums.AdmissionReason;
import net.hwyz.iov.cloud.sec.viam.api.vo.enums.BindStatus;
import net.hwyz.iov.cloud.sec.viam.api.vo.enums.CertificateStatus;
import net.hwyz.iov.cloud.sec.viam.api.vo.enums.DeviceStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 设备准入全链路集成测试（EDD-VAGW-DSN-CR-013 §8）。
 * <p>
 * 真实装配 {@link MqttAuthController} + {@link AuthAclServiceImpl} + {@link PeerCertificateContextFactory}
 * + {@link AuthSecurityMetrics}，仅以 fake/stub 替代 VIAM 准入、绑定与会话快照；
 * 覆盖 PEM → DER/serial/fingerprint → VIAM decide → ALLOW/DENY 映射与敏感数据不泄漏。
 * </p>
 */
class AuthAdmissionPathIntegrationTest {

    private static final String DEVICE = "DEVICE-001";
    private static final String CLIENT_ID = "client001";
    private static final String SOURCE_IP = "10.0.0.8";
    private static final String EXPECTED_SERIAL = "35c7a12acb912925e2ef0a4a1f1666086c4633e8";

    private FakeViamDeviceAdmissionService viam;
    private BindingService bindingService;
    private AuthenticatedIdentityStore identityStore;
    private AdmissionSessionContextRegistry admissionContextRegistry;
    private VagwSelfConnectionRegistry vagwSelfConnectionRegistry;
    private AuthSecurityMetrics metrics;
    private MqttAuthController controller;
    private SuperuserBypassGuard bypassGuard;
    private String validPem;

    @BeforeEach
    void setUp() {
        viam = new FakeViamDeviceAdmissionService();
        bindingService = mock(BindingService.class);
        identityStore = mock(AuthenticatedIdentityStore.class);
        admissionContextRegistry = mock(AdmissionSessionContextRegistry.class);
        vagwSelfConnectionRegistry = mock(VagwSelfConnectionRegistry.class);
        metrics = new AuthSecurityMetrics();

        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        PeerCertificateContextFactory factory = new PeerCertificateContextFactory(properties);
        AuthAclServiceImpl service = new AuthAclServiceImpl(viam, bindingService,
                mock(KeyprovProperties.class), mock(KeyprovContractGuard.class), factory, properties, metrics);
        SuperuserBypassGuard bypassGuard = mock(SuperuserBypassGuard.class);
        this.bypassGuard = bypassGuard;
        when(bypassGuard.isBypassAllowed(any())).thenReturn(false);
        when(admissionContextRegistry.saveCandidate(any(), any(), any(), any(), any(), any(), any(), any(), anyLong()))
                .thenReturn(Optional.of(new AdmissionSessionContext(DEVICE, "VIN001", EXPECTED_SERIAL,
                        CLIENT_ID, SOURCE_IP, 4, 1L, Instant.now(), Instant.now().plusSeconds(300),
                        "ADM-integration", 9L, 0L)));

        controller = new MqttAuthController(service, bypassGuard, metrics, identityStore, admissionContextRegistry,
                vagwSelfConnectionRegistry);
        validPem = resource("certs/leaf-valid.pem");
    }

    private MqttAuthRequest request(String peerCert) {
        return MqttAuthRequest.builder()
                .username(DEVICE)
                .clientId(CLIENT_ID)
                .peerCertCn(DEVICE)
                .peerCert(peerCert)
                .peerHost(SOURCE_IP)
                .protoVer(4)
                .build();
    }

    private static DeviceAdmissionResultVo allowResult() {
        return DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.ALLOW)
                .reason(AdmissionReason.NONE)
                .vin("VIN001")
                .bindStatus(BindStatus.BOUND)
                .certStatus(CertificateStatus.VALID)
                .deviceStatus(DeviceStatus.ACTIVATING)
                .bindVersion(9L)
                .updatedAt(Instant.parse("2026-10-12T00:00:00Z"))
                .admissionDecisionId("ADM-0123456789abcdef0123456789abcdef")
                .build();
    }

    @Test
    void goodCertificate_shouldAllowAndForwardExactDerSerialFingerprint() throws Exception {
        viam.response = allowResult();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request(validPem));

        assertEquals("allow", response.getBody().getResult());
        assertFalse(response.getBody().getIsSuperuser());
        assertEquals(1, viam.calls.size());
        DeviceAdmissionCheckVo vo = viam.calls.get(0);
        assertEquals(DEVICE, vo.getUid());
        assertEquals(EXPECTED_SERIAL, vo.getCertSerial());
        assertEquals(CLIENT_ID, vo.getClientId());
        assertEquals(SOURCE_IP, vo.getSourceIp());
        assertNotNull(vo.getTs());

        byte[] der = Base64.getDecoder().decode(vo.getLeafCertificateDer());
        assertEquals(vo.getCertFingerprint(), sha256Hex(der));
        // VIAM 拿到的 DER 可还原为同一张叶子证书
        X509Certificate parsed = parseDer(der);
        assertEquals(DEVICE, cn(parsed));
        // 准入成功后才固化会话快照与身份
        verify(identityStore).save(CLIENT_ID, DEVICE, EXPECTED_SERIAL, null);
        verify(admissionContextRegistry).saveCandidate(DEVICE, "VIN001", EXPECTED_SERIAL, CLIENT_ID, SOURCE_IP, 4,
                "ADM-0123456789abcdef0123456789abcdef", 9L, 0L);
        verify(bindingService).rememberAdmission(DEVICE, "VIN001", 9L);
    }

    @Test
    void emqxBase64DerCertificate_shouldAllowAndForwardExactDer() throws Exception {
        viam.response = allowResult();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request(base64Der(validPem)));

        assertEquals("allow", response.getBody().getResult());
        assertEquals(1, viam.calls.size());
        DeviceAdmissionCheckVo vo = viam.calls.get(0);
        assertEquals(DEVICE, vo.getUid());
        assertEquals(EXPECTED_SERIAL, vo.getCertSerial());
        assertArrayEquals(derOf(validPem), Base64.getDecoder().decode(vo.getLeafCertificateDer()));
        verify(admissionContextRegistry).saveCandidate(DEVICE, "VIN001", EXPECTED_SERIAL, CLIENT_ID, SOURCE_IP, 4,
                "ADM-0123456789abcdef0123456789abcdef", 9L, 0L);
    }

    @Test
    void placeholderLeak_shouldDeny804016BeforeTspCall() {
        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request("${cert_pem}"));

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("PEM_PLACEHOLDER_LEAK"));
        assertTrue(viam.calls.isEmpty(), "placeholder leak must not call VIAM");
        assertEquals(1L, metrics.get("mqtt_peer_certificate_validation_total{outcome=PEM_PLACEHOLDER_LEAK}"));
    }

    @Test
    void revokedCertificate_shouldDeny804002() {
        viam.response = DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.DENY)
                .reason(AdmissionReason.CERT_REVOKED)
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request(validPem));

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804002"));
        assertTrue(response.getBody().getReason().contains("CERT_REVOKED"));
        verifyNoInteractions(identityStore);
        verify(admissionContextRegistry, never()).saveCandidate(any(), any(), any(), any(), any(), any(), any(), any(), anyLong());
    }

    @Test
    void pkiDependencyUnavailable_shouldDeny804002() {
        viam.response = DeviceAdmissionResultVo.builder()
                .decision(AdmissionDecision.DENY)
                .reason(AdmissionReason.DEPENDENCY_UNAVAILABLE)
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request(validPem));

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804002"));
        assertTrue(response.getBody().getReason().contains("DEPENDENCY_UNAVAILABLE"));
    }

    @Test
    void viamThrows_shouldFailClosedWithoutAllow() {
        viam.error = new RuntimeException("VIAM timeout");

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request(validPem));

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804002"));
        verifyNoInteractions(identityStore);
    }

    @Test
    void missingPeerCert_shouldDeny804016BeforeTspCall() {
        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request(null));

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("PEM_MISSING"));
        assertTrue(viam.calls.isEmpty(), "invalid certificate must not call VIAM");
    }

    @Test
    void certificateCnMismatch_shouldDeny804016BeforeTspCall() {
        MqttAuthRequest mismatched = MqttAuthRequest.builder()
                .username("OTHER-DEVICE")
                .clientId(CLIENT_ID)
                .peerCertCn("OTHER-DEVICE")
                .peerCert(validPem)
                .peerHost(SOURCE_IP)
                .protoVer(4)
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(mismatched);

        assertEquals("deny", response.getBody().getResult());
        assertTrue(response.getBody().getReason().contains("804016"));
        assertTrue(response.getBody().getReason().contains("CN_MISMATCH"));
        assertTrue(viam.calls.isEmpty(), "CN mismatch must not call VIAM");
    }

    @Test
    void sensitiveFields_shouldNotAppearInDiagnosticsOrMetrics() {
        viam.response = allowResult();
        MqttAuthRequest request = request(validPem);
        request.setPassword("super-secret-service-password");

        String diagnostic = request.toString();
        assertFalse(diagnostic.contains("BEGIN CERTIFICATE"), "PEM must not leak via toString");
        assertFalse(diagnostic.contains("super-secret-service-password"), "password must not leak via toString");
        assertFalse(diagnostic.contains(validPem));

        controller.authenticate(request);

        Map<String, Long> snapshot = metrics.snapshot();
        for (String key : snapshot.keySet()) {
            assertFalse(key.contains("BEGIN CERTIFICATE"), "metric tag must not contain PEM");
            assertFalse(key.contains(EXPECTED_SERIAL), "metric tag must not contain full serial");
            assertFalse(key.contains(DEVICE), "metric tag must not contain full identity");
        }
    }

    @Test
    void superuserBypass_shouldMarkSelfConnectionWithoutDeviceFlow() {
        when(bypassGuard.isBypassAllowed(any())).thenReturn(true);

        MqttAuthRequest request = MqttAuthRequest.builder()
                .clientId("vehicle-access-gatewayintegration1")
                .build();

        ResponseEntity<MqttAuthResponse> response = controller.authenticate(request);

        assertEquals("allow", response.getBody().getResult());
        assertTrue(response.getBody().getIsSuperuser());
        verify(vagwSelfConnectionRegistry).mark("vehicle-access-gatewayintegration1");
        verify(admissionContextRegistry, never()).saveCandidate(any(), any(), any(), any(), any(), any(), any(), any(), anyLong());
        verify(identityStore, never()).save(any(), any(), any(), any());
        assertTrue(viam.calls.isEmpty(), "bypass must not call VIAM");
    }

    private static String resource(String path) {
        try (InputStream in = AuthAdmissionPathIntegrationTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in, "missing test resource: " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static X509Certificate parseDer(byte[] der) throws Exception {
        return (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new java.io.ByteArrayInputStream(der));
    }

    private static byte[] derOf(String pem) throws Exception {
        X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new java.io.ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
        return certificate.getEncoded();
    }

    private static String base64Der(String pem) throws Exception {
        return Base64.getEncoder().encodeToString(derOf(pem));
    }

    private static String cn(X509Certificate certificate) throws Exception {
        for (javax.naming.ldap.Rdn rdn :
                new javax.naming.ldap.LdapName(certificate.getSubjectX500Principal().getName()).getRdns()) {
            if ("CN".equalsIgnoreCase(rdn.getType())) {
                return rdn.getValue().toString();
            }
        }
        return null;
    }

    private static String sha256Hex(byte[] data) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    /**
     * 真实 ViamDeviceAdmissionService 的最小实现：捕获请求并返回配置结果。
     */
    private static final class FakeViamDeviceAdmissionService implements ViamDeviceAdmissionService {
        private final List<DeviceAdmissionCheckVo> calls = new ArrayList<>();
        private DeviceAdmissionResultVo response;
        private RuntimeException error;

        @Override
        public DeviceAdmissionResultVo decide(DeviceAdmissionCheckVo request) {
            calls.add(request);
            if (error != null) {
                throw error;
            }
            return response;
        }
    }
}
