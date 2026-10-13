package net.hwyz.iov.cloud.edd.vagw.infrastructure.security;

import net.hwyz.iov.cloud.edd.vagw.config.VagwAdmissionProperties;
import net.hwyz.iov.cloud.edd.vagw.model.security.VerifiedPeerCertificate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PeerCertificateContextFactory 单元测试（EDD-VAGW-DSN-CR-013 §8）。
 * <p>
 * 覆盖：合法 PEM → DER/serial/fingerprint；缺失、空白、畸形、私钥、多证书、尾随文本、DER 超 8 KiB；
 * CN 大小写归一化与不一致；serial 稳定解析。
 * </p>
 */
class PeerCertificateContextFactoryTest {

    private PeerCertificateContextFactory factory;
    private VagwAdmissionProperties properties;

    @BeforeEach
    void setUp() {
        properties = new VagwAdmissionProperties();
        factory = new PeerCertificateContextFactory(properties);
    }

    @Test
    void create_validPem_shouldDeriveDerSerialAndFingerprint() throws Exception {
        String pem = resource("certs/leaf-valid.pem");

        VerifiedPeerCertificate context = factory.create(pem, "DEVICE-001");

        assertEquals("DEVICE-001", context.normalizedUid());
        assertNotNull(context.der());
        assertTrue(context.der().length > 0);
        // canonical serial 与 openssl 独立解析一致（无符号小写 hex）
        assertEquals("35c7a12acb912925e2ef0a4a1f1666086c4633e8", context.canonicalSerial());
        // fingerprint = SHA-256(canonical DER) 小写 hex
        X509Certificate certificate = parse(pem);
        assertEquals(sha256Hex(certificate.getEncoded()), context.sha256Fingerprint());
        assertTrue(context.sha256Fingerprint().matches("[0-9a-f]{64}"));
        // DER 标准 Base64 可还原为同一证书
        byte[] decoded = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(context.der()));
        assertArrayEquals(context.der(), decoded);
    }

    @Test
    void create_validPem_shouldRejectPrivateKeyMarker() {
        String pem = resource("certs/leaf-private-key.pem");
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(pem, "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_PRIVATE_KEY, ex.getOutcome());
    }

    @Test
    void create_multipleCertificates_shouldReject() {
        String pem = resource("certs/leaf-multi.pem");
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(pem, "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_MULTIPLE_CERTIFICATES, ex.getOutcome());
    }

    @Test
    void create_trailingNonEmptyData_shouldReject() {
        String pem = resource("certs/leaf-trailing.pem");
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(pem, "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_TRAILING_DATA, ex.getOutcome());
    }

    @Test
    void create_derOverLimit_shouldReject() {
        String pem = resource("certs/leaf-oversized.pem");
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(pem, "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_TOO_LARGE, ex.getOutcome());
    }

    @Test
    void create_missingOrBlank_shouldReject() {
        assertEquals(PeerCertificateContextFactory.PEM_MISSING,
                assertThrows(PeerCertificateException.class, () -> factory.create(null, "DEVICE-001")).getOutcome());
        assertEquals(PeerCertificateContextFactory.PEM_MISSING,
                assertThrows(PeerCertificateException.class, () -> factory.create("  ", "DEVICE-001")).getOutcome());
    }

    @Test
    void create_malformedPem_shouldReject() {
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("not-a-pem", "DEVICE-001")).getOutcome());
        // EMQX 未替换的占位符原样透传：无 BEGIN CERTIFICATE → 畸形
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("${cert_pem}", "DEVICE-001")).getOutcome());
        // 合法块但 base64 非法
        String broken = "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----";
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create(broken, "DEVICE-001")).getOutcome());
    }

    @Test
    void create_caseInsensitiveExpectedUid_shouldPass() {
        VerifiedPeerCertificate context = factory.create(resource("certs/leaf-valid.pem"), "device-001");
        assertEquals("DEVICE-001", context.normalizedUid());
    }

    @Test
    void create_cnMismatch_shouldReject() {
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(resource("certs/leaf-valid.pem"), "OTHER-DEVICE"));
        assertEquals(PeerCertificateContextFactory.CERT_CN_MISMATCH, ex.getOutcome());
    }

    @Test
    void create_derExactlyAtLimit_shouldPass() {
        // 覆盖 max-der-bytes 边界：放宽上限后，超限样本可解析
        properties.getPeerCertificate().setMaxDerBytes(16384);
        VerifiedPeerCertificate context = factory.create(resource("certs/leaf-oversized.pem"), "DEVICE-001");
        assertNotNull(context.canonicalSerial());
    }

    private static String resource(String path) {
        try (InputStream in = PeerCertificateContextFactoryTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in, "missing test resource: " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static X509Certificate parse(String pem) throws Exception {
        try (InputStream in = new java.io.ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))) {
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
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
