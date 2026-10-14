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
 * PeerCertificateContextFactory 单元测试（EDD-VAGW-DSN-CR-013 §8，EDD-VAGW-DSN-CR-014 §7.1）。
 * <p>
 * 覆盖：严格 PEM 与标准 Base64(DER) 双格式、两种输入派生值一致、占位符泄漏、
 * 非法 Base64/URL-safe/缺填充、超限、非 X.509、拼接与尾随字节、私钥/多证书/尾随文本 PEM、
 * CN 归一化与不一致、serial 稳定解析，以及 outcome 收敛为固定 8 项。
 * </p>
 */
class PeerCertificateContextFactoryTest {

    private PeerCertificateContextFactory factory;
    private VagwAdmissionProperties properties;
    private String validPem;

    @BeforeEach
    void setUp() {
        properties = new VagwAdmissionProperties();
        factory = new PeerCertificateContextFactory(properties);
        validPem = resource("certs/leaf-valid.pem");
    }

    @Test
    void create_validPem_shouldDeriveDerSerialAndFingerprint() throws Exception {
        VerifiedPeerCertificate context = factory.create(validPem, "DEVICE-001");

        assertEquals("DEVICE-001", context.normalizedUid());
        assertNotNull(context.der());
        assertTrue(context.der().length > 0);
        // canonical serial 与 openssl 独立解析一致（无符号小写 hex）
        assertEquals("35c7a12acb912925e2ef0a4a1f1666086c4633e8", context.canonicalSerial());
        // fingerprint = SHA-256(canonical DER) 小写 hex
        X509Certificate certificate = parse(validPem);
        assertEquals(sha256Hex(certificate.getEncoded()), context.sha256Fingerprint());
        assertTrue(context.sha256Fingerprint().matches("[0-9a-f]{64}"));
        // DER 标准 Base64 可还原为同一证书
        byte[] decoded = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(context.der()));
        assertArrayEquals(context.der(), decoded);
    }

    @Test
    void create_validEmqxBase64Der_shouldDeriveDerSerialAndFingerprint() throws Exception {
        VerifiedPeerCertificate context = factory.create(base64Der(validPem), "DEVICE-001");

        assertEquals("DEVICE-001", context.normalizedUid());
        assertEquals("35c7a12acb912925e2ef0a4a1f1666086c4633e8", context.canonicalSerial());
        assertArrayEquals(derBytes(validPem), context.der());
        assertEquals(sha256Hex(context.der()), context.sha256Fingerprint());
    }

    @Test
    void create_pemAndBase64Der_shouldProduceIdenticalContext() throws Exception {
        VerifiedPeerCertificate fromPem = factory.create(validPem, "DEVICE-001");
        VerifiedPeerCertificate fromBase64 = factory.create(base64Der(validPem), "DEVICE-001");

        assertArrayEquals(fromPem.der(), fromBase64.der());
        assertEquals(fromPem.normalizedUid(), fromBase64.normalizedUid());
        assertEquals(fromPem.canonicalSerial(), fromBase64.canonicalSerial());
        assertEquals(fromPem.sha256Fingerprint(), fromBase64.sha256Fingerprint());
    }

    @Test
    void create_base64DerWithAsciiWhitespace_shouldPass() throws Exception {
        String base64 = base64Der(validPem);
        String withWhitespace = " \t" + base64.substring(0, 20) + "\r\n" + base64.substring(20) + "\n ";

        VerifiedPeerCertificate context = factory.create(withWhitespace, "DEVICE-001");

        assertArrayEquals(derBytes(validPem), context.der());
    }

    @Test
    void create_validPem_shouldRejectPrivateKeyMarker() {
        // 私钥/reject 结构缺陷统一收敛为 PEM_MALFORMED
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(resource("certs/leaf-private-key.pem"), "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED, ex.getOutcome());
    }

    @Test
    void create_multipleCertificates_shouldReject() {
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(resource("certs/leaf-multi.pem"), "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED, ex.getOutcome());
    }

    @Test
    void create_trailingNonEmptyData_shouldReject() {
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(resource("certs/leaf-trailing.pem"), "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED, ex.getOutcome());
    }

    @Test
    void create_derOverLimit_shouldReject() {
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(resource("certs/leaf-oversized.pem"), "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.DER_TOO_LARGE, ex.getOutcome());
    }

    @Test
    void create_base64DerOverLimit_shouldReject() {
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(base64Der(resource("certs/leaf-oversized.pem")), "DEVICE-001"));
        assertEquals(PeerCertificateContextFactory.DER_TOO_LARGE, ex.getOutcome());
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
        // 无 PEM 边界 → 走 Base64(DER) 路径，非 Base64 字符被拒
        assertEquals(PeerCertificateContextFactory.DER_BASE64_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("not-a-pem", "DEVICE-001")).getOutcome());
        // 合法 PEM 边界但 base64 体非法
        String broken = "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----";
        assertEquals(PeerCertificateContextFactory.PEM_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create(broken, "DEVICE-001")).getOutcome());
    }

    @Test
    void create_placeholderLeak_shouldRejectWithDedicatedOutcome() {
        assertEquals(PeerCertificateContextFactory.PEM_PLACEHOLDER_LEAK,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("${cert_pem}", "DEVICE-001")).getOutcome());
        assertEquals(PeerCertificateContextFactory.PEM_PLACEHOLDER_LEAK,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("  ${cert_pem}  ", "DEVICE-001")).getOutcome());
        assertEquals(PeerCertificateContextFactory.PEM_PLACEHOLDER_LEAK,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("prefix${cert_pem}suffix", "DEVICE-001")).getOutcome());
    }

    @Test
    void create_base64MalformedVariants_shouldReject() {
        // 非法字符
        assertEquals(PeerCertificateContextFactory.DER_BASE64_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("!!!!", "DEVICE-001")).getOutcome());
        // URL-safe 字母表
        assertEquals(PeerCertificateContextFactory.DER_BASE64_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("AQID-_==", "DEVICE-001")).getOutcome());
        // 缺失填充（重编码不一致）
        assertEquals(PeerCertificateContextFactory.DER_BASE64_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("AQIDBA", "DEVICE-001")).getOutcome());
        // 错误填充
        assertEquals(PeerCertificateContextFactory.DER_BASE64_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("AQIDBA=", "DEVICE-001")).getOutcome());
        // 仅 ASCII 空白 → 首层空值判定
        assertEquals(PeerCertificateContextFactory.PEM_MISSING,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create(" \t\r\n ", "DEVICE-001")).getOutcome());
        // 非 ASCII 空白不属于空值，也不被移除 → Base64 解码失败
        assertEquals(PeerCertificateContextFactory.DER_BASE64_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create("\u00A0", "DEVICE-001")).getOutcome());
    }

    @Test
    void create_base64OfNonX509_shouldReject() {
        String base64 = Base64.getEncoder().encodeToString("not-a-certificate".getBytes(StandardCharsets.US_ASCII));
        assertEquals(PeerCertificateContextFactory.X509_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create(base64, "DEVICE-001")).getOutcome());
    }

    @Test
    void create_base64OfConcatenatedDer_shouldReject() throws Exception {
        byte[] one = derBytes(validPem);
        byte[] two = new byte[one.length * 2];
        System.arraycopy(one, 0, two, 0, one.length);
        System.arraycopy(one, 0, two, one.length, one.length);
        String base64 = Base64.getEncoder().encodeToString(two);
        assertEquals(PeerCertificateContextFactory.X509_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create(base64, "DEVICE-001")).getOutcome());
    }

    @Test
    void create_base64WithTrailingBytes_shouldReject() throws Exception {
        byte[] one = derBytes(validPem);
        byte[] withTrailing = new byte[one.length + 3];
        System.arraycopy(one, 0, withTrailing, 0, one.length);
        String base64 = Base64.getEncoder().encodeToString(withTrailing);
        assertEquals(PeerCertificateContextFactory.X509_MALFORMED,
                assertThrows(PeerCertificateException.class,
                        () -> factory.create(base64, "DEVICE-001")).getOutcome());
    }

    @Test
    void create_caseInsensitiveExpectedUid_shouldPass() {
        VerifiedPeerCertificate context = factory.create(validPem, "device-001");
        assertEquals("DEVICE-001", context.normalizedUid());
    }

    @Test
    void create_cnMismatch_shouldReject() {
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(validPem, "OTHER-DEVICE"));
        assertEquals(PeerCertificateContextFactory.CN_MISMATCH, ex.getOutcome());
        // Base64(DER) 输入同样按 CN 校验
        PeerCertificateException base64Ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(base64Der(validPem), "OTHER-DEVICE"));
        assertEquals(PeerCertificateContextFactory.CN_MISMATCH, base64Ex.getOutcome());
    }

    @Test
    void create_derExactlyAtLimit_shouldPass() throws Exception {
        // 覆盖 max-der-bytes 边界：放宽上限后，超限样本可解析
        properties.getPeerCertificate().setMaxDerBytes(16384);
        VerifiedPeerCertificate fromPem = factory.create(resource("certs/leaf-oversized.pem"), "DEVICE-001");
        assertNotNull(fromPem.canonicalSerial());
        VerifiedPeerCertificate fromBase64 =
                factory.create(base64Der(resource("certs/leaf-oversized.pem")), "DEVICE-001");
        assertArrayEquals(fromPem.der(), fromBase64.der());
    }

    @Test
    void create_failureOutcomeAndMessage_shouldNotLeakSensitiveInput() throws Exception {
        String base64 = base64Der(validPem);
        PeerCertificateException ex = assertThrows(PeerCertificateException.class,
                () -> factory.create(base64, "OTHER-DEVICE"));
        assertEquals(PeerCertificateContextFactory.CN_MISMATCH, ex.getOutcome());
        assertFalse(ex.getMessage().contains(base64));
        assertFalse(ex.getMessage().contains("DEVICE-001"));
    }

    private static byte[] derBytes(String pem) throws Exception {
        return parse(pem).getEncoded();
    }

    private static String base64Der(String pem) throws Exception {
        return Base64.getEncoder().encodeToString(derBytes(pem));
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
