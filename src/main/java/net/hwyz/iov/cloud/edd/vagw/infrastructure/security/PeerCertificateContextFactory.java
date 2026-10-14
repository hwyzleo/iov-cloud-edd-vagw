package net.hwyz.iov.cloud.edd.vagw.infrastructure.security;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAdmissionProperties;
import net.hwyz.iov.cloud.edd.vagw.model.security.VerifiedPeerCertificate;
import org.springframework.stereotype.Component;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 叶子证书上下文工厂（EDD-VAGW-DSN-CR-013 §4.2，EDD-VAGW-DSN-CR-014 §4.1）。
 * <p>
 * 从 EMQX HTTP authn 回调的 {@code peer_cert}（{@code ${cert_pem}} 原样值）构造瞬时
 * {@link VerifiedPeerCertificate}。EMQX 5.8.x 的 {@code ${cert_pem}} 实际输出单张叶子证书
 * DER 的标准 Base64 文本（无 {@code BEGIN/END} 边界）；VAGW 兼容严格 PEM 输入：
 * <ol>
 *   <li>拒绝空值；</li>
 *   <li>拒绝含 {@code ${}} 的未替换占位符（{@link #PEM_PLACEHOLDER_LEAK}），不再继续解析；</li>
 *   <li>含精确 {@code -----BEGIN CERTIFICATE-----} 边界时只走严格单证书 PEM 路径，失败不回退 Base64；</li>
 *   <li>否则按标准 Base64(DER) 严格解析：仅移除 ASCII 空白，标准解码后以标准编码重编码比对；</li>
 *   <li>使用 JCA CertificateFactory 解析单张 X.509，且 {@code getEncoded()} 与解码 DER 一致；</li>
 *   <li>解析前后限制 DER ≤ maxDerBytes（默认 8 KiB）；</li>
 *   <li>读取 Subject CN，trim + {@link Locale#ROOT} 大写，且必须等于 peer_cert_cn；</li>
 *   <li>从 X509Certificate.getSerialNumber() 读取 serial，规范化为无符号 canonical hex；</li>
 *   <li>SHA-256 fingerprint 输出小写 hex。</li>
 * </ol>
 * 任何不确定均 fail-closed（{@link PeerCertificateException}），不调用 TSP、不生成 ACL、不建立会话。
 * 原始 PEM/DER 与 fingerprint 仅存在于调用栈内，不进入缓存、数据库、事件或普通日志。
 * </p>
 * <p>
 * 对外 {@code outcome} 严格收敛为有界固定集（EDD-VAGW-DSN-CR-014 §4.2），保证
 * {@code vagw_peer_certificate_validation_total{outcome}} 低基数；更细的 PEM 结构缺陷仅作为
 * 异常 cause/subtype 供内部排障，不进入指标标签或对外错误原因。
 * </p>
 */
@Component
@RequiredArgsConstructor
public class PeerCertificateContextFactory {

    /** 空值 */
    static final String PEM_MISSING = "PEM_MISSING";
    /** 未替换的 EMQX 占位符（如 {@code ${cert_pem}}） */
    static final String PEM_PLACEHOLDER_LEAK = "PEM_PLACEHOLDER_LEAK";
    /** PEM 结构/单证书约束失败（含私钥标记、多证书、尾随数据、畸形边界与非法 base64 体） */
    static final String PEM_MALFORMED = "PEM_MALFORMED";
    /** 标准 Base64(DER) 解码或规范性失败 */
    static final String DER_BASE64_MALFORMED = "DER_BASE64_MALFORMED";
    /** DER 超限 */
    static final String DER_TOO_LARGE = "DER_TOO_LARGE";
    /** X.509 解析或完整消费失败 */
    static final String X509_MALFORMED = "X509_MALFORMED";
    /** 证书 Subject CN 缺失、多值或与 peer_cert_cn 不一致 */
    static final String CN_MISMATCH = "CN_MISMATCH";
    /** serial / fingerprint 等派生值失败 */
    static final String CERTIFICATE_METADATA_INVALID = "CERTIFICATE_METADATA_INVALID";

    private static final Pattern CERTIFICATE_BLOCK = Pattern.compile(
            "-----BEGIN CERTIFICATE-----([\\s\\S]*?)-----END CERTIFICATE-----");
    private static final String CERTIFICATE_BEGIN = "-----BEGIN CERTIFICATE-----";
    private static final String PRIVATE_KEY_MARKER = "PRIVATE KEY";
    private static final String PLACEHOLDER_MARKER = "${";

    private final VagwAdmissionProperties properties;

    /**
     * 构造已验证叶子证书上下文。
     *
     * @param peerCert    EMQX {@code ${cert_pem}} 原样值：标准 Base64(DER) 或严格 PEM
     * @param expectedUid 期望身份（EMQX {@code ${cert_common_name}} / 归一化 peer_cert_cn）；
     *                    非空时必须等于证书内 Subject CN
     * @return 瞬时证书上下文
     * @throws PeerCertificateException 任一校验失败（fail-closed）
     */
    public VerifiedPeerCertificate create(String peerCert, String expectedUid) {
        if (peerCert == null || peerCert.isBlank()) {
            throw new PeerCertificateException(PEM_MISSING);
        }
        // 占位符未替换：EMQX 模板/版本配置错误，直接拒绝，不进入格式猜测
        if (peerCert.contains(PLACEHOLDER_MARKER)) {
            throw new PeerCertificateException(PEM_PLACEHOLDER_LEAK);
        }
        // 私钥/CSR 等非叶子证书内容一律拒绝，避免认证模板错配把私钥误传入（PEM 结构缺陷，收敛为 PEM_MALFORMED）
        if (peerCert.contains(PRIVATE_KEY_MARKER)) {
            throw pemMalformed("PRIVATE_KEY_MARKER");
        }

        boolean pemInput = peerCert.contains(CERTIFICATE_BEGIN);
        int maxDerBytes = properties.getPeerCertificate().getMaxDerBytes();
        byte[] der = pemInput
                ? decodeStrictSingleCertificatePem(peerCert)
                : decodeStrictCanonicalBase64Der(peerCert, maxDerBytes);

        if (der.length == 0 || der.length > maxDerBytes) {
            throw new PeerCertificateException(DER_TOO_LARGE);
        }

        X509Certificate certificate = parseExactlyOneX509(der);
        byte[] canonicalDer;
        try {
            canonicalDer = certificate.getEncoded();
        } catch (Exception e) {
            throw new PeerCertificateException(X509_MALFORMED, e);
        }
        // 解析后再次做边界检查
        if (canonicalDer.length == 0 || canonicalDer.length > maxDerBytes) {
            throw new PeerCertificateException(DER_TOO_LARGE);
        }
        // Base64(DER) 路径要求解析完整消费：重编码 DER 必须与解码字节一致（拒绝拼接/尾随/非规范）
        if (!pemInput && !MessageDigest.isEqual(canonicalDer, der)) {
            throw new PeerCertificateException(X509_MALFORMED);
        }

        String normalizedCn = extractSingleCommonName(certificate);
        if (!normalizedCn.equals(normalizeUid(expectedUid))) {
            throw new PeerCertificateException(CN_MISMATCH);
        }

        String canonicalSerial = canonicalSerial(certificate);
        String fingerprint = sha256Hex(canonicalDer);

        return new VerifiedPeerCertificate(canonicalDer, normalizedCn, canonicalSerial, fingerprint);
    }

    /**
     * 严格单证书 PEM 解析：拒绝私钥标记、多证书、尾随非空数据与非法 base64 体。
     * 任一失败均收敛为 {@link #PEM_MALFORMED}，且调用方不得回退 Base64 路径。
     */
    private static byte[] decodeStrictSingleCertificatePem(String pem) {
        Matcher matcher = CERTIFICATE_BLOCK.matcher(pem);
        int blockCount = 0;
        int firstStart = -1;
        int firstEnd = -1;
        String base64Body = null;
        while (matcher.find()) {
            blockCount++;
            if (blockCount == 1) {
                firstStart = matcher.start();
                firstEnd = matcher.end();
                base64Body = matcher.group(1);
            }
        }
        if (blockCount == 0) {
            throw pemMalformed("NO_CERTIFICATE_BLOCK");
        }
        if (blockCount > 1) {
            throw pemMalformed("MULTIPLE_CERTIFICATES");
        }
        // 尾随非空数据（BEGIN 之前或 END 之后）一律拒绝：防止拼接/注入
        if (!pem.substring(0, firstStart).trim().isEmpty() || !pem.substring(firstEnd).trim().isEmpty()) {
            throw pemMalformed("TRAILING_DATA");
        }

        String compact = base64Body.replaceAll("\\s", "");
        try {
            return Base64.getDecoder().decode(compact);
        } catch (IllegalArgumentException e) {
            throw pemMalformed("BASE64_DECODE_FAILED", e);
        }
    }

    /**
     * 严格标准 Base64(DER) 解析（EDD-VAGW-DSN-CR-014 §4.1）：
     * 仅移除 ASCII SP/TAB/CR/LF；标准解码后以标准编码重编码并比对，拒绝 URL-safe、缺失/错误填充与其他非规范形式。
     */
    private static byte[] decodeStrictCanonicalBase64Der(String value, int maxDerBytes) {
        String normalized = removeAsciiWhitespace(value);
        if (normalized.isEmpty()) {
            throw new PeerCertificateException(DER_BASE64_MALFORMED);
        }
        // 保守上限预判：Base64 编码长度超过上限时不可能解码为合法大小的 DER
        long maxBase64Length = ((long) (maxDerBytes / 3) + 1) * 4;
        if (normalized.length() > maxBase64Length) {
            throw new PeerCertificateException(DER_TOO_LARGE);
        }

        byte[] der;
        try {
            der = Base64.getDecoder().decode(normalized);
        } catch (IllegalArgumentException e) {
            throw new PeerCertificateException(DER_BASE64_MALFORMED, e);
        }
        String canonical = Base64.getEncoder().encodeToString(der);
        if (!MessageDigest.isEqual(
                canonical.getBytes(StandardCharsets.US_ASCII),
                normalized.getBytes(StandardCharsets.US_ASCII))) {
            throw new PeerCertificateException(DER_BASE64_MALFORMED);
        }
        return der;
    }

    private static X509Certificate parseExactlyOneX509(byte[] der) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Collection<? extends java.security.cert.Certificate> certificates =
                    factory.generateCertificates(new ByteArrayInputStream(der));
            if (certificates.size() != 1) {
                throw new PeerCertificateException(X509_MALFORMED);
            }
            java.security.cert.Certificate certificate = certificates.iterator().next();
            if (!(certificate instanceof X509Certificate x509Certificate)) {
                throw new PeerCertificateException(X509_MALFORMED);
            }
            return x509Certificate;
        } catch (PeerCertificateException e) {
            throw e;
        } catch (Exception e) {
            throw new PeerCertificateException(X509_MALFORMED, e);
        }
    }

    private static String extractSingleCommonName(X509Certificate certificate) {
        List<String> commonNames = new ArrayList<>();
        try {
            LdapName subject = new LdapName(certificate.getSubjectX500Principal().getName());
            for (Rdn rdn : subject.getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    Object value = rdn.getValue();
                    if (value != null) {
                        commonNames.add(value.toString());
                    }
                }
            }
        } catch (Exception e) {
            throw new PeerCertificateException(CN_MISMATCH, e);
        }
        if (commonNames.isEmpty()) {
            throw new PeerCertificateException(CN_MISMATCH);
        }
        // 多个 CN 无法确定权威身份，fail-closed
        if (commonNames.size() > 1) {
            throw new PeerCertificateException(CN_MISMATCH);
        }
        String normalized = normalizeUid(commonNames.get(0));
        if (normalized.isEmpty()) {
            throw new PeerCertificateException(CN_MISMATCH);
        }
        return normalized;
    }

    private static String canonicalSerial(X509Certificate certificate) {
        BigInteger serial = certificate.getSerialNumber();
        if (serial == null) {
            throw new PeerCertificateException(CERTIFICATE_METADATA_INVALID);
        }
        // 统一为无符号 canonical hex（小写、无前导零），不受 DER 符号位影响
        String canonical = new BigInteger(1, serial.toByteArray()).toString(16);
        if (canonical.isEmpty()) {
            throw new PeerCertificateException(CERTIFICATE_METADATA_INVALID);
        }
        return canonical;
    }

    private static String sha256Hex(byte[] der) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(der);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (Exception e) {
            // SHA-256 在所有 JVM 均可用；不可用即无法保证一致性，fail-closed
            throw new PeerCertificateException(CERTIFICATE_METADATA_INVALID, e);
        }
    }

    /** 仅移除 ASCII SP/TAB/CR/LF，保留其余字符交由严格 Base64 校验拒绝。 */
    private static String removeAsciiWhitespace(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    /** PEM 结构缺陷统一收敛为 {@link #PEM_MALFORMED}，detail 仅作为内部异常 cause，不进入 outcome 指标。 */
    private static PeerCertificateException pemMalformed(String detail) {
        return new PeerCertificateException(PEM_MALFORMED, new IllegalArgumentException(detail));
    }

    private static PeerCertificateException pemMalformed(String detail, Throwable cause) {
        return new PeerCertificateException(PEM_MALFORMED,
                new IllegalArgumentException(detail, cause));
    }

    private static String normalizeUid(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
