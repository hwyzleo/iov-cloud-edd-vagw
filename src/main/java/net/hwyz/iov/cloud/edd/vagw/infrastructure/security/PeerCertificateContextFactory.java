package net.hwyz.iov.cloud.edd.vagw.infrastructure.security;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAdmissionProperties;
import net.hwyz.iov.cloud.edd.vagw.model.security.VerifiedPeerCertificate;
import org.springframework.stereotype.Component;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.io.ByteArrayInputStream;
import java.math.BigInteger;
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
 * 叶子证书上下文工厂（EDD-VAGW-DSN-CR-013 §4.2）。
 * <p>
 * 从 EMQX {@code ${cert_pem}} 严格解析本次 mTLS 叶子证书，构造瞬时 {@link VerifiedPeerCertificate}：
 * <ol>
 *   <li>拒绝空值、私钥标记、多个 CERTIFICATE block 与尾随非空数据；</li>
 *   <li>使用 JCA CertificateFactory 解析单张 X.509；</li>
 *   <li>解析前后均限制 DER ≤ maxDerBytes（默认 8 KiB）；</li>
 *   <li>读取 Subject CN，trim + {@link Locale#ROOT} 大写，且必须等于 peer_cert_cn；</li>
 *   <li>从 X509Certificate.getSerialNumber() 读取 serial，规范化为无符号 canonical hex；</li>
 *   <li>SHA-256 fingerprint 输出小写 hex。</li>
 * </ol>
 * 任何不确定均 fail-closed（{@link PeerCertificateException}），不调用 TSP、不生成 ACL、不建立会话。
 * 原始 PEM/DER 与 fingerprint 仅存在于调用栈内，不进入缓存、数据库、事件或普通日志。
 * </p>
 */
@Component
@RequiredArgsConstructor
public class PeerCertificateContextFactory {

    static final String PEM_MISSING = "PEM_MISSING";
    static final String PEM_MALFORMED = "PEM_MALFORMED";
    static final String PEM_PRIVATE_KEY = "PEM_PRIVATE_KEY";
    static final String PEM_MULTIPLE_CERTIFICATES = "PEM_MULTIPLE_CERTIFICATES";
    static final String PEM_TRAILING_DATA = "PEM_TRAILING_DATA";
    static final String PEM_TOO_LARGE = "PEM_TOO_LARGE";
    static final String CERT_CN_MISSING = "CERT_CN_MISSING";
    static final String CERT_CN_MISMATCH = "CERT_CN_MISMATCH";
    static final String CERT_SERIAL_UNRESOLVED = "CERT_SERIAL_UNRESOLVED";

    private static final Pattern CERTIFICATE_BLOCK = Pattern.compile(
            "-----BEGIN CERTIFICATE-----([\\s\\S]*?)-----END CERTIFICATE-----");
    private static final String PRIVATE_KEY_MARKER = "PRIVATE KEY";

    private final VagwAdmissionProperties properties;

    /**
     * 构造已验证叶子证书上下文。
     *
     * @param pem         本次 mTLS 叶子证书 PEM（EMQX {@code ${cert_pem}}）
     * @param expectedUid 期望身份（EMQX {@code ${cert_common_name}} / 归一化 peer_cert_cn）；
     *                    非空时必须等于证书内 Subject CN
     * @return 瞬时证书上下文
     * @throws PeerCertificateException 任一校验失败（fail-closed）
     */
    public VerifiedPeerCertificate create(String pem, String expectedUid) {
        if (pem == null || pem.isBlank()) {
            throw new PeerCertificateException(PEM_MISSING);
        }
        // 私钥/CSR 等非叶子证书内容一律拒绝，避免认证模板错配把私钥误传入
        if (pem.contains(PRIVATE_KEY_MARKER)) {
            throw new PeerCertificateException(PEM_PRIVATE_KEY);
        }

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
            throw new PeerCertificateException(PEM_MALFORMED);
        }
        if (blockCount > 1) {
            throw new PeerCertificateException(PEM_MULTIPLE_CERTIFICATES);
        }
        // 尾随非空数据（BEGIN 之前或 END 之后）一律拒绝：防止拼接/注入
        if (!pem.substring(0, firstStart).trim().isEmpty() || !pem.substring(firstEnd).trim().isEmpty()) {
            throw new PeerCertificateException(PEM_TRAILING_DATA);
        }

        byte[] der = decodeBase64(base64Body);

        int maxDerBytes = properties.getPeerCertificate().getMaxDerBytes();
        if (der.length == 0 || der.length > maxDerBytes) {
            throw new PeerCertificateException(PEM_TOO_LARGE);
        }

        X509Certificate certificate = parseSingleCertificate(der);
        byte[] canonicalDer;
        try {
            canonicalDer = certificate.getEncoded();
        } catch (Exception e) {
            throw new PeerCertificateException(PEM_MALFORMED, e);
        }
        // 解析前后都做边界检查
        if (canonicalDer.length == 0 || canonicalDer.length > maxDerBytes) {
            throw new PeerCertificateException(PEM_TOO_LARGE);
        }

        String normalizedCn = extractSingleCommonName(certificate);
        if (!normalizedCn.equals(normalizeUid(expectedUid))) {
            throw new PeerCertificateException(CERT_CN_MISMATCH);
        }

        String canonicalSerial = canonicalSerial(certificate);
        String fingerprint = sha256Hex(canonicalDer);

        return new VerifiedPeerCertificate(canonicalDer, normalizedCn, canonicalSerial, fingerprint);
    }

    private static byte[] decodeBase64(String base64Body) {
        String compact = base64Body.replaceAll("\\s", "");
        try {
            return Base64.getDecoder().decode(compact);
        } catch (IllegalArgumentException e) {
            throw new PeerCertificateException(PEM_MALFORMED, e);
        }
    }

    private static X509Certificate parseSingleCertificate(byte[] der) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Collection<? extends java.security.cert.Certificate> certificates =
                    factory.generateCertificates(new ByteArrayInputStream(der));
            if (certificates.size() != 1) {
                throw new PeerCertificateException(PEM_MULTIPLE_CERTIFICATES);
            }
            java.security.cert.Certificate certificate = certificates.iterator().next();
            if (!(certificate instanceof X509Certificate x509Certificate)) {
                throw new PeerCertificateException(PEM_MALFORMED);
            }
            return x509Certificate;
        } catch (PeerCertificateException e) {
            throw e;
        } catch (Exception e) {
            throw new PeerCertificateException(PEM_MALFORMED, e);
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
            throw new PeerCertificateException(CERT_CN_MISSING, e);
        }
        if (commonNames.isEmpty()) {
            throw new PeerCertificateException(CERT_CN_MISSING);
        }
        // 多个 CN 无法确定权威身份，fail-closed
        if (commonNames.size() > 1) {
            throw new PeerCertificateException(CERT_CN_MISMATCH);
        }
        String normalized = normalizeUid(commonNames.get(0));
        if (normalized.isEmpty()) {
            throw new PeerCertificateException(CERT_CN_MISSING);
        }
        return normalized;
    }

    private static String canonicalSerial(X509Certificate certificate) {
        BigInteger serial = certificate.getSerialNumber();
        if (serial == null) {
            throw new PeerCertificateException(CERT_SERIAL_UNRESOLVED);
        }
        // 统一为无符号 canonical hex（小写、无前导零），不受 DER 符号位影响
        String canonical = new BigInteger(1, serial.toByteArray()).toString(16);
        if (canonical.isEmpty()) {
            throw new PeerCertificateException(CERT_SERIAL_UNRESOLVED);
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
            throw new PeerCertificateException(PEM_MALFORMED, e);
        }
    }

    private static String normalizeUid(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
