package net.hwyz.iov.cloud.edd.vagw.model.security;

/**
 * 已验证叶子证书瞬时上下文（EDD-VAGW-DSN-CR-013 §4.2）。
 * <p>
 * 由 {@code PeerCertificateContextFactory} 从 EMQX {@code ${cert_pem}} 严格解析构造：
 * der / normalizedUid / canonicalSerial / sha256Fingerprint。该对象仅存在于本次认证调用栈内，
 * 原始 PEM、DER 与 fingerprint 不进入缓存、数据库、事件或普通日志；canonicalSerial 可沿用
 * EDD-VAGW-DSN-CR-012 既有 AdmissionSessionContext/AuthenticatedIdentityStore/DEVICE_ONLINE
 * 契约受控保存或传递，不新增副本。
 * </p>
 */
public record VerifiedPeerCertificate(
        byte[] der,
        String normalizedUid,
        String canonicalSerial,
        String sha256Fingerprint) {

    public VerifiedPeerCertificate {
        der = der == null ? null : der.clone();
    }

    @Override
    public byte[] der() {
        return der == null ? null : der.clone();
    }
}
