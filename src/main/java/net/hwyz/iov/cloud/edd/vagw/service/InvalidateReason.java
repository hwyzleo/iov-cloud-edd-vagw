package net.hwyz.iov.cloud.edd.vagw.service;

/**
 * 绑定失效原因（EDD-VAGW-DSN-CR-007 §5）。
 * <p>
 * 换件、解绑、身份不一致、绑定版本变化或会话断开时，旧双向 key 原子失效。
 * </p>
 */
public enum InvalidateReason {
    /** 换件 / 重新绑定 */
    REBIND,
    /** 解绑 */
    UNBIND,
    /** VIAM 返回更高 bindingVersion */
    VERSION_CHANGED,
    /** 身份不一致（Envelope device_id 与当前 hsmUid 不符） */
    IDENTITY_MISMATCH,
    /** 会话断开 */
    SESSION_END,
    /** 安全封禁（SEC-VIAM-DSN-CR-001 / G-3 访问失效 BLOCK） */
    SECURITY_BLOCKED,
    /** 退役（G-3 访问失效 RETIRE） */
    DEVICE_RETIRED,
    /** 证书/密钥吊销（G-3 访问失效 REVOKE） */
    CERT_REVOKED
}
