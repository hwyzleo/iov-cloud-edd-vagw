package net.hwyz.iov.cloud.edd.vagw.infrastructure.security;

import lombok.Getter;

/**
 * 叶子证书解析/校验失败（EDD-VAGW-DSN-CR-013 §4.2/§5）。
 * <p>
 * fail-closed 语义：任何异常均映射连接认证错误码 804016，不调用 TSP、不生成 ACL、不建立会话。
 * {@code outcome} 用于脱敏指标标签，不包含证书正文或完整身份。
 * </p>
 */
@Getter
public class PeerCertificateException extends RuntimeException {

    private final String outcome;

    public PeerCertificateException(String outcome) {
        super(outcome);
        this.outcome = outcome;
    }

    public PeerCertificateException(String outcome, Throwable cause) {
        super(outcome, cause);
        this.outcome = outcome;
    }
}
