package net.hwyz.iov.cloud.edd.vagw.model.deviceonline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * 准入会话快照（EDD-VAGW-DSN-CR-012 §3.1，证据字段由 EDD-VAGW-DSN-CR-019 补充）。
 * <p>
 * {@code /mqtt/auth} 获得 VIAM ALLOW 后写入的不可变快照；{@code client.connected} 只允许关联
 * 到本快照（hsmUid 来自归一化 peer_cert_cn，VIN 来自 VIAM ALLOW）。快照仅供本次连接
 * generation 使用，不跨重连复用；DENY、超时、身份不一致或过期快照不得匹配 Hook。
 * {@code admissionDecisionId} 为 VIAM 准入裁决ID（FR-019-04 可信证据，ALLOW 时必填），
 * 进入 DEVICE_ONLINE 事件供 VIAM 校验；旧 Redis 记录缺字段反序列化为 null，
 * 不得被当作可信成功登录证据（VIAM 证据门禁拒绝/隔离）。
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AdmissionSessionContext(
        String hsmUid,
        String vin,
        String certSerial,
        String clientId,
        String sourceIp,
        Integer protoVer,
        long connectionGeneration,
        Instant admittedAt,
        Instant expiresAt,
        String admissionDecisionId,
        Long bindingVersion,
        long securityVersion) {
}
