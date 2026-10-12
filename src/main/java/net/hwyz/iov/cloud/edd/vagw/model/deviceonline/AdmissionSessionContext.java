package net.hwyz.iov.cloud.edd.vagw.model.deviceonline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * 准入会话快照（EDD-VAGW-DSN-CR-012 §3.1）。
 * <p>
 * {@code /mqtt/auth} 获得 TSP ALLOW 后写入的不可变快照；{@code client.connected} 只允许关联
 * 到本快照（hsmUid 来自归一化 peer_cert_cn，VIN 来自 TSP ALLOW）。快照仅供本次连接
 * generation 使用，不跨重连复用；DENY、超时、身份不一致或过期快照不得匹配 Hook。
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
        Instant expiresAt) {
}
