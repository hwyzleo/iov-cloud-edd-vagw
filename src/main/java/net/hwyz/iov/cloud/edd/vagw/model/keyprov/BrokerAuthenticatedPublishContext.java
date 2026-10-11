package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

import java.time.Instant;

/**
 * EMSX 已认证发布上下文（EDD-VAGW-DSN-CR-011 §3.2）。
 * <p>
 * 任一字段均来源于 EMQX 已认证连接（证书 CN/serial）与 VAGW 会话注册表，不得由 MQTT payload、
 * username、clientId 或 Topic 外部参数自行声明。Topic hsmUid 仅用于与权威值交叉校验。
 * </p>
 *
 * @param source               上下文来源（当前 {@link KeyprovContextSource#ACL_SESSION_CORRELATED}）
 * @param hsmUid               归一化设备身份（证书 CN / Envelope.device_id / Topic device key）
 * @param clientId             传输会话标识（非身份来源）
 * @param certSerial           证书序列号（认证期由 /mqtt/auth 透传并写入会话快照，可能为 null）
 * @param certFingerprint      证书指纹（如可用，可能为 null）
 * @param connectionGeneration 连接代际（重连/会话替换递增，用于 generation guard）
 * @param brokerSessionId      broker 侧会话标识（当前等于 clientId）
 * @param receivedAt           接收时间
 */
public record BrokerAuthenticatedPublishContext(
        KeyprovContextSource source,
        String hsmUid,
        String clientId,
        String certSerial,
        String certFingerprint,
        long connectionGeneration,
        String brokerSessionId,
        Instant receivedAt) {
}
