package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import vehicle.common.v1.Envelope;

/**
 * VMD keyprov relay 命令（EDD-VAGW-DSN-CR-016 §2.4）。
 * <p>
 * VAGW 应用层 Port 的输入：认证会话上下文 + canonical Envelope + canonical payload。
 * {@code canonicalPayload} 为敏感密文，只允许在单次处理调用栈及其有界内存任务中短暂存在。
 * </p>
 */
public record VmdKeyProvisioningCommand(
        BrokerAuthenticatedPublishContext context,
        Envelope.VehicleMessageEnvelope envelope,
        byte[] canonicalPayload) {

    /**
     * 安全摘要：绝不输出 Envelope/payload 内容（Envelope 默认 toString 会展开 payload 字节）。
     */
    @Override
    public String toString() {
        return "VmdKeyProvisioningCommand{hsmUid=" + (context == null ? null : context.hsmUid())
                + ", messageId=" + (envelope == null ? null : envelope.getMessageId())
                + ", requestId=" + (envelope == null ? null : envelope.getRequestId())
                + ", payloadType=" + (envelope == null ? null : envelope.getPayloadType())
                + ", payloadBytes=" + (canonicalPayload == null ? 0 : canonicalPayload.length)
                + '}';
    }
}
