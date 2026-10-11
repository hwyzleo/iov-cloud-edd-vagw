package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import vehicle.common.v1.Envelope;

/**
 * VMD Key Provisioning Adapter SPI（EDD-VAGW-DSN-CR-011 §3.5）。
 * <p>
 * VAGW 校验 EMQX 已认证上下文与 canonical 协议后，把 Request/Ack/StatusRequest 的原 canonical
 * payload + Envelope 关联元数据 + 认证上下文转发 VMD，并接收对应 canonical 下行 payload 与非敏感
 * 路由结果。
 * </p>
 * <p>
 * 传输由平台统一 Service API 实现（VMD 正式 CR 发布后接入 Feign）；当前默认实现
 * {@link DisabledVmdKeyProvisioningClient} fail-closed。实现约束：
 * <ul>
 *   <li>payload 必须使用同一 proto-keyprov 生成类型/bytes，不得转换为私有 JSON wire；</li>
 *   <li>服务认证身份来自 VAGW workload identity，不接受请求体自报 caller；</li>
 *   <li>deadline 有界；请求已发送但结果未知时不得自动更换 requestId 重试；</li>
 *   <li>不向 VAGW 返回 KMS 内部 keyRef；</li>
 *   <li>Wrapped Key/parameters/importProof 仅在调用栈内短暂透传，不落任何持久化。</li>
 * </ul>
 * </p>
 */
public interface VmdKeyProvisioningClient {

    /**
     * 转发一条 canonical keyprov 上行并返回 VMD 结果。
     */
    VmdRelayResponse relay(VmdRelayRequest request);

    /**
     * relay 请求：认证上下文 + Envelope + canonical payload。
     * <p>
     * 注意：{@code canonicalPayload} 为敏感密文，字节数组默认 toString 不输出内容；
     * 严禁对其调用生成类的 toString()/JSON formatter 或写入日志。
     * </p>
     */
    record VmdRelayRequest(
            KeyprovContextSource source,
            BrokerAuthenticatedPublishContext context,
            Envelope.VehicleMessageEnvelope envelope,
            byte[] canonicalPayload) {

        /**
         * 安全摘要：绝不输出 Envelope/payload 内容（Envelope 默认 toString 会以 TextFormat
         * 展开 payload 字节）。只保留可用于审计的关联标识与类型。
         */
        @Override
        public String toString() {
            return "VmdRelayRequest{source=" + source
                    + ", hsmUid=" + (context == null ? null : context.hsmUid())
                    + ", messageId=" + (envelope == null ? null : envelope.getMessageId())
                    + ", requestId=" + (envelope == null ? null : envelope.getRequestId())
                    + ", payloadType=" + (envelope == null ? null : envelope.getPayloadType())
                    + ", payloadBytes=" + (canonicalPayload == null ? 0 : canonicalPayload.length)
                    + '}';
        }
    }
}
