package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import com.google.protobuf.ByteString;
import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

import java.util.UUID;

/**
 * keyprov 下行 Envelope 工厂（EDD-VAGW-DSN-CR-011 §3.6）。
 * <p>
 * 固定 service=security.keyprov、protocol_version=keyprov-v1、message_kind=RESPONSE、
 * device_id=认证 hsmUid；保持原 request_id，生成唯一 message_id，correlation_id 精确指向
 * 对应上行 message_id；expire_at_ms 使用 VMD 返回 TTL。VAGW 不修改 VMD 返回的 payload 内容。
 * </p>
 */
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovEnvelopeFactory {

    private final KeyprovProperties properties;

    public Envelope.VehicleMessageEnvelope buildDownlink(BrokerAuthenticatedPublishContext context,
                                                         Envelope.VehicleMessageEnvelope uplink,
                                                         String payloadType,
                                                         byte[] canonicalPayload,
                                                         Long expireAtMs) {
        Envelope.VehicleMessageEnvelope.Builder builder = Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId(uplink.getRequestId())
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion(properties.getProtocolVersion())
                .setDeviceId(context.hsmUid())
                .setMessageId(UUID.randomUUID().toString())
                .setCorrelationId(uplink.getMessageId())
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_RESPONSE)
                .setService(properties.getService())
                .setPayloadType(payloadType)
                .setPayload(ByteString.copyFrom(canonicalPayload));
        if (expireAtMs != null && expireAtMs > 0) {
            builder.setExpireAtMs(expireAtMs);
        }
        return builder.build();
    }
}
