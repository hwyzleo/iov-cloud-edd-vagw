package net.hwyz.iov.cloud.edd.vagw.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.adapter.kafka.GatewayDeliveryStatusProducer;
import org.springframework.stereotype.Service;
import vehicle.common.v1.Envelope;
import vagw.v1.Delivery;

/**
 * 技术投递结果服务（EDD-VAGW-DSN-CR-006 §8）。
 * <p>
 * 将 VAGW 侧不可投递状态映射为 proto-vagw 生成类并生产到 iov.vagw.delivery.fota；
 * 应用层只填充生成类，不保存 delivery.proto、wire DTO、Outcome 别名或 JSON fallback。
 * OUTCOME_ACCEPTED 仅表示 VAGW／MQTT 技术接管，OUTCOME_UNKNOWN 不得提升为成功。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GatewayDeliveryService {

    private static final String STAGE_DOWNLINK_RECEIVED = "DOWNLINK_RECEIVED";

    private final GatewayDeliveryStatusProducer producer;

    /**
     * 生产技术投递 REJECTED（Key=VIN）。
     *
     * @return 原 FOTA 传输消息 message_id
     */
    public String produceRejected(Envelope.VehicleMessageEnvelope envelope, String vin,
                                  DeliveryReason reason) {
        Delivery.GatewayDeliveryStatus.Builder b = Delivery.GatewayDeliveryStatus.newBuilder()
                .setOriginalMessageId(envelope.getMessageId())
                .setVin(vin)
                .setStage(STAGE_DOWNLINK_RECEIVED)
                .setOutcome(Delivery.Outcome.OUTCOME_REJECTED)
                .setReason(reason.name())
                .setRetryable(reason.isRetryable())
                .setOccurredAtMs(System.currentTimeMillis());
        if (envelope.hasCorrelationId()) {
            b.setCorrelationId(envelope.getCorrelationId());
        }
        if (reason.getRetryAfterMs() != null) {
            b.setRetryAfterMs(reason.getRetryAfterMs());
        }
        return producer.produce(b.build(), vin);
    }
}
