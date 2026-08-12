package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.kafka.FotaKafkaTechnicalResultProducer;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.springframework.stereotype.Service;

/**
 * FOTA 技术投递结果服务，对齐 EDD-VAGW-DSN-CR-005 §7。
 * <p>
 * 将 VAGW 侧的不可投递状态映射为 OTA 契约 reasonCode，并生产 delivery-rejected。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FotaDeliveryResultService {

    private final FotaKafkaTechnicalResultProducer technicalResultProducer;

    /**
     * 生产 ota.transport.delivery-rejected（Key=VIN）。
     *
     * @return 新生成的结果 messageId
     */
    public String produceDeliveryRejected(EnvelopeProto.Envelope envelope, String vin, String deviceSn,
                                          DeliveryReason reason) {
        return technicalResultProducer.produceDeliveryRejected(
                vin,
                deviceSn,
                envelope.getMsgId(),
                reason.name(),
                reason.isRetryable(),
                reason.getRetryAfterSec()
        );
    }
}
