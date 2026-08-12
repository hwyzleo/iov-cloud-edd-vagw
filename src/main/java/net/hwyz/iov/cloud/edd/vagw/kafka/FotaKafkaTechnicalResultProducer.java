package net.hwyz.iov.cloud.edd.vagw.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.FotaBridgeException;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.service.FotaRouteConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * FOTA 技术投递结果生产者，对齐 EDD-VAGW-DSN-CR-005 §7。
 * <p>
 * 当车辆离线、VIN 未绑定、权限拒绝、TTL 过期或 MQTT 发布失败时，向 iov.vagw.up.fota
 * 生产 ota.transport.delivery-rejected，携带原 messageId/correlationId、reasonCode、
 * retryable 和可选 retryAfterSec。VAGW 只产生技术投递结果，不伪造 OTA 业务回执。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FotaKafkaTechnicalResultProducer {

    private static final long SEND_TIMEOUT_SECONDS = 3;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 生产 delivery-rejected 到 iov.vagw.up.fota（Key=VIN）。
     *
     * @return 新生成的结果 messageId
     */
    public String produceDeliveryRejected(String vin, String deviceSn, String correlationId,
                                          String reasonCode, boolean retryable, Integer retryAfterSec) {
        String messageId = UUID.randomUUID().toString();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("reasonCode", reasonCode);
        payload.put("retryable", retryable);
        if (retryAfterSec != null) {
            payload.put("retryAfterSec", retryAfterSec);
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("messageType", "ota.transport.delivery-rejected");
        body.put("messageId", messageId);
        body.put("correlationId", correlationId);
        body.put("vin", vin);
        body.put("deviceId", deviceSn);
        body.put("payload", payload);

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(body);
            Message<byte[]> message = MessageBuilder
                    .withPayload(bytes)
                    .setHeader(KafkaHeaders.TOPIC, FotaRouteConfig.KAFKA_UP_TOPIC)
                    .setHeader(KafkaHeaders.KEY, vin)
                    .setHeader("service", "fota")
                    .setHeader("msg_type", "ota.transport.delivery-rejected")
                    .setHeader("message_id", messageId)
                    .setHeader("correlation_id", correlationId)
                    .build();
            kafkaTemplate.send(message).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("FOTA delivery-rejected produced: vin={}, reasonCode={}, correlationId={}, messageId={}",
                    LogMask.mask(vin), reasonCode, correlationId, messageId);
            return messageId;
        } catch (Exception e) {
            log.error("FOTA delivery-rejected produce failed: vin={}, reasonCode={}, correlationId={}",
                    LogMask.mask(vin), reasonCode, correlationId, e);
            throw new FotaBridgeException("Failed to produce delivery-rejected: " + reasonCode, e);
        }
    }
}
