package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.kafka.UplinkKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class UplinkService {

    private final RouteService routeService;
    private final UplinkKafkaProducer kafkaProducer;
    private final BindingService bindingService;

    /**
     * 处理上行消息
     * @param payload MQTT 消息负载（Envelope 字节）
     * @param connectionDeviceSn 连接身份 device_sn（来自 topic）
     * @return 处理结果
     */
    public ProcessResult processUplink(byte[] payload, String connectionDeviceSn) {
        EnvelopeProto.Envelope envelope;
        try {
            envelope = EnvelopeProto.Envelope.parseFrom(payload);
        } catch (Exception e) {
            log.warn("Failed to parse Envelope: {}", e.getMessage());
            return ProcessResult.fail(ErrorCode.INVALID_ENVELOPE, "Envelope parse failed");
        }

        String envelopeDeviceSn = envelope.getDeviceSn();
        if (envelopeDeviceSn == null || envelopeDeviceSn.isBlank()) {
            log.warn("Envelope missing device_sn");
            return ProcessResult.fail(ErrorCode.INVALID_ENVELOPE, "Missing device_sn in envelope");
        }

        // Validate device_sn matches connection identity
        if (!envelopeDeviceSn.equalsIgnoreCase(connectionDeviceSn)) {
            log.warn("device_sn mismatch: envelope={}, connection={}", envelopeDeviceSn, connectionDeviceSn);
            return ProcessResult.fail(ErrorCode.IDENTITY_MISMATCH, "device_sn does not match connection identity");
        }

        String service = envelope.getService();
        if (service == null || service.isBlank()) {
            log.warn("Envelope missing service");
            return ProcessResult.fail(ErrorCode.INVALID_ENVELOPE, "Missing service in envelope");
        }

        // Check route exists
        String topic = routeService.getTopic(service);
        if (topic == null) {
            log.warn("No route for service: {}", service);
            return ProcessResult.fail(ErrorCode.ROUTE_UNAVAILABLE, "No route for service: " + service);
        }

        // Resolve VIN for northbound enrichment
        Optional<String> vinOpt = bindingService.resolveVin(envelopeDeviceSn);
        String vin = vinOpt.orElse(null);

        // Determine Kafka topic (append .ack for UP_ACK messages)
        String kafkaTopic = topic;
        if (envelope.getMsgType() == EnvelopeProto.MsgType.UP_ACK) {
            kafkaTopic = topic + ".ack";
        }

        // Send to Kafka with device_sn as key (for ordering)
        // VIN is included in headers for northbound consumers
        String receivedAt = Instant.now().toString();
        kafkaProducer.send(kafkaTopic, envelopeDeviceSn, vin, payload, service,
                envelope.getMsgType().name(), receivedAt, null);

        log.info("Uplink routed: deviceSn={}, vin={}, service={}, msgType={}, topic={}",
                envelopeDeviceSn, vin, service, envelope.getMsgType(), kafkaTopic);

        return ProcessResult.success();
    }

    public record ProcessResult(boolean ok, ErrorCode errorCode, String reason) {
        public static ProcessResult success() {
            return new ProcessResult(true, null, null);
        }

        public static ProcessResult fail(ErrorCode code, String reason) {
            return new ProcessResult(false, code, reason);
        }
    }
}
