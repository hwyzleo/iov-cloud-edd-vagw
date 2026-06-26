package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.kafka.UplinkKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class UplinkService {

    private final RouteService routeService;
    private final UplinkKafkaProducer kafkaProducer;

    public ProcessResult processUplink(byte[] payload, String connectionVin) {
        EnvelopeProto.Envelope envelope;
        try {
            envelope = EnvelopeProto.Envelope.parseFrom(payload);
        } catch (Exception e) {
            log.warn("Failed to parse Envelope: {}", e.getMessage());
            return ProcessResult.fail(ErrorCode.INVALID_ENVELOPE, "Envelope parse failed");
        }

        String envelopeVin = envelope.getVin();
        if (envelopeVin == null || envelopeVin.isBlank()) {
            log.warn("Envelope missing VIN");
            return ProcessResult.fail(ErrorCode.INVALID_ENVELOPE, "Missing VIN in envelope");
        }

        // Validate VIN matches connection identity
        if (!envelopeVin.equalsIgnoreCase(connectionVin)) {
            log.warn("VIN mismatch: envelope={}, connection={}", envelopeVin, connectionVin);
            return ProcessResult.fail(ErrorCode.VIN_MISMATCH, "VIN does not match connection identity");
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

        // Determine Kafka topic (append .ack for UP_ACK messages)
        String kafkaTopic = topic;
        if (envelope.getMsgType() == EnvelopeProto.MsgType.UP_ACK) {
            kafkaTopic = topic + ".ack";
        }

        // Send to Kafka
        String receivedAt = Instant.now().toString();
        kafkaProducer.send(kafkaTopic, envelopeVin, payload, service,
                envelope.getMsgType().name(), receivedAt, null);

        log.info("Uplink routed: vin={}, service={}, msgType={}, topic={}",
                envelopeVin, service, envelope.getMsgType(), kafkaTopic);

        return ProcessResult.success();
    }

    public record ProcessResult(boolean success, ErrorCode errorCode, String reason) {
        public static ProcessResult success() {
            return new ProcessResult(true, null, null);
        }

        public static ProcessResult fail(ErrorCode code, String reason) {
            return new ProcessResult(false, code, reason);
        }
    }
}
