package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.kafka.UplinkKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.kms.KmsKeyProvClient;
import net.hwyz.iov.cloud.edd.vagw.kms.KmsKeyProvException;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import net.hwyz.iov.cloud.edd.vagw.proto.KeyProvProto;
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
    private final KmsKeyProvClient kmsKeyProvClient;
    private final MqttClientManager mqttClientManager;

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

        // Handle keyprov specially (synchronous flow, not Kafka)
        if ("keyprov".equals(service)) {
            return processKeyProv(envelope, connectionDeviceSn);
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

    /**
     * Process keyprov uplink message
     */
    private ProcessResult processKeyProv(EnvelopeProto.Envelope envelope, String connectionDeviceSn) {
        // Validate device_sn consistency
        if (!envelope.getDeviceSn().equalsIgnoreCase(connectionDeviceSn)) {
            log.warn("Keyprov device_sn mismatch: envelope={}, connection={}", envelope.getDeviceSn(), connectionDeviceSn);
            return ProcessResult.fail(ErrorCode.IDENTITY_MISMATCH, "device_sn does not match connection identity");
        }

        // Parse keyprov request payload
        KeyProvProto.KeyProvRequest keyProvRequest;
        try {
            keyProvRequest = KeyProvProto.KeyProvRequest.parseFrom(envelope.getPayload());
        } catch (Exception e) {
            log.warn("Failed to parse KeyProvRequest: {}", e.getMessage());
            return ProcessResult.fail(ErrorCode.PAYLOAD_DECODE_FAILED, "KeyProvRequest parse failed");
        }

        // Call KMS to get wrapped key
        KmsKeyProvClient.KeyProvIssueResult issueResult;
        try {
            KmsKeyProvClient.KeyProvIssueRequest issueRequest = new KmsKeyProvClient.KeyProvIssueRequest(
                    envelope.getDeviceSn(),
                    null, // certSerial - may need to get from connection context
                    keyProvRequest.getBizDomain(),
                    keyProvRequest.getUsage(),
                    envelope.getPayload().toByteArray()
            );
            issueResult = kmsKeyProvClient.issue(issueRequest);
        } catch (KmsKeyProvException e) {
            log.error("KMS keyprov failed for device_sn={}: {}", envelope.getDeviceSn(), e.getMessage());
            // Send failure response to vehicle
            sendKeyProvFailureResponse(envelope, ErrorCode.DEPENDENCY_UNAVAILABLE);
            return ProcessResult.fail(ErrorCode.DEPENDENCY_UNAVAILABLE, "KMS keyprov failed");
        }

        // Build keyprov response
        KeyProvProto.KeyProvResponse keyProvResponse = KeyProvProto.KeyProvResponse.newBuilder()
                .setResultCode(0)
                .setWrappedKey(com.google.protobuf.ByteString.copyFrom(issueResult.wrappedKey()))
                .setKeyId(issueResult.keyId())
                .setKeyVersion(issueResult.keyVersion())
                .setAlg(issueResult.alg())
                .setValidUntil(issueResult.validUntil())
                .setKdfParams(com.google.protobuf.ByteString.copyFrom(issueResult.kdfParams()))
                .build();

        // Build downlink envelope
        EnvelopeProto.Envelope downlinkEnvelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(envelope.getVer())
                .setMsgId(envelope.getMsgId())
                .setDeviceSn(envelope.getDeviceSn())
                .setService("keyprov")
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setTs(Instant.now().toEpochMilli())
                .setSeq(envelope.getSeq())
                .setTtlMs(envelope.getTtlMs())
                .setCompression(EnvelopeProto.Compression.COMPRESSION_NONE)
                .setPayload(com.google.protobuf.ByteString.copyFrom(keyProvResponse.toByteArray()))
                .build();

        // Publish down/keyprov message
        String downTopic = "vehicle/" + envelope.getDeviceSn() + "/down/keyprov";
        try {
            mqttClientManager.publish(downTopic, downlinkEnvelope.toByteArray(), 1);
            log.info("Keyprov response published: deviceSn={}, topic={}", envelope.getDeviceSn(), downTopic);
        } catch (Exception e) {
            log.error("Failed to publish keyprov response: deviceSn={}", envelope.getDeviceSn(), e);
            return ProcessResult.fail(ErrorCode.ROUTE_UNAVAILABLE, "Failed to publish keyprov response");
        }

        return ProcessResult.success();
    }

    /**
     * Send keyprov failure response to vehicle
     */
    private void sendKeyProvFailureResponse(EnvelopeProto.Envelope envelope, ErrorCode errorCode) {
        KeyProvProto.KeyProvResponse failureResponse = KeyProvProto.KeyProvResponse.newBuilder()
                .setResultCode(errorCode.getCode())
                .build();

        EnvelopeProto.Envelope failureEnvelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(envelope.getVer())
                .setMsgId(envelope.getMsgId())
                .setDeviceSn(envelope.getDeviceSn())
                .setService("keyprov")
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setTs(Instant.now().toEpochMilli())
                .setSeq(envelope.getSeq())
                .setTtlMs(envelope.getTtlMs())
                .setCompression(EnvelopeProto.Compression.COMPRESSION_NONE)
                .setPayload(com.google.protobuf.ByteString.copyFrom(failureResponse.toByteArray()))
                .build();

        String downTopic = "vehicle/" + envelope.getDeviceSn() + "/down/keyprov";
        try {
            mqttClientManager.publish(downTopic, failureEnvelope.toByteArray(), 1);
            log.info("Keyprov failure response published: deviceSn={}, errorCode={}", envelope.getDeviceSn(), errorCode);
        } catch (Exception e) {
            log.error("Failed to publish keyprov failure response: deviceSn={}", envelope.getDeviceSn(), e);
        }
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
