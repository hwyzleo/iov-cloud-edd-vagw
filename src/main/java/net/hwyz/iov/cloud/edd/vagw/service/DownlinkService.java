package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import net.hwyz.iov.cloud.edd.vagw.model.dto.DownlinkCommandResponse;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DownlinkService {

    private final SessionService sessionService;
    private final RouteService routeService;
    private final MqttClientManager mqttClientManager;

    public DownlinkCommandResponse sendCommand(String vin, String service, byte[] payload, String msgId, Long ttl) {
        // Check online status
        if (!sessionService.isOnline(vin)) {
            log.warn("Downlink rejected: vehicle offline, vin={}", vin);
            return DownlinkCommandResponse.builder()
                    .accepted(false)
                    .msgId(msgId)
                    .reason("Vehicle offline")
                    .errorCode(ErrorCode.VEHICLE_OFFLINE.getCode())
                    .build();
        }

        // Check route exists
        if (!routeService.hasRoute(service)) {
            log.warn("Downlink rejected: no route for service={}, vin={}", service, vin);
            return DownlinkCommandResponse.builder()
                    .accepted(false)
                    .msgId(msgId)
                    .reason("Unknown service: " + service)
                    .errorCode(ErrorCode.ROUTE_UNAVAILABLE.getCode())
                    .build();
        }

        // Build Envelope
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId(msgId != null ? msgId : UUID.randomUUID().toString())
                .setVin(vin)
                .setService(service)
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setTs(Instant.now().toEpochMilli())
                .setTtlMs(ttl != null ? ttl : 0)
                .setCompression(EnvelopeProto.Compression.COMPRESSION_NONE)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        // Publish to downlink topic
        String topic = "vehicle/" + vin + "/down/" + service;
        try {
            mqttClientManager.publish(topic, envelope.toByteArray(), 1);
            log.info("Downlink sent: vin={}, service={}, msgId={}", vin, service, envelope.getMsgId());

            return DownlinkCommandResponse.builder()
                    .accepted(true)
                    .msgId(envelope.getMsgId())
                    .build();
        } catch (MqttException e) {
            log.error("Downlink publish failed: vin={}, service={}", vin, service, e);
            return DownlinkCommandResponse.builder()
                    .accepted(false)
                    .msgId(msgId)
                    .reason("Publish failed: " + e.getMessage())
                    .errorCode(ErrorCode.ROUTE_UNAVAILABLE.getCode())
                    .build();
        }
    }
}
