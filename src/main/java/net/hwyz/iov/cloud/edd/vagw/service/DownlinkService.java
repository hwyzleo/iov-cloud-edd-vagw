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
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DownlinkService {

    private final SessionService sessionService;
    private final RouteService routeService;
    private final MqttClientManager mqttClientManager;
    private final BindingService bindingService;

    /**
     * 发送下行指令
     * @param vin 车辆识别码（北向接口使用 VIN）
     * @param service 业务服务名
     * @param payload 业务负载
     * @param msgId 消息 ID
     * @param ttl 超时时间（毫秒）
     * @return 下发结果
     */
    public DownlinkCommandResponse sendCommand(String vin, String service, byte[] payload, String msgId, Long ttl) {
        // Resolve VIN → device_sn
        Optional<String> deviceSnOpt = bindingService.resolveDeviceSn(vin);
        if (deviceSnOpt.isEmpty()) {
            log.warn("Downlink rejected: VIN not bound to any device_sn, vin={}", vin);
            return DownlinkCommandResponse.builder()
                    .accepted(false)
                    .msgId(msgId)
                    .reason("VIN not bound to any device")
                    .errorCode(ErrorCode.VIN_UNAUTHORIZED.getCode())
                    .build();
        }

        String deviceSn = deviceSnOpt.get();

        // Check online status using device_sn
        if (!sessionService.isOnlineByDeviceSn(deviceSn)) {
            log.warn("Downlink rejected: vehicle offline, vin={}, deviceSn={}", vin, deviceSn);
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

        // Build Envelope with device_sn
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId(msgId != null ? msgId : UUID.randomUUID().toString())
                .setDeviceSn(deviceSn)
                .setService(service)
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setTs(Instant.now().toEpochMilli())
                .setTtlMs(ttl != null ? ttl.intValue() : 0)
                .setCompression(EnvelopeProto.Compression.COMPRESSION_NONE)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build();

        // Publish to downlink topic (using device_sn in topic)
        String topic = "vehicle/" + deviceSn + "/down/" + service;
        try {
            mqttClientManager.publish(topic, envelope.toByteArray(), 1);
            log.info("Downlink sent: vin={}, deviceSn={}, service={}, msgId={}", vin, deviceSn, service, envelope.getMsgId());

            return DownlinkCommandResponse.builder()
                    .accepted(true)
                    .msgId(envelope.getMsgId())
                    .build();
        } catch (MqttException e) {
            log.error("Downlink publish failed: vin={}, deviceSn={}, service={}", vin, deviceSn, service, e);
            return DownlinkCommandResponse.builder()
                    .accepted(false)
                    .msgId(msgId)
                    .reason("Publish failed: " + e.getMessage())
                    .errorCode(ErrorCode.ROUTE_UNAVAILABLE.getCode())
                    .build();
        }
    }
}
