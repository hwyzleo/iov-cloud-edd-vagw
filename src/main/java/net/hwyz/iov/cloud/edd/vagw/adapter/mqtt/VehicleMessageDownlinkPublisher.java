package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import org.springframework.stereotype.Component;

/**
 * 车辆消息下行发布器（EDD-VAGW-DSN-CR-006 §7）。
 * <p>
 * 将原 Envelope bytes 以 QoS1 发布到 vehicle/{device-key}/down/fota；不重建 payload、
 * 不生成新 message_id、不改写关联链，也不把 PUBACK 解释为 FOTA 业务成功。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleMessageDownlinkPublisher {

    private final MqttClientManager mqttClientManager;

    public void publish(String topic, byte[] envelopeBytes) throws Exception {
        mqttClientManager.publish(topic, envelopeBytes, VehicleRouteCatalog.QOS);
    }
}
