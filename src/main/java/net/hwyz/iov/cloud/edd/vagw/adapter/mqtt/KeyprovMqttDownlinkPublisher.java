package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * keyprov 下行发布器（EDD-VAGW-DSN-CR-011 §3.6）。
 * <p>
 * 以 QoS1 发布到 {@code vehicle/{hsm_uid}/down/keyprov}，不 retain、不持久离线；
 * PUBACK 只表示传输结果，不代表 TBOX 已导入密钥或已激活。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovMqttDownlinkPublisher {

    private static final String TOPIC_TEMPLATE = "vehicle/%s/down/keyprov";

    private final MqttClientManager mqttClientManager;
    private final KeyprovProperties properties;

    /**
     * @return 实际发布的 MQTT Topic
     */
    public String publish(String hsmUid, byte[] envelopeBytes) throws Exception {
        String topic = String.format(TOPIC_TEMPLATE, hsmUid);
        mqttClientManager.publish(topic, envelopeBytes, properties.getQos());
        return topic;
    }
}
