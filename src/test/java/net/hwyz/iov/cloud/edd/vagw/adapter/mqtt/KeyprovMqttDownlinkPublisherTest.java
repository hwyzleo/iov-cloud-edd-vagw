package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * keyprov 下行发布器测试（EDD-VAGW-DSN-CR-011 §3.6）。
 */
@ExtendWith(MockitoExtension.class)
class KeyprovMqttDownlinkPublisherTest {

    @Mock
    private MqttClientManager mqttClientManager;

    @Test
    void publish_shouldUseDownKeyprovTopicAndQos1() throws Exception {
        KeyprovMqttDownlinkPublisher publisher =
                new KeyprovMqttDownlinkPublisher(mqttClientManager, new KeyprovProperties());

        String topic = publisher.publish("DEVICE001", new byte[]{1, 2, 3});

        assertEquals("vehicle/DEVICE001/down/keyprov", topic);
        verify(mqttClientManager).publish(eq("vehicle/DEVICE001/down/keyprov"), any(byte[].class), eq(1));
    }
}
