package net.hwyz.iov.cloud.edd.vagw.config;

import cn.hutool.core.util.RandomUtil;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * MQTT生产配置
 *
 * @author hwyz_leo
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class MqttConfig {

    @Value("${spring.mqtt.username}")
    private String username;

    @Value("${spring.mqtt.password}")
    private String password;

    @Value("${spring.mqtt.url}")
    private String hostUrl;

    @Value("${spring.mqtt.client.id}")
    private String clientId;

    /**
     * 客户端对象
     */
    private MqttClient client;

    private final RsmsProducer rsmsProducer;
    private final TboxEventProducer tboxEventProducer;

    /**
     * 在bean初始化后连接到服务器
     */
    @PostConstruct
    public void init() {
        connect();
    }

    /**
     * 客户端连接服务端
     */
    public void connect() {
        String randomClientId = clientId + "-" + RandomUtil.randomString(5);
        logger.info("客户端[{}]开始连接MQTT服务器[{}]", randomClientId, hostUrl);
        try {
            client = new MqttClient(hostUrl, randomClientId, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            // 是否清空session，设置false表示服务器会保留客户端的连接记录（订阅主题，qos）
            // 客户端重连之后能获取到服务器在客户端断开连接期间推送的消息
            // 设置为true表示每次连接服务器都是以新的身份
            options.setCleanSession(true);
            options.setUserName(username);
            options.setPassword(password.toCharArray());
            //设置超时时间，单位为秒
            options.setConnectionTimeout(60);
            //设置心跳时间 单位为秒，表示服务器每隔 1.5*20秒的时间向客户端发送心跳判断客户端是否在线
            options.setKeepAliveInterval(20);
            //设置遗嘱消息的话题，若客户端和服务器之间的连接意外断开，服务器将发布客户端的遗嘱信息
            options.setWill("willTopic", (clientId + "与服务器断开连接").getBytes(), 0, false);
            client.setCallback(new MqttCallBack(rsmsProducer, tboxEventProducer));
            client.connect(options);
            int[] qos = {1, 1};
            String[] topics = {
                    "$queue/UP/+/FIND_VEHICLE",
                    "$queue/UP/+/RSMS"
            };
            client.subscribe(topics, qos);
        } catch (MqttException e) {
            logger.error("客户端[{}]连接MQTT服务器[{}]异常", randomClientId, hostUrl, e);
            if (client != null) {
                try {
                    client.close();
                    client.disconnect();
                } catch (MqttException ex) {
                }
            }
        }
    }

    /**
     * 发送MQTT消息
     *
     * @param retained 是否保留
     * @param topic    Topic
     * @param message  消息
     */
    public Integer publish(boolean retained, String topic, String message) {
        // 默认QOS质量1
        return publish(1, retained, topic, message);
    }

    /**
     * 发送MQTT消息
     *
     * @param qos      QOS级别
     * @param retained 是否保留
     * @param topic    Topic
     * @param message  消息
     */
    public Integer publish(int qos, boolean retained, String topic, String message) {
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setQos(qos);
        mqttMessage.setRetained(retained);
        mqttMessage.setPayload(message.getBytes());
        MqttTopic mqttTopic = client.getTopic(topic);
        //提供一种机制来跟踪消息的传递进度
        //用于在以非阻塞方式（在后台运行）执行发布是跟踪消息的传递进度
        MqttDeliveryToken token;
        try {
            //将指定消息发布到主题，但不等待消息传递完成，返回的token可用于跟踪消息的传递状态
            //一旦此方法干净地返回，消息就已被客户端接受发布，当连接可用，将在后台完成消息传递。
            token = mqttTopic.publish(mqttMessage);
            token.waitForCompletion();
            return token.getMessageId();
        } catch (MqttException e) {
            e.printStackTrace();
            return null;
        }
    }

}
