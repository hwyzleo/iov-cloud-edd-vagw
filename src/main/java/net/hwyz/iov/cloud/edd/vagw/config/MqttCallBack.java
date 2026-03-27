package net.hwyz.iov.cloud.edd.vagw.config;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Map;

/**
 * MQTT消息回调
 *
 * @author hwyz_leo
 */
@Slf4j
public class MqttCallBack implements MqttCallback {

    private RsmsProducer rsmsProducer;
    private TboxEventProducer tboxEventProducer;

    public MqttCallBack() {
    }

    public MqttCallBack(RsmsProducer rsmsProducer, TboxEventProducer tboxEventProducer) {
        this.rsmsProducer = rsmsProducer;
        this.tboxEventProducer = tboxEventProducer;
    }

    /**
     * 客户端断开连接的回调
     */
    @Override
    public void connectionLost(Throwable throwable) {
        logger.error("出现异常[{}]与服务器断开连接", throwable.getMessage(), throwable);
    }

    /**
     * 消息到达的回调
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        logger.debug("接收消息主题[{}]消息大小[{}]QOS[{}]", topic, message.getPayload().length, message.getQos());
        // 国标消息单独处理
        if (topic.endsWith("RSMS")) {
            String vin = topic.replaceAll("^.*UP/(.*?)/RSMS$", "$1").toUpperCase();
            rsmsProducer.send(vin, message.getPayload());
            return;
        }
        String eventStr = new String(message.getPayload());
        if (tboxEventProducer != null) {
            JSONObject jsonObject = JSONUtil.parseObj(eventStr);
            String vin = jsonObject.getStr("vin");
            if (StrUtil.isNotBlank(vin)) {
                tboxEventProducer.send(vin, new String(message.getPayload()));
            } else {
                logger.warn("TBOX事件[{}]车辆为空，不进行处理", eventStr);
            }
        }
    }

    /**
     * 消息发布成功的回调
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        logger.debug("消息发布成功");
        if (tboxEventProducer != null) {
            Map<String, Object> cmdMap = TboxCmdConsumer.cmdMapping.get(iMqttDeliveryToken.getMessageId());
            if (cmdMap != null) {
                cmdMap.put("type", "CMD_ACK");
                cmdMap.put("ackTime", System.currentTimeMillis());
                tboxEventProducer.send(String.valueOf(cmdMap.get("vin")), cmdMap);
            }
        }
    }
}
