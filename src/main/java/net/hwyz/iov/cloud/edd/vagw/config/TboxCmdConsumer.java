package net.hwyz.iov.cloud.edd.vagw.config;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TBOX指令消息消费者
 *
 * @author hwyz_leo
 */
@Slf4j
@Component
@RefreshScope
@RequiredArgsConstructor
public class TboxCmdConsumer {

    private final KafkaProperties properties;
    private final MqttConfig mqttProvider;

    @Value("${spring.kafka.consumer.reactive-concurrency:5}")
    private Integer concurrency;

    private final String TOPIC_TBOX_VAGW_CMD = "tbox-vagw-cmd";

    /**
     * 指令映射表
     * key: MQTT消息ID
     * value: 指令
     */
    protected static ConcurrentHashMap<Integer, Map<String, Object>> cmdMapping = new ConcurrentHashMap<>();

    /**
     * 消费TBOX服务指令消息
     */
    @PostConstruct
    public void consume() {
        ReceiverOptions<byte[], byte[]> options = ReceiverOptions.create(properties.buildConsumerProperties());
        options = options.subscription(Collections.singleton(TOPIC_TBOX_VAGW_CMD));
        logger.info("开始监听TBOX服务[{}]指令消息", options.consumerProperties().get("bootstrap.servers"));
        new ReactiveKafkaConsumerTemplate<>(options)
                .receiveAutoAck()
                .flatMap(record -> {
                    String vin = null;
                    try {
                        vin = new String(record.key());
                        String cmdJson = new String(record.value());
                        if (StrUtil.isNotBlank(vin)) {
                            logger.debug("收到车辆[{}]指令消息[{}]", vin, cmdJson);
                            JSONObject cmd = JSONUtil.parseObj(cmdJson);
                            Integer msgId = mqttProvider.publish(true, getTopic(vin, cmd.getStr("type")), cmdJson);
                            if (msgId != null) {
                                Map<String, Object> map = new HashMap<>();
                                map.put("vin", vin);
                                map.put("cmdId", cmd.getStr("cmdId"));
                                cmdMapping.put(msgId, map);
                            }
                        } else {
                            logger.warn("收到缺失VIN的异常指令消息[{}]", cmdJson);
                        }
                    } catch (Exception e) {
                        logger.error("消费车辆[{}]指令消息异常", vin, e);
                    }
                    return Mono.empty();
                }, concurrency)
                .doOnError(throwable -> {
                    logger.error("消费车辆指令消息消息异常", throwable);
                })
                .subscribe();
    }

    /**
     * 组装Topic
     *
     * @param vin     车架号
     * @param cmdType 指令类型
     * @return 下行Topic
     */
    private String getTopic(String vin, String cmdType) {
        return "DOWN/" + vin.toUpperCase() + "/" + cmdType;
    }

}
