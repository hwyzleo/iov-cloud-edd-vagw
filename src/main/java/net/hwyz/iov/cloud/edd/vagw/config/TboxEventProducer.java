package net.hwyz.iov.cloud.edd.vagw.config;

import cn.hutool.json.JSONUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * TBOX事件消息生产者
 *
 * @author hwyz_leo
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TboxEventProducer {

    private final ReactiveKafkaProducerTemplate<String, String> stringReactiveKafkaProducerTemplate;

    private final String TOPIC_VAGW_TBOX_VAGW_EVENT = "vagw-tbox-event";

    /**
     * 发送TBOX事件至TBOX服务
     *
     * @param vin   车架号
     * @param event TBOX事件
     */
    public void send(String vin, Map<String, Object> event) {
        String eventJson = JSONUtil.toJsonStr(event);
        send(vin, eventJson);
    }

    /**
     * 发送TBOX事件至TBOX服务
     *
     * @param vin       车架号
     * @param eventJson TBOX事件JSON
     */
    public void send(String vin, String eventJson) {
        logger.debug("车辆[{}]发送TBOX事件[{}]至TBOX服务", vin, eventJson);
        stringReactiveKafkaProducerTemplate.send(TOPIC_VAGW_TBOX_VAGW_EVENT, vin, eventJson)
                .doOnError(throwable -> logger.error("车辆[{}]发送TBOX事件[{}]至TBOX服务异常[{}]", vin, eventJson, throwable.getMessage()))
                .subscribe();
    }

}
