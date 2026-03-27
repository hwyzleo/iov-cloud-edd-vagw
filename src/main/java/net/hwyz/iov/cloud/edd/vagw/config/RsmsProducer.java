package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;

/**
 * 国标消息生产者
 *
 * @author hwyz_leo
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RsmsProducer {

    private final ReactiveKafkaProducerTemplate<String, byte[]> bytesReactiveKafkaProducerTemplate;

    /**
     * 国标数据主题
     */
    private final String TOPIC_GB_DATA = "vagw-rsms-data";

    /**
     * 发送国标消息至国标服务
     *
     * @param vin  车架号
     * @param data 国标数据
     */
    public void send(String vin, byte[] data) {
        logger.debug("车辆[{}]发送国标消息至国标服务", vin);
        bytesReactiveKafkaProducerTemplate.send(TOPIC_GB_DATA, vin, data)
                .doOnError(throwable -> logger.error("车辆[{}]发送国标消息至国标服务异常[{}]", vin, throwable.getMessage()))
                .subscribe();
    }

}
