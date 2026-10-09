package net.hwyz.iov.cloud.edd.vagw.infrastructure.route;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import org.springframework.stereotype.Component;

/**
 * 车辆消息路由目录（EDD-VAGW-DSN-CR-006 §3/§4 + EDD-VAGW-DSN-CR-008 §2）。
 * <p>
 * MQTT up/down/fota、Kafka 下行 ota.fota 与消费组保持不变；
 * VAGW 作为 Producer 的 4 个 FOTA Topic（上行业务 / 技术投递结果 / 上行 DLQ / 下行 DLQ）
 * 统一为 vagw.fota、vagw.fota.delivery、vagw.fota.dlq.up、vagw.fota.dlq.down，
 * 名称经 {@link VagwFotaTopicProperties} 环境配置注入，不在代码中硬编码旧名称；
 * fota Topic segment 映射为 service=vehicle.fota 与对应 Kafka route。
 * </p>
 * <p>
 * 约束：不得在此形成任意 Kafka／MQTT Topic 隧道；allowlist 拒绝 vehicle.ota、vehicle.ota.v1、
 * 未知 service、非法 PayloadType 与方向。
 * </p>
 */
@Component
@RequiredArgsConstructor
public class VehicleRouteCatalog {

    /** MQTT Topic segment 对应的业务 service（FOTA 固定） */
    public static final String FOTA_SERVICE = "vehicle.fota";
    /** MQTT Topic 的 service segment（vehicle/{device-key}/up/fota 中的 fota） */
    public static final String FOTA_TOPIC_SEGMENT = "fota";

    public static final String MQTT_UP_TEMPLATE = "vehicle/{device-key}/up/fota";
    public static final String MQTT_DOWN_TEMPLATE = "vehicle/{device-key}/down/fota";

    /**
     * FOTA 下行业务 Topic（VAGW 消费，ota.fota）。Producer 为 IOV-OTA。
     */
    public static final String KAFKA_DOWN_TOPIC = "ota.fota";

    /** Kafka 主 Topic Key 固定为 VIN（同 VIN 物理分区顺序） */
    public static final String KAFKA_KEY = "vin";

    public static final int QOS = 1;
    public static final String DOWNLINK_CONSUMER_GROUP = "edd-vagw-fota-downlink";

    private final VagwFotaTopicProperties fotaTopicProperties;

    /** FOTA 上行业务 Topic（VAGW Producer，默认 vagw.fota） */
    public String kafkaUpTopic() {
        return fotaTopicProperties.getUplinkTopic();
    }

    /** FOTA 技术投递结果 Topic（VAGW Producer，默认 vagw.fota.delivery） */
    public String kafkaDeliveryTopic() {
        return fotaTopicProperties.getDeliveryTopic();
    }

    /** FOTA 上行 DLQ（VAGW Producer，默认 vagw.fota.dlq.up） */
    public String upDlqTopic() {
        return fotaTopicProperties.getUplinkDlqTopic();
    }

    /** FOTA 下行 DLQ（VAGW Producer，默认 vagw.fota.dlq.down） */
    public String downDlqTopic() {
        return fotaTopicProperties.getDownlinkDlqTopic();
    }

    public RouteEntry fotaRoute() {
        return new RouteEntry(
                FOTA_TOPIC_SEGMENT,
                FOTA_SERVICE,
                MQTT_UP_TEMPLATE,
                MQTT_DOWN_TEMPLATE,
                kafkaUpTopic(),
                KAFKA_DOWN_TOPIC,
                upDlqTopic(),
                downDlqTopic(),
                KAFKA_KEY,
                QOS
        );
    }

    /**
     * 单条 route 定义（CR §3 物理 Topic 表）。
     */
    public record RouteEntry(
            String mqttTopicSegment,
            String service,
            String mqttUpTemplate,
            String mqttDownTemplate,
            String kafkaUpTopic,
            String kafkaDownTopic,
            String upDlqTopic,
            String downDlqTopic,
            String kafkaKey,
            int qos
    ) {
        public String mqttDownTopic(String deviceKey) {
            return mqttDownTemplate.replace("{device-key}", deviceKey);
        }

        public String mqttUpTopic(String deviceKey) {
            return mqttUpTemplate.replace("{device-key}", deviceKey);
        }
    }
}
