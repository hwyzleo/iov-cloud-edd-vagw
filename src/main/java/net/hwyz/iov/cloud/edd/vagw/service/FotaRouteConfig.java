package net.hwyz.iov.cloud.edd.vagw.service;

/**
 * FOTA 路由配置（EDD-VAGW-DSN-CR-005）
 * <p>
 * VAGW 作为车云接入契约 owner，固定 FOTA 的 MQTT/Kafka 物理 Topic、DLQ 与消费组；
 * 具体 OTA 业务由 Envelope messageType 区分，不按 messageType 拆物理 Topic。
 * </p>
 */
public final class FotaRouteConfig {

    public static final String SERVICE = "fota";
    public static final String MQTT_UP_TEMPLATE = "vehicle/{device_sn}/up/fota";
    public static final String MQTT_DOWN_TEMPLATE = "vehicle/{device_sn}/down/fota";
    public static final String KAFKA_UP_TOPIC = "iov.vagw.up.fota";
    public static final String KAFKA_DOWN_TOPIC = "iov.vagw.down.fota";
    public static final String UP_DLQ_TOPIC = "iov.vagw.up.fota.dlq";
    public static final String DOWN_DLQ_TOPIC = "iov.vagw.down.fota.dlq";
    public static final String KAFKA_KEY = "vin";
    public static final int QOS = 1;
    public static final String DOWNLINK_CONSUMER_GROUP = "edd-vagw-fota-downlink";

    public static final RouteEntry ROUTE = new RouteEntry(
            SERVICE,
            MQTT_UP_TEMPLATE,
            MQTT_DOWN_TEMPLATE,
            KAFKA_UP_TOPIC,
            KAFKA_DOWN_TOPIC,
            UP_DLQ_TOPIC,
            DOWN_DLQ_TOPIC,
            KAFKA_KEY,
            QOS,
            true
    );

    /**
     * RouteEntry（对齐 EDD-VAGW-DSN-CR-005 §3）
     */
    public record RouteEntry(
            String service,
            String mqttUpTemplate,
            String mqttDownTemplate,
            String kafkaUpTopic,
            String kafkaDownTopic,
            String upDlqTopic,
            String downDlqTopic,
            String kafkaKey,
            int qos,
            boolean enabled
    ) {
        public String mqttDownTopic(String deviceSn) {
            return mqttDownTemplate.replace("{device_sn}", deviceSn);
        }
    }

    private FotaRouteConfig() {
    }
}
