package net.hwyz.iov.cloud.edd.vagw.infrastructure.route;

import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 车辆消息路由目录测试（EDD-VAGW-DSN-CR-008 §2）。
 * <p>
 * 验证 VAGW 作为 Producer 的 4 个 FOTA Topic 默认名称与 CR-008 目标一致、
 * 环境配置可覆盖、RouteEntry 字段映射正确，且 FOTA 下行业务 Topic 为 ota.fota。
 * </p>
 */
@DisplayName("VehicleRouteCatalog 测试")
class VehicleRouteCatalogTest {

    private VehicleRouteCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new VehicleRouteCatalog(new VagwFotaTopicProperties());
    }

    @Test
    @DisplayName("Producer Topic 默认名与 CR-008 目标一致")
    void defaultProducerTopicNames() {
        assertEquals("vagw.fota", catalog.kafkaUpTopic());
        assertEquals("vagw.fota.delivery", catalog.kafkaDeliveryTopic());
        assertEquals("vagw.fota.dlq.up", catalog.upDlqTopic());
        assertEquals("vagw.fota.dlq.down", catalog.downDlqTopic());
    }

    @Test
    @DisplayName("FOTA 下行业务 Topic 为 ota.fota")
    void downTopicUnchanged() {
        assertEquals("ota.fota", VehicleRouteCatalog.KAFKA_DOWN_TOPIC);
    }

    @Test
    @DisplayName("环境配置覆盖后 RouteEntry 反映目标名称")
    void configuredTopicNamesReflectedInRoute() {
        VagwFotaTopicProperties props = new VagwFotaTopicProperties();
        props.setUplinkTopic("vagw.fota.prod");
        props.setDeliveryTopic("vagw.fota.delivery.prod");
        props.setUplinkDlqTopic("vagw.fota.dlq.up.prod");
        props.setDownlinkDlqTopic("vagw.fota.dlq.down.prod");
        VehicleRouteCatalog configured = new VehicleRouteCatalog(props);

        VehicleRouteCatalog.RouteEntry route = configured.fotaRoute();
        assertEquals("vagw.fota.prod", route.kafkaUpTopic());
        assertEquals("ota.fota", route.kafkaDownTopic());
        assertEquals("vagw.fota.dlq.up.prod", route.upDlqTopic());
        assertEquals("vagw.fota.dlq.down.prod", route.downDlqTopic());
        assertEquals("vehicle.fota", route.service());
        assertEquals("vin", route.kafkaKey());
    }

    @Test
    @DisplayName("MQTT 模板替换生成上下行 Topic")
    void mqttTopicTemplates() {
        VehicleRouteCatalog.RouteEntry route = catalog.fotaRoute();
        assertEquals("vehicle/DEVICE001/down/fota", route.mqttDownTopic("DEVICE001"));
        assertEquals("vehicle/DEVICE001/up/fota", route.mqttUpTopic("DEVICE001"));
    }
}
