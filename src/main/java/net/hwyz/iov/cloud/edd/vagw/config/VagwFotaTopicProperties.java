package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * VAGW FOTA Kafka 物理 Topic 环境配置（EDD-VAGW-DSN-CR-008 §2）。
 * <p>
 * VAGW 作为 Producer 的 4 个 FOTA Topic 名称唯一配置源，由 {@code VehicleRouteCatalog}
 * 与 {@code VagwFotaTopicDefinitionProvider} 共同读取，生产环境经 Nacos 覆盖；
 * 默认值为 Kafka Topic 目录中的规范名称（vagw.fota / vagw.fota.delivery /
 * vagw.fota.dlq.up / vagw.fota.dlq.down）。
 * </p>
 * <p>
 * 不包含 FOTA 下行业务 Topic：其 Producer 为 IOV-OTA，不在本 CR 范围内。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.kafka.fota")
public class VagwFotaTopicProperties {

    /** FOTA 上行业务 Topic（VAGW Producer） */
    private String uplinkTopic = "vagw.fota";

    /** FOTA 技术投递结果 Topic（VAGW Producer） */
    private String deliveryTopic = "vagw.fota.delivery";

    /** FOTA 上行 DLQ（VAGW Producer） */
    private String uplinkDlqTopic = "vagw.fota.dlq.up";

    /** FOTA 下行 DLQ（VAGW Producer） */
    private String downlinkDlqTopic = "vagw.fota.dlq.down";
}
