package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * VAGW MQTT 订阅配置（EDD-VAGW-DSN-CR-015 §3）。
 * <p>
 * 配置只保存原始业务 filter；运行时由 {@code VagwUplinkSubscriptionRegistry} 统一生成
 * {@code $share/${shared-group}/${filter}}。禁止在配置或各 Adapter 中出现 {@code $queue/}
 * 或自行拼接 group 前缀。{@code shared-group} 固定为部署单元级 {@code vagw}，不按业务
 * service 派生多个共享组。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.mqtt")
public class VagwMqttProperties {

    /** 统一上行共享订阅组；固定为部署单元名 {@code vagw}，不代表业务名。 */
    private String sharedGroup = "vagw";

    /** 受治理的上行订阅目录；service/filter 必须来自静态 allowlist。 */
    private List<Subscription> uplinkSubscriptions = new ArrayList<>(List.of(
            new Subscription("fota", "vehicle/+/up/fota", 1, true),
            new Subscription("security.keyprov", "vehicle/+/up/keyprov", 1, false)));

    /**
     * 单条上行订阅定义：只描述原始业务 filter 与启用条件。
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Subscription {

        /** 业务 service 名（allowlist：fota / security.keyprov）。 */
        private String service;

        /** 原始业务 filter（例如 vehicle/+/up/fota），不包含 {@code $share/} 前缀。 */
        private String filter;

        /** 请求 QoS（受治理固定 1）。 */
        private int qos = 1;

        /** 启用条件；与对应 capability/feature gate 保持一致。 */
        private boolean enabled;
    }
}
