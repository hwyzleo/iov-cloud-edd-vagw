package net.hwyz.iov.cloud.edd.vagw.mqtt;

/**
 * VAGW 上行订阅入口 Adapter 抽象（EDD-VAGW-DSN-CR-015 §2/§4）。
 * <p>
 * 统一共享组 {@code vagw} 只表达同一部署单元内实例间的竞争消费，不合并业务链路：
 * 每个受治理 service 保留独立入口 Adapter、独立协议校验、独立线程/容量与独立下游路由。
 * 本接口只提供 service/topic segment 元数据与消息入口，不承载任何 business 状态机。
 * </p>
 */
public interface UplinkSubscriptionHandler {

    /** 业务 service 名（metrics 标签，例如 fota / security.keyprov）。 */
    String service();

    /** MQTT Topic 中的 service segment（例如 fota / keyprov）。 */
    String topicSegment();

    /** 处理一条共享订阅投递的上行消息。 */
    void handleUplink(String topic, byte[] payload);
}
