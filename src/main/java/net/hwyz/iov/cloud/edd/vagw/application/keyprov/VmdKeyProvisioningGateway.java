package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

/**
 * VMD Key Provisioning 应用层 Port（EDD-VAGW-DSN-CR-016 §2.4）。
 * <p>
 * VAGW 应用层只依赖本 Port；其 Adapter {@code VmdKeyProvisioningGatewayAdapter} 直接委托
 * edd-vmd-api 官方 {@code VmdKeyProvisioningService} Feign 契约（request／ack／status 三端点），
 * 并完成认证上下文／canonical bytes 模型映射、VMD 806xxx／异常→VAGW 804010 reason 映射及
 * deadline、有限重试与熔断治理。
 * </p>
 * <p>
 * 本 Port 不是第二套 VMD wire 契约：不定义 URL、服务名常量或 wire fallback。
 * </p>
 */
public interface VmdKeyProvisioningGateway {

    /**
     * 在 Keyprov 专属有界执行资源内阻塞转发一条 canonical keyprov 上行并返回 VMD 结果。
     */
    VmdRelayResult relay(VmdKeyProvisioningCommand command);
}
