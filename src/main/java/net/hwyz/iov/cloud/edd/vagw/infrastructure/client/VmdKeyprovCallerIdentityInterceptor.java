package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Target;
import net.hwyz.iov.cloud.framework.common.constant.CustomHeaders;
import net.hwyz.iov.cloud.framework.common.constant.ServiceNameConstants;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * VAGW → VMD Keyprov 调用方服务身份透传拦截器（VMD-DSN-CR-059 §4/§6，EDD-VAGW-DSN-CR-016 §2.4）。
 * <p>
 * VMD 以服务认证上下文确认受信调用方：{@code KeyProvisioningCallerResolver} 从入站
 * {@code X-Client-Id} 头解析 service identity，并与 {@code allowed-callers}（默认 {@code VAGW}）
 * 精确比对，缺失即 fail-closed 返回 806090。
 * </p>
 * <p>
 * 与用户请求链路不同，Keyprov relay 运行在专属 {@code KeyprovExecutor} 线程，不存在入站
 * Servlet 请求，框架 {@code FeignRequestInterceptor} 无从透传身份，因此必须在此显式附加
 * VAGW 服务身份。拦截器仅作用于官方 VMD 服务（{@link ServiceNameConstants#EDD_VMD}）的 Feign
 * 调用，不影响 TSP 等其他客户端；不定义第二套 {@code @FeignClient}、URL 或服务名常量。
 * </p>
 */
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
public class VmdKeyprovCallerIdentityInterceptor implements RequestInterceptor {

    /**
     * 受信调用方服务身份，与 VMD {@code vmd.business-key.provisioning.keyprov.allowed-callers}
     * 默认值 {@code VAGW} 一致（VMD 侧大小写不敏感比对）。
     */
    static final String VAGW_CALLER_IDENTITY = "VAGW";

    @Override
    public void apply(RequestTemplate template) {
        Target<?> target = template.feignTarget();
        if (target != null && ServiceNameConstants.EDD_VMD.equals(target.name())) {
            template.header(CustomHeaders.CLIENT_ID, VAGW_CALLER_IDENTITY);
        }
    }
}
