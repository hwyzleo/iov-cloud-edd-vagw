package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Target;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.framework.common.constant.CustomHeaders;
import net.hwyz.iov.cloud.framework.common.constant.ServiceNameConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * VAGW → VIAM 服务身份注入拦截器（SEC-VIAM-DSN-CR-001 / R1，G-4 平台内网信任链配套）。
 * <p>
 * 仅对官方 {@link ServiceNameConstants#SEC_VIAM} 服务的 Feign 调用注入
 * {@code X-Client-Id}（平台受控内网服务身份约定，默认 VAGW）。覆盖准入/身份解析/Keyprov/
 * 失效回执全部 VIAM 官方调用；Kafka 消费触发 receipt 时无外部 HTTP 请求同样注入。
 * 不定义第二套签名协议，不传播外部身份头，不全局污染其它 Feign 客户端。
 * 外部伪造由网关边界与受限网络可达性负责（CR-001 R1.2 部署核验），本层不宣称逐请求
 * 密码学验证。
 * </p>
 */
@Slf4j
@Component
public class ViamServiceClientIdInterceptor implements RequestInterceptor {

    @Value("${vagw.security.viam-client-id.client-id:VAGW}")
    private String clientId;

    @Override
    public void apply(RequestTemplate template) {
        Target<?> target = template.feignTarget();
        if (target == null || !ServiceNameConstants.SEC_VIAM.equals(target.name())) {
            return;
        }
        if (clientId == null || clientId.isBlank()) {
            log.warn("VIAM 服务身份未配置（vagw.security.viam-client-id.client-id），拒绝注入身份头");
            return;
        }
        // 覆盖（先移除外部同名头再注入）而非追加，防止外部身份污染
        template.removeHeader(CustomHeaders.CLIENT_ID);
        template.header(CustomHeaders.CLIENT_ID, clientId.trim());
        if (log.isDebugEnabled()) {
            log.debug("VIAM 服务身份已注入: clientId={}, target={}, method={}, path={}",
                    clientId, target.name(), template.method(), template.path());
        }
    }
}
