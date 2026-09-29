package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * VAGW 自身 MQTT 连接超级用户 bypass 配置（EDD-VAGW-DSN-CR-009 §2.6/§5/§9）。
 * <p>
 * 四因子 AND 校验：peerhost 白名单 + 专属服务证书 CN + 受控 clientId + 独立 internal listener。
 * 其中 internal listener 无法从 HTTP authn 请求中区分，须由 EMQX 网络隔离保证
 * （仅 internal listener 可达 VAGW 自身连接；TBOX 外部 listener 永不授予 bypass），
 * 本类注释与 application.yml 将其登记为部署前置条件。
 * </p>
 * <p>
 * 任一因子未配置/无法验证 → fail-closed（不授予 bypass），绝不回退到仅凭 clientId 前缀放行。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "vagw.auth.bypass")
public class VagwAuthBypassProperties {

    /**
     * 总开关。默认关闭（最安全）：未显式开启并配置全部因子时，任何连接都不授予超级用户 bypass。
     * 生产启用示例见 application.yml。
     */
    private boolean enabled = false;

    /**
     * peerhost 白名单（CIDR 或 IP），VAGW 自身节点来源。为空时 bypass 永不成立。
     * 例：10.0.0.0/8、172.16.0.10。
     */
    private List<String> peerHostCidrs = new ArrayList<>();

    /**
     * 专属 VAGW 服务证书 CN（mTLS，须精确匹配 peer_cert_cn）。为空时 bypass 永不成立。
     */
    private String serviceCertCn = "";

    /**
     * 受控 clientId 前缀规则（VAGW 自身 MQTT 客户端 ID 规则）。
     */
    private String clientIdPrefix = "vehicle-access-gateway";
}
