package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAuthBypassProperties;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import org.springframework.stereotype.Component;

/**
 * VAGW 自身 MQTT 连接超级用户 bypass 守卫（EDD-VAGW-DSN-CR-009 §2.6/§3/§5）。
 * <p>
 * 原实现仅凭 clientId.startsWith("vehicle-access-gateway") 即授予 isSuperuser=true，
 * 外部客户端可伪造 clientId 前缀直接获得超级用户权限（提权漏洞），已移除。
 * </p>
 * <p>
 * 现采用四因子 AND 校验，任一因子缺失/无法验证一律 fail-closed：
 * <ol>
 *   <li>peerhost 白名单（CIDR/IP），VAGW 自身节点来源；</li>
 *   <li>专属 VAGW 服务证书 CN（mTLS，peer_cert_cn 精确匹配）；</li>
 *   <li>受控 clientId 前缀规则；</li>
 *   <li>独立 internal listener —— HTTP authn 请求不含 listener 标识，该项由 EMQX 网络隔离保证
 *       （仅 internal listener 可达 VAGW 自身连接、TBOX 外部 listener 永不授予 bypass），
 *       作为部署前置条件，见 {@link VagwAuthBypassProperties} 与 application.yml 注释。</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SuperuserBypassGuard {

    private final VagwAuthBypassProperties properties;
    private final AuthSecurityMetrics metrics;

    public boolean isBypassAllowed(MqttAuthRequest request) {
        if (request == null || !properties.isEnabled()) {
            return false;
        }

        // 因子 1：peerhost 白名单
        if (properties.getPeerHostCidrs() == null || properties.getPeerHostCidrs().isEmpty()) {
            metrics.incBypassDenied("no_peerhost_allowlist");
            return false;
        }
        if (!matchesPeerHostAllowlist(request.getPeerHost())) {
            metrics.incBypassDenied("peerhost");
            return false;
        }

        // 因子 2：专属服务证书 CN（mTLS）
        String serviceCertCn = properties.getServiceCertCn();
        if (serviceCertCn == null || serviceCertCn.isBlank()) {
            metrics.incBypassDenied("no_service_cert_cn");
            return false;
        }
        if (!serviceCertCn.equalsIgnoreCase(request.getPeerCertCn())) {
            metrics.incBypassDenied("service_cert_cn");
            return false;
        }

        // 因子 3：受控 clientId 规则
        String clientId = request.getClientId();
        String prefix = properties.getClientIdPrefix();
        if (clientId == null || prefix == null || prefix.isBlank()
                || !clientId.startsWith(prefix) || clientId.length() <= prefix.length()) {
            metrics.incBypassDenied("clientid");
            return false;
        }

        // 因子 4：独立 internal listener —— 部署前置（EMQX 网络隔离），见类注释
        log.debug("VAGW superuser bypass all factors satisfied: peerHost={}", request.getPeerHost());
        return true;
    }

    private boolean matchesPeerHostAllowlist(String peerHost) {
        if (peerHost == null || peerHost.isBlank()) {
            return false;
        }
        String ip = stripPort(peerHost.trim());
        for (String cidr : properties.getPeerHostCidrs()) {
            if (cidr != null && cidrMatches(ip, cidr.trim())) {
                return true;
            }
        }
        return false;
    }

    private static String stripPort(String host) {
        int idx = host.lastIndexOf(':');
        // IPv4 host:port 或裸 IPv4；含多个冒号为 IPv6，按原值处理
        if (idx > 0 && host.indexOf(':') == idx) {
            return host.substring(0, idx);
        }
        return host;
    }

    private static boolean cidrMatches(String ip, String cidr) {
        try {
            String[] parts = cidr.split("/");
            String network = parts[0];
            int prefix = parts.length > 1 ? Integer.parseInt(parts[1]) : 32;
            if (prefix < 0 || prefix > 32) {
                return false;
            }
            long ipLong = ipToLong(ip);
            long netLong = ipToLong(network);
            long mask = prefix == 0 ? 0 : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
            return (ipLong & mask) == (netLong & mask);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static long ipToLong(String ip) {
        String[] octets = ip.split("\\.");
        if (octets.length != 4) {
            throw new IllegalArgumentException("not an IPv4 address: " + ip);
        }
        long value = 0;
        for (String octet : octets) {
            int part = Integer.parseInt(octet);
            if (part < 0 || part > 255) {
                throw new IllegalArgumentException("bad octet: " + ip);
            }
            value = (value << 8) | part;
        }
        return value;
    }
}
