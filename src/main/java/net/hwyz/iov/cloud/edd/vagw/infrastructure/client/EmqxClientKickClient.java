package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;

/**
 * EMQX 管理面断连客户端（EDD-VAGW-DSN-CR-012 §3.2/§4，可选）。
 * <p>
 * 仅在设备上线 fail-closed（无准入上下文、旧 generation、身份不一致、持久化失败）时，
 * 主动断开会话，避免不可信连接保持在线。未配置 Base URL 或未启用时退化为仅会话下线 + 告警，
 * 不阻断主流程。凭据经 Secret 注入，不落日志。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EmqxClientKickClient {

    private final VagwDeviceOnlineProperties properties;
    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * 尽力断开指定 clientId；返回是否实际发起并成功。
     */
    public boolean kick(String clientId, String reason) {
        VagwDeviceOnlineProperties.EmqxApi api = properties.getEmqxApi();
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        if (!api.isEnabled() || api.getBaseUrl() == null || api.getBaseUrl().isBlank()) {
            log.warn("SEC-DEVICE-ONLINE | session not terminated via EMQX API (disabled/unconfigured): reason={}", reason);
            return false;
        }
        try {
            String url = trimTrailingSlash(api.getBaseUrl()) + "/api/v5/clients/"
                    + UriUtils.encodePathSegment(clientId, StandardCharsets.UTF_8);
            HttpHeaders headers = new HttpHeaders();
            headers.setBasicAuth(api.getApiKey(), api.getApiSecret(), StandardCharsets.UTF_8);
            headers.setContentType(MediaType.APPLICATION_JSON);
            restTemplate.exchange(url, HttpMethod.DELETE, new HttpEntity<>(headers), Void.class);
            log.warn("SEC-DEVICE-ONLINE | kicked untrusted session via EMQX API: reason={}", reason);
            return true;
        } catch (Exception e) {
            log.error("SEC-DEVICE-ONLINE | EMQX kick failed (best-effort): reason={}, err={}", reason, e.getMessage());
            return false;
        }
    }

    private static String trimTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}
