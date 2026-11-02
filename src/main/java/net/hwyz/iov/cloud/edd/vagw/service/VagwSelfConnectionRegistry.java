package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * VAGW 自身 MQTT 连接（superuser bypass）可信标记注册表。
 * <p>
 * {@code /mqtt/auth} 四因子 bypass 放行时按 clientId 写入 TTL 标记；{@code client.connected}
 * / {@code client.disconnected} Hook 命中该标记即视为 VAGW 自身受信服务连接，跳过设备上线
 * （DEVICE_ONLINE）流程与 fail-closed 断连，避免把自身连接误判为不可信设备而自踢/自断
 * （auth 端 bypass 与设备准入是两条无共享状态路径，Hook 无法自行区分）。
 * </p>
 * <p>
 * 标记经 Redis 承载保证跨实例一致（EMQX Hook 可能落在任意实例）；仅承载非敏感布尔标记，
 * 不含任何身份/凭据。读取失败 fail-closed（不信任未经正向证据的连接），写失败仅告警
 * 不阻断 auth（实际信任已在 bypass 四因子校验时建立，标记只是 Hook 关联的桥梁）。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class VagwSelfConnectionRegistry {

    private static final String KEY_PREFIX = "vagw:auth:service:";
    private static final long TTL_SECONDS = 300;

    private final StringRedisTemplate redisTemplate;

    /**
     * 记录 VAGW 自身连接标记（bypass 放行时调用）。失败仅告警，不抛出。
     */
    public void mark(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        try {
            redisTemplate.opsForValue().set(KEY_PREFIX + clientId, "1", TTL_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Failed to persist VAGW self-connection marker: clientId={}", clientId, e);
        }
    }

    /**
     * 判断该 clientId 是否为本实例认证过的 VAGW 自身连接。读取失败 fail-closed 返回 false。
     */
    public boolean isSelfConnection(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        try {
            return redisTemplate.opsForValue().get(KEY_PREFIX + clientId) != null;
        } catch (Exception e) {
            log.warn("Failed to read VAGW self-connection marker: clientId={}", clientId, e);
            return false;
        }
    }
}
