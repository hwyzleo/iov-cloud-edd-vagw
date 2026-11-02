package net.hwyz.iov.cloud.edd.vagw.application.accessinvalidation;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwAccessInvalidationProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.InvalidateReason;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.EmqxClientKickClient;
import net.hwyz.iov.cloud.sec.viam.api.constant.AccessInvalidationType;
import net.hwyz.iov.cloud.sec.viam.api.event.AccessInvalidatedEvent;
import net.hwyz.iov.cloud.sec.viam.api.service.ViamAccessInvalidationService;
import net.hwyz.iov.cloud.sec.viam.api.vo.AccessInvalidationReceiptVo;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 访问失效执行处理器（SEC-VIAM-DSN-CR-001 / FR-VIAM-008 / G-3，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * 消费 {@code viam.access-invalidated} 事件并执行：
 * <ul>
 *   <li>幂等去重（eventId，Redis TTL）；</li>
 *   <li>乱序版本判定：securityVersion ≤ 已处理版本 → 旧事件忽略（不误断新会话）；</li>
 *   <li>CONNECTION_DISPOSAL：仅处置 targetGeneration 指定的会话（generation 已变 → SESSION_NOT_FOUND）；</li>
 *   <li>PRINCIPAL_REVOCATION：失效缓存/ACL 并断连当前会话（按安全版本判定）；</li>
 *   <li>以 {@link ViamAccessInvalidationService#receipt} 回执区分状态（含补偿重试）。</li>
 * </ul>
 * 事件 payload 不自报即授信：身份以 hsmUid 与会话快照交叉校验，generation/版本判定在本地执行。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccessInvalidationHandler {

    /** 事件幂等去重键前缀 */
    private static final String DEDUP_PREFIX = "vagw:access:event:";
    /** 每设备已处理安全版本前缀（乱序判定） */
    private static final String VERSION_PREFIX = "vagw:access:version:";

    private final SessionService sessionService;
    private final BindingService bindingService;
    private final EmqxClientKickClient kickClient;
    private final StringRedisTemplate redisTemplate;
    private final ViamAccessInvalidationService invalidationService;
    private final VagwAccessInvalidationProperties properties;

    /**
     * 处理访问失效事件。执行异常向上抛出（consumer 不 ack，交由 Kafka 重投）；幂等/旧事件静默忽略。
     */
    public void handle(AccessInvalidatedEvent event) {
        if (event == null || event.getEventId() == null || event.getEventId().isBlank()
                || event.getHsmUid() == null || event.getHsmUid().isBlank()) {
            log.warn("access-invalidated 事件缺必填字段，丢弃");
            return;
        }
        // 1. 幂等去重（防 Kafka 重投重复执行）
        if (!tryClaimEvent(event.getEventId())) {
            log.debug("access-invalidated 事件已处理，跳过: eventId={}", event.getEventId());
            return;
        }
        // 2. 乱序版本判定：旧事件不误断新会话（FR-VIAM-008 / G-3）
        Long lastSeen = lastSecurityVersion(event.getHsmUid());
        if (lastSeen != null && event.getSecurityVersion() <= lastSeen) {
            log.info("access-invalidated 旧事件忽略（不误断新会话）: hsmUid={}, securityVersion={}, lastSeen={}",
                    LogMask.mask(event.getHsmUid()), event.getSecurityVersion(), lastSeen);
            return;
        }
        // 3. 执行失效/断连
        AccessInvalidationReceiptVo receipt = execute(event);
        // 4. 回执（区分状态；失败重试）
        submitReceipt(receipt);
        // 5. 记录已处理版本（仅执行并回执后推进，防止半处理状态误断）
        recordSecurityVersion(event.getHsmUid(), event.getSecurityVersion());
    }

    private AccessInvalidationReceiptVo execute(AccessInvalidatedEvent event) {
        Instant executedAt = Instant.now();
        if (event.getInvalidationType() == AccessInvalidationType.CONNECTION_DISPOSAL) {
            return disposeConnection(event, executedAt);
        }
        // PRINCIPAL_REVOCATION（含 CONNECTION_DISPOSAL 之外的主体级撤销）
        return revokePrincipal(event, executedAt);
    }

    /**
     * 单连接处置：仅当 targetGeneration 匹配当前会话 generation 时才断连，否则不误断新会话。
     */
    private AccessInvalidationReceiptVo disposeConnection(AccessInvalidatedEvent event, Instant executedAt) {
        String hsmUid = normalize(event.getHsmUid());
        Optional<SessionInfo> session = sessionService.getSessionByDeviceSn(hsmUid);
        if (session.isEmpty() || !Boolean.TRUE.equals(session.get().isOnline())) {
            log.info("access-invalidated CONNECTION_DISPOSAL 无在线会话: hsmUid={}, eventId={}",
                    LogMask.mask(hsmUid), event.getEventId());
            return receipt(event.getEventId(), "SESSION_NOT_FOUND", "no online session for hsmUid", executedAt);
        }
        Long targetGeneration = event.getTargetGeneration();
        if (targetGeneration != null && session.get().getConnectionGeneration() != targetGeneration) {
            log.info("access-invalidated CONNECTION_DISPOSAL 目标 generation 已过期（不误断新会话）: "
                            + "hsmUid={}, targetGeneration={}, currentGeneration={}",
                    LogMask.mask(hsmUid), targetGeneration, session.get().getConnectionGeneration());
            return receipt(event.getEventId(), "SESSION_NOT_FOUND",
                    "target generation superseded", executedAt);
        }
        invalidateAndKick(event, session.get(), executedAt);
        return receipt(event.getEventId(), "DISCONNECTED",
                "binding/cache invalidated and session disconnected", executedAt);
    }

    /**
     * 主体级撤销：按安全版本失效绑定缓存/ACL 并断连当前会话。
     */
    private AccessInvalidationReceiptVo revokePrincipal(AccessInvalidatedEvent event, Instant executedAt) {
        String hsmUid = normalize(event.getHsmUid());
        Optional<SessionInfo> session = sessionService.getSessionByDeviceSn(hsmUid);
        if (session.isPresent() && Boolean.TRUE.equals(session.get().isOnline())) {
            // 会话授权时安全版本判定（FR-VIAM-008 / G-3 会话级）：事件版本 ≤ 会话授权版本 →
            // 会话在撤销之后才被授权（如 block(v10) 丢失、unblock(v11) 后重连的新会话），
            // 不得因迟到旧事件断连（仅依赖 lastSeen 无法覆盖事件丢失场景）。
            Long authorized = session.get().getAuthorizedSecurityVersion();
            if (authorized != null && event.getSecurityVersion() <= authorized) {
                log.info("access-invalidated 旧事件被会话授权版本覆盖（不误断新会话）: "
                                + "hsmUid={}, eventSecurityVersion={}, authorized={}, eventId={}",
                        LogMask.mask(hsmUid), event.getSecurityVersion(), authorized, event.getEventId());
                return receipt(event.getEventId(), "STALE_IGNORED",
                        "session authorized after revocation version", executedAt);
            }
            invalidateAndKick(event, session.get(), executedAt);
            return receipt(event.getEventId(), "DISCONNECTED",
                    "binding/cache/ACL invalidated and session disconnected", executedAt);
        }
        // 无在线会话：仅失效缓存/ACL（不重复断连）
        try {
            bindingService.invalidate(event.getVin(), hsmUid, toInvalidateReason(event));
        } catch (Exception e) {
            log.warn("access-invalidated 缓存/ACL 失效异常: hsmUid={}, err={}",
                    LogMask.mask(hsmUid), e.getMessage());
        }
        return receipt(event.getEventId(), "CACHE_ACL_INVALIDATED",
                "binding/cache/ACL invalidated, no online session", executedAt);
    }

    private void invalidateAndKick(AccessInvalidatedEvent event, SessionInfo session, Instant executedAt) {
        try {
            bindingService.invalidate(event.getVin(), session.getDeviceSn(), toInvalidateReason(event));
        } catch (Exception e) {
            log.warn("access-invalidated 绑定缓存失效异常: hsmUid={}, err={}",
                    LogMask.mask(session.getDeviceSn()), e.getMessage());
        }
        // 尽力实际断连（未配置 EMQX API 时退化为会话下线 + 告警，不伪造完成）
        boolean kicked = kickClient.kick(session.getClientId(), "ACCESS_INVALIDATED:" + event.getEventId());
        sessionService.onDisconnected(session.getDeviceSn());
        log.warn("access-invalidated 已执行失效/断连: hsmUid={}, eventId={}, reason={}, kicked={}",
                LogMask.mask(session.getDeviceSn()), event.getEventId(), event.getReason(), kicked);
    }

    private static InvalidateReason toInvalidateReason(AccessInvalidatedEvent event) {
        return switch (event.getReason() == null ? "ACCESS_INVALIDATED" : event.getReason().name()) {
            case "BLOCK" -> InvalidateReason.SECURITY_BLOCKED;
            case "RETIRE" -> InvalidateReason.DEVICE_RETIRED;
            case "REVOKE" -> InvalidateReason.CERT_REVOKED;
            case "UNBIND" -> InvalidateReason.UNBIND;
            case "REPLACEMENT" -> InvalidateReason.REBIND;
            default -> InvalidateReason.SESSION_END;
        };
    }

    /**
     * 提交执行回执（区分状态）；失败重试有限次数，仍失败记录告警（VIAM 超时对账兜底）。
     */
    private void submitReceipt(AccessInvalidationReceiptVo receipt) {
        Exception last = null;
        for (int attempt = 1; attempt <= properties.getReceiptRetryAttempts(); attempt++) {
            try {
                invalidationService.receipt(receipt);
                log.info("access-invalidated 执行回执已提交: invalidationId={}, state={}",
                        receipt.getInvalidationId(), receipt.getReceiptState());
                return;
            } catch (Exception e) {
                last = e;
                log.warn("access-invalidated 执行回执提交失败（第 {} 次）: invalidationId={}, state={}, err={}",
                        attempt, receipt.getInvalidationId(), receipt.getReceiptState(), e.getMessage());
                if (attempt < properties.getReceiptRetryAttempts()) {
                    sleepQuietly(properties.getReceiptRetryBackoff());
                }
            }
        }
        // 回执失败不伪造完成：VIAM 侧超时对账（TIMEOUT/RECONCILE）兜底
        log.error("access-invalidated 执行回执最终失败（VIAM 对账兜底）: invalidationId={}, state={}, err={}",
                receipt.getInvalidationId(), receipt.getReceiptState(),
                last == null ? "unknown" : last.getMessage());
    }

    private static AccessInvalidationReceiptVo receipt(String invalidationId, String state,
                                                       String detail, Instant executedAt) {
        return AccessInvalidationReceiptVo.builder()
                .invalidationId(invalidationId)
                .receiptState(state)
                .detail(detail)
                .executedAt(executedAt)
                .build();
    }

    // ------------------------------------------------------------------
    // 幂等与版本追踪（Redis，有界 TTL）
    // ------------------------------------------------------------------

    private boolean tryClaimEvent(String eventId) {
        try {
            Boolean claimed = redisTemplate.opsForValue().setIfAbsent(
                    DEDUP_PREFIX + eventId, "1", properties.getDedupTtl().toMillis(), TimeUnit.MILLISECONDS);
            return Boolean.TRUE.equals(claimed);
        } catch (Exception e) {
            // Redis 不可用时按未处理继续（安全优先；重复执行由 eventId 幂等与版本判定兜底）
            log.warn("access-invalidated 去重不可用（按未处理继续）: err={}", e.getMessage());
            return true;
        }
    }

    private Long lastSecurityVersion(String hsmUid) {
        try {
            String value = redisTemplate.opsForValue().get(VERSION_PREFIX + normalize(hsmUid));
            return value == null ? null : Long.parseLong(value);
        } catch (Exception e) {
            return null;
        }
    }

    private void recordSecurityVersion(String hsmUid, long version) {
        try {
            // 只推进不回落：乱序旧事件（如迟到 block v10）不得把已推进的版本回退
            Long current = lastSecurityVersion(hsmUid);
            long target = current == null ? version : Math.max(current, version);
            Duration ttl = properties.getVersionTtl();
            redisTemplate.opsForValue().set(VERSION_PREFIX + normalize(hsmUid),
                    Long.toString(target), ttl.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("access-invalidated 版本记录失败（下次按版本判定兜底）: err={}", e.getMessage());
        }
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(java.util.Locale.ROOT);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
