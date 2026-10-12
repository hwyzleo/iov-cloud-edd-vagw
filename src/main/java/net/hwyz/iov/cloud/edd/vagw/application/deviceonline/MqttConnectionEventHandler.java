package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.EmqxClientKickClient;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox.DeviceOnlineOutboxStore;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineHookResult;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

/**
 * MQTT 连接事件处理器（EDD-VAGW-DSN-CR-012 §3.2/§4）。
 * <p>
 * 仅当 {@code client.connected} 可关联到本次 TSP ALLOW 准入快照、身份一致、未过期且
 * generation 当前时，才原子更新会话并写入 DeviceOnline Outbox；未知身份、无准入上下文、
 * 旧 generation 或时间明显倒退时 fail-closed：不建立可信会话、不写 Outbox、告警并断连。
 * 持久化失败做有界恢复，仍失败则终止会话。
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MqttConnectionEventHandler {

    private final AdmissionSessionContextRegistry registry;
    private final SessionService sessionService;
    private final DeviceOnlineOutboxStore outboxStore;
    private final DeviceOnlineEventFactory eventFactory;
    private final VagwDeviceOnlineProperties properties;
    private final DeviceOnlineMetrics metrics;
    private final EmqxClientKickClient kicker;

    /**
     * 处理 client.connected Hook。
     */
    public DeviceOnlineHookResult handleConnected(MqttEventRequest request) {
        if (!properties.isEnabled()) {
            metrics.hookResult("disabled");
            return DeviceOnlineHookResult.DISABLED;
        }
        String hsmUid = normalize(request.getUsername());
        String clientId = request.getClientId();
        if (hsmUid == null || hsmUid.isBlank()) {
            metrics.hookResult("blank-identity");
            return DeviceOnlineHookResult.NO_ADMISSION_CONTEXT;
        }

        Optional<AdmissionSessionContext> contextOpt = registry.findCandidate(hsmUid, clientId);
        if (contextOpt.isEmpty()) {
            metrics.hookResult("no-admission-context");
            metrics.admissionContextMissing("NO_ADMISSION_CONTEXT");
            terminate(hsmUid, clientId, "NO_ADMISSION_CONTEXT");
            return DeviceOnlineHookResult.NO_ADMISSION_CONTEXT;
        }
        AdmissionSessionContext context = contextOpt.get();

        if (context.expiresAt() != null && context.expiresAt().isBefore(Instant.now())) {
            metrics.hookResult("expired");
            terminate(hsmUid, clientId, "EXPIRED");
            return DeviceOnlineHookResult.EXPIRED;
        }
        if (!hsmUid.equals(normalize(context.hsmUid()))) {
            metrics.hookResult("identity-mismatch");
            terminate(hsmUid, clientId, "IDENTITY_MISMATCH");
            return DeviceOnlineHookResult.IDENTITY_MISMATCH;
        }
        if (isStaleGeneration(hsmUid, context.connectionGeneration())) {
            metrics.hookResult("stale-generation");
            return DeviceOnlineHookResult.STALE_GENERATION;
        }

        DeviceOnlineEvent event = eventFactory.create(context, request, Instant.now());
        String payloadJson = eventFactory.toJson(event);

        int attempts = Math.max(1, properties.getPersistRetryAttempts());
        for (int i = 1; i <= attempts; i++) {
            try {
                // 先固化可信会话事实（active generation = auth 候选 generation），再写待发布事件
                sessionService.onConnected(hsmUid, clientId, request.getPeerHost(), request.getProtoVer(),
                        context.connectionGeneration(), event.eventId());
                DeviceOnlineOutboxStore.EnqueueResult result =
                        outboxStore.enqueue(event, hsmUid, payloadJson);
                boolean duplicate = result == DeviceOnlineOutboxStore.EnqueueResult.DUPLICATE;
                metrics.hookResult(duplicate ? "duplicate" : "enqueued");
                metrics.outboxStatus(duplicate ? "DUPLICATE" : "PENDING");
                return duplicate ? DeviceOnlineHookResult.DUPLICATE : DeviceOnlineHookResult.ENQUEUED;
            } catch (Exception e) {
                log.warn("DEVICE-ONLINE | persist attempt {}/{} failed: hsmUidDigest={}, err={}",
                        i, attempts, DeviceOnlineEventFactory.identityDigest(hsmUid), e.getMessage());
                sleep(properties.getPersistRetryBackoff());
            }
        }

        metrics.hookResult("persist-failed");
        terminate(hsmUid, clientId, "PERSIST_FAILED");
        return DeviceOnlineHookResult.PERSIST_FAILED;
    }

    private boolean isStaleGeneration(String hsmUid, long contextGeneration) {
        Optional<SessionInfo> current = sessionService.getSessionByDeviceSn(hsmUid);
        return current.filter(session -> session.getConnectionGeneration() > contextGeneration).isPresent();
    }

    /**
     * fail-closed 终止：会话下线 + 清理在线索引 + 尽力断连（不可信连接不得保持在线）。
     */
    private void terminate(String hsmUid, String clientId, String reason) {
        try {
            sessionService.onDisconnected(hsmUid);
        } catch (Exception e) {
            log.warn("DEVICE-ONLINE | mark session offline failed: reason={}, err={}", reason, e.getMessage());
        }
        outboxStore.removeOnline(hsmUid);
        metrics.disconnect(reason);
        kicker.kick(clientId, reason);
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private static void sleep(java.time.Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            return;
        }
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
