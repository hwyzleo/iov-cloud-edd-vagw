package net.hwyz.iov.cloud.edd.vagw.application.deviceonline;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.VagwDeviceOnlineProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox.DeviceOnlineOutboxStore;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.DeviceOnlineMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * 设备上线会话对账（EDD-VAGW-DSN-CR-012 §3.5/§4）。
 * <p>
 * 以在线会话索引（ZSET）为准，核对每个在线会话的 DEVICE_ONLINE eventId 是否已有 Outbox 记录：
 * 记录缺失（如外部淘汰或入队部分失败）时按已固化 eventId 重建 payload 重新入队；
 * 会话已离线或过期则清理索引。对账只是安全网，正常路径由 Hook 原子入队保证。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceOnlineReconciler {

    private final DeviceOnlineOutboxStore store;
    private final SessionService sessionService;
    private final DeviceOnlineEventFactory eventFactory;
    private final VagwDeviceOnlineProperties properties;
    private final DeviceOnlineMetrics metrics;

    @Scheduled(fixedDelayString = "${vagw.kafka.device-online.reconcile-interval-ms:60000}")
    public void reconcile() {
        if (!properties.isEnabled()) {
            return;
        }
        Set<String> members = store.onlineMembers(properties.getReconcileBatchSize());
        for (String hsmUid : members) {
            try {
                reconcileSession(hsmUid);
            } catch (Exception e) {
                log.warn("DEVICE-ONLINE | reconcile failed: hsmUidDigest={}",
                        DeviceOnlineEventFactory.identityDigest(hsmUid), e);
            }
        }
    }

    private void reconcileSession(String hsmUid) {
        Optional<SessionInfo> sessionOpt = sessionService.getSessionByDeviceSn(hsmUid);
        if (sessionOpt.isEmpty() || !sessionOpt.get().isOnline()) {
            store.removeOnline(hsmUid);
            metrics.reconcile("stale-session-removed");
            return;
        }
        SessionInfo session = sessionOpt.get();
        String eventId = session.getLastDeviceOnlineEventId();
        if (eventId == null || eventId.isBlank()) {
            metrics.reconcile("missing-event-id");
            return;
        }
        if (store.recordExists(eventId)) {
            metrics.reconcile("ok");
            return;
        }
        // 记录被淘汰或入队未完成：按已固化 eventId 重建 payload 重新入队（下游仍按 eventId 幂等）
        DeviceOnlineEvent rebuilt = rebuild(session, eventId);
        store.enqueue(rebuilt, session.getDeviceSn(), eventFactory.toJson(rebuilt));
        log.warn("DEVICE-ONLINE | reconciled missing outbox record: eventId={}, hsmUidDigest={}",
                eventId, DeviceOnlineEventFactory.identityDigest(hsmUid));
        metrics.reconcile("reenqueued");
        metrics.outboxStatus("PENDING");
    }

    private DeviceOnlineEvent rebuild(SessionInfo session, String eventId) {
        Instant occurredAt = session.getConnectedAt() == null ? Instant.now() : session.getConnectedAt();
        return new DeviceOnlineEvent(
                eventId,
                DeviceOnlineEvent.TYPE,
                session.getDeviceSn(),
                session.getVin(),
                session.getBrokerSessionId(),
                session.getConnectionGeneration(),
                session.getCertSerial(),
                null,
                occurredAt,
                DeviceOnlineEvent.TIME_SOURCE_VAGW_RECEIVED);
    }
}
