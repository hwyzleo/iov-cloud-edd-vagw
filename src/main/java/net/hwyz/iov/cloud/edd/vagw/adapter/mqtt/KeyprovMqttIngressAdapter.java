package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.AuthenticatedPublishContextResolver;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.KeyprovProtocolValidator;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.KeyprovRelayAppService;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovAdmissionBulkhead;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovExecutor;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.mqtt.UplinkSubscriptionHandler;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

import java.time.Instant;

/**
 * keyprov MQTT 上行入站适配器（EDD-VAGW-DSN-CR-011 §3.1，由 EDD-VAGW-DSN-CR-016 §2.1 增量修订）。
 * <p>
 * 受理 {@code vehicle/{hsm_uid}/up/keyprov}（EMQX 共享订阅，QoS1）。ingress 只负责读取认证上下文、
 * 校验 Topic/会话/Envelope/PayloadType/TTL/大小、尝试获取 Keyprov 专属并发许可，并将最小不可变任务
 * 提交到 {@link KeyprovExecutor} 后立即返回；<b>不在 MQTT 回调内执行 Feign 调用、等待 Future、
 * 阻塞获取线程池槽位或使用 CallerRunsPolicy</b>。许可耗尽立即 {@code KEYPROV_OVERLOADED}，
 * 执行器拒绝立即 {@code KEYPROV_EXECUTION_REJECTED}。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovMqttIngressAdapter implements UplinkSubscriptionHandler {

    private static final String SERVICE_SEGMENT = "keyprov";

    /** 业务 service 名（metrics 标签）。 */
    public static final String SERVICE = "security.keyprov";

    private final KeyprovProperties properties;
    private final KeyprovContractGuard contractGuard;
    private final KeyprovProtocolValidator protocolValidator;
    private final AuthenticatedPublishContextResolver contextResolver;
    private final KeyprovAdmissionBulkhead bulkhead;
    private final KeyprovExecutor executor;
    private final KeyprovRelayAppService relayService;
    private final VagwMqttSubscriptionMetrics metrics;

    @Override
    public String service() {
        return SERVICE;
    }

    @Override
    public String topicSegment() {
        return SERVICE_SEGMENT;
    }

    @Override
    public void handleUplink(String topic, byte[] payload) {
        String[] parts = topic == null ? new String[0] : topic.split("/");
        if (parts.length != 4 || !"vehicle".equals(parts[0]) || !"up".equals(parts[2])
                || !SERVICE_SEGMENT.equals(parts[3])) {
            metrics.uplinkRejected(SERVICE, "invalid_topic_format");
            log.warn("keyprov uplink topic rejected (format): service segment invalid");
            return;
        }
        String topicHsmUid = parts[1];

        if (!properties.isEnabled()) {
            // disabled 时不应获授 keyprov ACL；此处仍 fail-closed，仅审计
            metrics.uplinkRejected(SERVICE, "keyprov_disabled");
            log.warn("keyprov uplink rejected: KEYPROV_DISABLED, hsmUid={}", LogMask.mask(topicHsmUid));
            return;
        }
        if (!contractGuard.compatible()) {
            metrics.uplinkRejected(SERVICE, "keyprov_protocol_unsupported");
            log.warn("keyprov uplink rejected: KEYPROV_PROTOCOL_UNSUPPORTED (contract incompatible), hsmUid={}",
                    LogMask.mask(topicHsmUid));
            return;
        }
        if (payload == null || payload.length == 0 || payload.length > properties.getMaxEnvelopeBytes()) {
            metrics.uplinkRejected(SERVICE, "keyprov_payload_too_large");
            log.warn("keyprov uplink rejected: KEYPROV_PAYLOAD_TOO_LARGE, hsmUid={}, size={}",
                    LogMask.mask(topicHsmUid), payload == null ? 0 : payload.length);
            return;
        }

        Envelope.VehicleMessageEnvelope envelope;
        try {
            envelope = Envelope.VehicleMessageEnvelope.parseFrom(payload);
        } catch (Exception e) {
            metrics.uplinkRejected(SERVICE, "keyprov_payload_invalid");
            log.warn("keyprov uplink rejected: KEYPROV_PAYLOAD_INVALID (envelope parse failed), hsmUid={}",
                    LogMask.mask(topicHsmUid));
            return;
        }

        AuthenticatedPublishContextResolver.Resolution resolution =
                contextResolver.resolve(topicHsmUid, Instant.now());
        if (resolution.context() == null) {
            metrics.uplinkRejected(SERVICE, "keyprov_session_context_missing");
            log.warn("keyprov uplink rejected: reason={}, hsmUid={}",
                    resolution.failureReason(), LogMask.mask(topicHsmUid));
            return;
        }
        BrokerAuthenticatedPublishContext context = resolution.context();

        // 身份/协议/TTL/大小门禁在 bulkhead 之前完成，避免用无效消息占用并发许可
        KeyprovProtocolValidator.Failure failure = protocolValidator.validateUplink(envelope, payload, context);
        if (failure != null) {
            metrics.uplinkRejected(SERVICE, failure.reason().name());
            log.warn("keyprov uplink rejected: reason={}, detail={}, hsmUid={}",
                    failure.reason(), failure.message(), LogMask.mask(topicHsmUid));
            return;
        }

        // 获得专属并发许可（许可覆盖“提交→VMD→下行”完整生命周期，在任务 finally 释放）
        KeyprovAdmissionBulkhead.Permit permit = bulkhead.tryAcquire();
        if (permit == null) {
            metrics.uplinkRejected(SERVICE, "KEYPROV_OVERLOADED");
            log.warn("keyprov uplink rejected: KEYPROV_OVERLOADED (bulkhead saturated), hsmUid={}",
                    LogMask.mask(topicHsmUid));
            return;
        }

        boolean submitted;
        try {
            submitted = executor.submit(() -> {
                try {
                    relayService.relay(context, envelope, payload);
                } finally {
                    permit.close();
                }
            });
        } catch (RuntimeException e) {
            permit.close();
            submitted = false;
            log.error("keyprov task submission failed: hsmUid={}, exception={}",
                    LogMask.mask(topicHsmUid), e.getClass().getSimpleName());
        }
        if (!submitted) {
            metrics.uplinkRejected(SERVICE, "KEYPROV_EXECUTION_REJECTED");
            log.warn("keyprov uplink rejected: KEYPROV_EXECUTION_REJECTED (executor capacity exhausted), hsmUid={}",
                    LogMask.mask(topicHsmUid));
        }
        // MQTT 回调在此返回；不等待 VMD，也不代表密钥已创建或车端已导入
    }
}
