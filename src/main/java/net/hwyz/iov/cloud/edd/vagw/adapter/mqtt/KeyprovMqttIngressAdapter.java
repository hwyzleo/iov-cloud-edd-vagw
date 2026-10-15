package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.AuthenticatedPublishContextResolver;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.KeyprovRelayAppService;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import net.hwyz.iov.cloud.edd.vagw.mqtt.UplinkSubscriptionHandler;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

import java.time.Instant;

/**
 * keyprov MQTT 上行入站适配器（EDD-VAGW-DSN-CR-011 §3.1）。
 * <p>
 * 受理 {@code vehicle/{hsm_uid}/up/keyprov}（EMQX 共享订阅，QoS1）；执行原始大小门禁后，
 * 交给认证上下文解析器与 canonical 协议校验器，再经 relay 编排转发 VMD。
 * 不写 Kafka、DLQ 或普通持久化队列。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KeyprovMqttIngressAdapter implements UplinkSubscriptionHandler {

    private static final String SERVICE_SEGMENT = "keyprov";

    /** 业务 service 名（metrics 标签）。 */
    public static final String SERVICE = "security.keyprov";

    private final KeyprovProperties properties;
    private final KeyprovContractGuard contractGuard;
    private final AuthenticatedPublishContextResolver contextResolver;
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

        KeyprovRelayResult result = relayService.relay(resolution.context(), envelope, payload);
        if (!result.delivered()) {
            KeyprovReason reason = result.reason();
            metrics.uplinkRejected(SERVICE, reason == null ? "keyprov_upstream_failure" : reason.name());
            log.warn("keyprov uplink relay not delivered: messageId={}, reason={}, hsmUid={}",
                    envelope.getMessageId(), result.reason(), LogMask.mask(topicHsmUid));
        } else if (result.duplicate()) {
            metrics.uplinkDuplicate(SERVICE);
        }
    }
}
