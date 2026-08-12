package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.springframework.stereotype.Component;

/**
 * FOTA 接入 Envelope 校验器，对齐 EDD-VAGW-DSN-CR-005 §4。
 * <p>
 * VAGW 只解析接入 Envelope 头（service / deviceId / VIN / msgType / schemaVersion / TTL），
 * payload 为 OTA 契约 bytes，对 VAGW 不透明；不得改写 messageId、关联链、摘要或 payload。
 * </p>
 */
@Slf4j
@Component
public class FotaEnvelopeValidator {

    public static final int SUPPORTED_SCHEMA_VERSION = 1;

    /**
     * 基础校验（上下行共用）：schemaVersion / service / messageId / msgType / payload。
     */
    public FotaValidationFailure validateBase(EnvelopeProto.Envelope envelope) {
        if (envelope == null) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID, "envelope is null");
        }
        if (envelope.getVer() != SUPPORTED_SCHEMA_VERSION) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID,
                    "unsupported schemaVersion: " + envelope.getVer());
        }
        if (!FotaRouteConfig.SERVICE.equals(envelope.getService())) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID, "service mismatch");
        }
        if (envelope.getMsgId() == null || envelope.getMsgId().isBlank()) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID, "missing messageId");
        }
        if (envelope.getMsgType() == EnvelopeProto.MsgType.MSG_TYPE_UNSPECIFIED) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID, "missing msgType");
        }
        if (envelope.getPayload() == null || envelope.getPayload().isEmpty()) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID, "missing payload");
        }
        return null;
    }

    /**
     * 上行校验：基础校验 + deviceId 存在且 == 连接身份（越权 → DEVICE_MISMATCH）。
     */
    public FotaValidationFailure validateUplink(EnvelopeProto.Envelope envelope, String connectionDeviceSn) {
        FotaValidationFailure base = validateBase(envelope);
        if (base != null) {
            return base;
        }
        if (envelope.getDeviceSn() == null || envelope.getDeviceSn().isBlank()) {
            return new FotaValidationFailure(DeliveryReason.CONTRACT_INVALID, "missing deviceId");
        }
        if (connectionDeviceSn != null && !envelope.getDeviceSn().equalsIgnoreCase(connectionDeviceSn)) {
            return new FotaValidationFailure(DeliveryReason.DEVICE_MISMATCH,
                    "deviceId does not match connection identity");
        }
        return null;
    }

    /**
     * 下行校验：基础校验 + Kafka Key/VIN 与 Envelope VIN 一致性 + TTL 过期。
     */
    public FotaValidationFailure validateDownlink(EnvelopeProto.Envelope envelope, String kafkaKeyVin) {
        FotaValidationFailure base = validateBase(envelope);
        if (base != null) {
            return base;
        }
        if (kafkaKeyVin != null && !kafkaKeyVin.isBlank()
                && envelope.getVin() != null && !envelope.getVin().isBlank()
                && !envelope.getVin().equalsIgnoreCase(kafkaKeyVin)) {
            return new FotaValidationFailure(DeliveryReason.DEVICE_MISMATCH,
                    "envelope vin does not match kafka key vin");
        }
        if (envelope.getTtlMs() > 0) {
            long expireAt = envelope.getTs() + envelope.getTtlMs();
            if (System.currentTimeMillis() > expireAt) {
                return new FotaValidationFailure(DeliveryReason.MESSAGE_EXPIRED, "message expired");
            }
        }
        return null;
    }

    /**
     * 校验失败结果：reasonCode（OTA 契约）+ 描述。
     */
    public record FotaValidationFailure(DeliveryReason reason, String message) {
    }
}
