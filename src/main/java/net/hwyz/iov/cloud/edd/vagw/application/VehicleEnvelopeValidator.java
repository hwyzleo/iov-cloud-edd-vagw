package net.hwyz.iov.cloud.edd.vagw.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.ProtocolContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

/**
 * 公共 Envelope 元数据校验器（EDD-VAGW-DSN-CR-006 §6 校验顺序 2/5/6/7、US-011/US-015）。
 * <p>
 * 只依赖 iov-cloud-proto-vehicle-common 生成的 vehicle.common.v1.VehicleMessageEnvelope 与
 * PAR-PROTO release 的只读 PayloadType registry／manifest；不解析 Envelope.payload，不依赖
 * iov-cloud-proto-fota。payload 是唯一业务负载来源，对 VAGW 不透明。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VehicleEnvelopeValidator {

    private final ProtocolContractGuard contractGuard;

    @Value("${vagw.fota.max-envelope-bytes:262144}")
    private long maxEnvelopeBytes;
    @Value("${vagw.fota.max-payload-bytes:262016}")
    private long maxPayloadBytes;

    /**
     * 公共契约校验（上下行共用）：大小、required fields、service、PayloadType allowlist、
     * message_kind、protocol major、expire_at_ms。
     */
    public Failure validateBase(Envelope.VehicleMessageEnvelope envelope, byte[] envelopeBytes) {
        if (envelope == null) {
            return new Failure(Reason.CONTRACT_INVALID, "envelope is null");
        }
        if (envelopeBytes != null && envelopeBytes.length > maxEnvelopeBytes) {
            return new Failure(Reason.SIZE_EXCEEDED,
                    "envelope exceeds max size: " + envelopeBytes.length);
        }
        if (isBlank(envelope.getMessageId())) {
            return new Failure(Reason.CONTRACT_INVALID, "missing message_id");
        }
        if (isBlank(envelope.getDeviceId())) {
            return new Failure(Reason.CONTRACT_INVALID, "missing device_id");
        }
        if (isBlank(envelope.getProtocolVersion())) {
            return new Failure(Reason.CONTRACT_INVALID, "missing protocol_version");
        }
        if (isBlank(envelope.getPayloadType())) {
            return new Failure(Reason.CONTRACT_INVALID, "missing payload_type");
        }
        if (!VehicleRouteCatalog.FOTA_SERVICE.equals(envelope.getService())) {
            return new Failure(Reason.CONTRACT_INVALID,
                    "service mismatch: " + envelope.getService());
        }
        if (envelope.getMessageKind() == Envelope.MessageKind.MESSAGE_KIND_UNSPECIFIED) {
            return new Failure(Reason.CONTRACT_INVALID, "message_kind UNSPECIFIED rejected");
        }
        if (envelope.getPayload() == null || envelope.getPayload().isEmpty()) {
            return new Failure(Reason.CONTRACT_INVALID, "missing payload");
        }
        if (envelope.getPayload().size() > maxPayloadBytes) {
            return new Failure(Reason.SIZE_EXCEEDED,
                    "payload exceeds max size: " + envelope.getPayload().size());
        }
        // PayloadType allowlist 来自 PAR-PROTO release manifest（VAGW 不手写第二目录）
        if (!contractGuard.payloadTypes().contains(envelope.getPayloadType())) {
            return new Failure(Reason.PAYLOAD_TYPE_DENIED,
                    "payload_type not in PAR-PROTO registry: " + envelope.getPayloadType());
        }
        // 过期判断使用 expire_at_ms（绝对时间），不沿用旧 seq/ttl_ms
        if (envelope.hasExpireAtMs() && envelope.getExpireAtMs() > 0
                && envelope.getExpireAtMs() <= System.currentTimeMillis()) {
            return new Failure(Reason.MESSAGE_EXPIRED, "message expired (expire_at_ms)");
        }
        return null;
    }

    /**
     * 上行校验：公共校验 + 身份由 AccessIdentityValidator 单独校验。
     */
    public Failure validateUplink(Envelope.VehicleMessageEnvelope envelope, byte[] envelopeBytes) {
        return validateBase(envelope, envelopeBytes);
    }

    /**
     * 下行校验：公共校验 + Kafka Key(VIN) 与 Envelope.vin 一致性（US-013）。
     */
    public Failure validateDownlink(Envelope.VehicleMessageEnvelope envelope, byte[] envelopeBytes,
                                    String kafkaKeyVin) {
        Failure base = validateBase(envelope, envelopeBytes);
        if (base != null) {
            return base;
        }
        if (isBlank(envelope.getVin())) {
            return new Failure(Reason.CONTRACT_INVALID, "downlink missing vin in envelope");
        }
        if (isBlank(kafkaKeyVin)) {
            return new Failure(Reason.CONTRACT_INVALID, "downlink kafka key (vin) is blank");
        }
        if (!envelope.getVin().equalsIgnoreCase(kafkaKeyVin)) {
            return new Failure(Reason.DEVICE_MISMATCH,
                    "envelope vin does not match kafka key vin");
        }
        return null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    public enum Reason {
        CONTRACT_INVALID,      // 不可恢复契约错误 → DLQ
        PAYLOAD_TYPE_DENIED,   // allowlist 拒绝 → DLQ
        SIZE_EXCEEDED,         // 超限 → DLQ
        DEVICE_MISMATCH,       // → 技术投递 REJECTED
        MESSAGE_EXPIRED        // → 技术投递 REJECTED
    }

    public record Failure(Reason reason, String message) {
    }
}
