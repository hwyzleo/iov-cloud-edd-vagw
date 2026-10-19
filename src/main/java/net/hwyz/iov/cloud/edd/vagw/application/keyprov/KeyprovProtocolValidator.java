package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import com.google.protobuf.MessageLite;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovReadiness;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.security.keyprov.v1.KeyImportItemResult;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningAck;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningStatusRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_ACK_ALGORITHM_INVALID;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_ACK_CERTIFICATE_DIGEST_INVALID;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_ACK_PROOF_INVALID;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_MESSAGE_EXPIRED;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_PAYLOAD_INVALID;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_PAYLOAD_TOO_LARGE;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING;

/**
 * keyprov canonical 协议校验器（EDD-VAGW-DSN-CR-011 §3.3，由 EDD-VAGW-DSN-CR-017 §5/§7 修订）。
 * <p>
 * 固定顺序：认证会话 → Envelope service/protocol/messageKind/payloadType → readiness 门控 →
 * device_id → TTL/size/标识 → CR-008 Descriptor 结构门禁。只做 schema、长度、枚举、资源和会话
 * 边界校验，不补值、不协商、不降级、不重序列化 payload。
 * </p>
 * <p>
 * VAGW 不解析 proof r/s、不检查 low-S、不构造 proofInputV1、不做证书绑定或验签；ACK proof
 * bytes 通过门禁后保持原样交给 VMD。所有失败 detail 只包含字段名/长度/稳定 reason，不含 payload。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovProtocolValidator {

    private static final String BUSINESS_DOMAIN_V2C = "V2C_COMM";
    private static final int MAX_REPEATED_ITEMS = 8;
    private static final int MAX_REQUEST_ID_BYTES = 128;
    private static final int MAX_CERTIFICATE_DIGEST_BYTES = 64;
    private static final int MAX_CERTIFICATE_PROFILE_BYTES = 64;
    private static final int MAX_KEY_ENCIPHERMENT_KEY_ID_BYTES = 128;
    private static final int IMPORT_PROOF_BYTES = 64;
    private static final int ENVELOPE_DIGEST_BYTES = 32;
    private static final int RECIPIENT_CERTIFICATE_DIGEST_BYTES = 32;
    private static final int KEY_CHECK_VALUE_BYTES = 3;
    private static final int UPLINK_PURPOSE = 1;
    private static final int DOWNLINK_PURPOSE = 2;
    private static final int IMPORTED_RESULT = 1;
    private static final int FAILED_RESULT = 2;
    private static final int UNKNOWN_RESULT = 3;
    private static final int ECDH_ES_P256_V1_FORMAT = 1;

    private final KeyprovProperties properties;
    private final KeyprovContractGuard contractGuard;

    /**
     * 上行校验。
     *
     * @return null 通过；否则返回稳定失败 reason
     */
    public Failure validateUplink(Envelope.VehicleMessageEnvelope envelope, byte[] rawEnvelope,
                                  BrokerAuthenticatedPublishContext context) {
        if (envelope == null) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED, "envelope is null");
        }
        if (context == null || isBlank(context.hsmUid())) {
            return new Failure(KEYPROV_SESSION_CONTEXT_MISSING, "authenticated context missing");
        }
        if (!properties.getService().equals(envelope.getService())) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED, "service mismatch: " + envelope.getService());
        }
        if (!properties.getProtocolVersion().equals(envelope.getProtocolVersion())) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "protocol_version mismatch: " + envelope.getProtocolVersion());
        }
        if (envelope.getMessageKind() != Envelope.MessageKind.MESSAGE_KIND_REQUEST) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "message_kind must be REQUEST: " + envelope.getMessageKind());
        }
        Optional<KeyprovMessageType> type = KeyprovMessageType.fromFullName(envelope.getPayloadType());
        if (type.isEmpty() || !type.get().isUpstream()) {
            return new Failure(KEYPROV_PROTOCOL_UNSUPPORTED,
                    "payload_type not in keyprov upstream registry: " + envelope.getPayloadType());
        }

        // readiness 是统一门控：NOT_READY 时仅保留协议/API 兼容下的只读 Status 对账
        KeyprovReadiness readiness = contractGuard.readiness();
        if (readiness == null) {
            readiness = fallbackReadiness();
        }
        if (!readiness.allows(type.get())) {
            KeyprovReason reason = readiness.reason() == null
                    ? KEYPROV_PROTOCOL_UNSUPPORTED : readiness.reason();
            return new Failure(reason, "keyprov not ready: " + readiness.notReadyDetail());
        }

        if (isBlank(envelope.getDeviceId()) || !context.hsmUid().equalsIgnoreCase(envelope.getDeviceId())) {
            return new Failure(KeyprovReason.IDENTITY_MISMATCH, "device_id does not match authenticated hsmUid");
        }
        int size = envelope.getPayload().size();
        if (size == 0) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "missing payload");
        }
        if (size > properties.getMaxPayloadBytes()) {
            return new Failure(KEYPROV_PAYLOAD_TOO_LARGE, "payload exceeds max size: " + size);
        }
        if (envelope.hasExpireAtMs() && envelope.getExpireAtMs() > 0) {
            long now = System.currentTimeMillis();
            long expiresAt = envelope.getExpireAtMs();
            if (expiresAt <= now) {
                // 只输出非敏感时间差：既证过期程度，又能定位设备时钟偏移 / 陈旧重投
                // （deviceTimestampSkewMs > 0 表示设备 timestamp_ms 落后于网关时钟；-1 表示未提供）
                long deviceTimestampSkewMs =
                        envelope.getTimestampMs() > 0 ? now - envelope.getTimestampMs() : -1L;
                return new Failure(KEYPROV_MESSAGE_EXPIRED,
                        "message expired (expire_at_ms), expiredByMs=" + (now - expiresAt)
                                + ", deviceTimestampSkewMs=" + deviceTimestampSkewMs);
            }
        }
        int maxId = properties.getIdentifierMaxLength();
        if (isBlank(envelope.getMessageId()) || exceeds(envelope.getMessageId(), maxId)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "message_id missing or too long");
        }
        if (isBlank(envelope.getRequestId()) || exceeds(envelope.getRequestId(), maxId)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request_id missing or too long");
        }

        // CR-008 Descriptor-based parse + 结构门禁：失败即拒绝（不落盘、不回显内容）
        byte[] canonicalPayload = envelope.getPayload().toByteArray();
        try {
            MessageLite parsed = type.get().parser().parseFrom(canonicalPayload);
            return switch (type.get()) {
                case KEY_PROVISIONING_REQUEST -> validateRequestStructure((KeyProvisioningRequest) parsed);
                case KEY_PROVISIONING_ACK -> validateAckStructure((KeyProvisioningAck) parsed);
                case KEY_PROVISIONING_STATUS_REQUEST ->
                        validateStatusRequestStructure((KeyProvisioningStatusRequest) parsed);
                default -> new Failure(KEYPROV_PROTOCOL_UNSUPPORTED, "downlink type on uplink");
            };
        } catch (Exception e) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "canonical payload decode failed");
        }
    }

    public boolean isAllowedDownlinkPayloadType(String payloadType) {
        return KeyprovMessageType.fromFullName(payloadType).filter(t -> !t.isUpstream()).isPresent();
    }

    /**
     * Request 结构门禁：不补造 capability、不替换枚举、不静默降级。
     */
    private Failure validateRequestStructure(KeyProvisioningRequest request) {
        if (isBlank(request.getRequestId()) || exceeds(request.getRequestId(), MAX_REQUEST_ID_BYTES)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request.request_id missing or too long");
        }
        if (isBlank(request.getIdempotencyKey())
                || exceeds(request.getIdempotencyKey(), MAX_REQUEST_ID_BYTES)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request.idempotency_key missing or too long");
        }
        if (!BUSINESS_DOMAIN_V2C.equals(request.getBusinessDomain())) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request.business_domain not V2C_COMM");
        }
        if (request.getPurposesCount() != 2) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request.purposes must contain exactly two values");
        }
        Set<Integer> purposes = new HashSet<>();
        for (int i = 0; i < request.getPurposesCount(); i++) {
            purposes.add(request.getPurposesValue(i));
        }
        if (!purposes.equals(Set.of(UPLINK_PURPOSE, DOWNLINK_PURPOSE))) {
            return new Failure(KEYPROV_PAYLOAD_INVALID,
                    "request.purposes must be exactly UPLINK_AEAD and DOWNLINK_AEAD without duplicates");
        }
        if (request.getSupportedWrappingSuitesCount() > MAX_REPEATED_ITEMS
                || request.getSupportedEnvelopeFormatsCount() > MAX_REPEATED_ITEMS
                || request.getCurrentVersionsCount() > MAX_REPEATED_ITEMS) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request repeated field exceeds item limit");
        }
        for (int i = 0; i < request.getSupportedWrappingSuitesCount(); i++) {
            int suite = request.getSupportedWrappingSuitesValue(i);
            if (suite != 1 && suite != 2) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.supported_wrapping_suites contains unknown/UNSPECIFIED value");
            }
        }
        for (int i = 0; i < request.getSupportedEnvelopeFormatsCount(); i++) {
            int format = request.getSupportedEnvelopeFormatsValue(i);
            if (format != ECDH_ES_P256_V1_FORMAT) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.supported_envelope_formats contains unknown/UNSPECIFIED value");
            }
        }
        Set<Integer> currentPurposes = new HashSet<>();
        for (int i = 0; i < request.getCurrentVersionsCount(); i++) {
            int purpose = request.getCurrentVersions(i).getPurposeValue();
            if (purpose != UPLINK_PURPOSE && purpose != DOWNLINK_PURPOSE) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.current_versions contains unknown/UNSPECIFIED purpose");
            }
            if (!currentPurposes.add(purpose)) {
                return new Failure(KEYPROV_PAYLOAD_INVALID, "request.current_versions duplicate purpose");
            }
            if (request.getCurrentVersions(i).getBusinessKeyVersion() < 0) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.current_versions business_key_version must be >= 0");
            }
        }
        if (request.getDeviceIdentityDigest().size() > MAX_CERTIFICATE_DIGEST_BYTES) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "request.device_identity_digest too long");
        }
        if (request.hasCertificateRef()) {
            if (request.getCertificateRef().getCertificateDigest().size() > MAX_CERTIFICATE_DIGEST_BYTES) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.certificate_ref.certificate_digest too long");
            }
            if (exceeds(request.getCertificateRef().getCertificateProfile(),
                    MAX_CERTIFICATE_PROFILE_BYTES)) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.certificate_ref.certificate_profile too long");
            }
            if (exceeds(request.getCertificateRef().getKeyEnciphermentKeyId(),
                    MAX_KEY_ENCIPHERMENT_KEY_ID_BYTES)) {
                return new Failure(KEYPROV_PAYLOAD_INVALID,
                        "request.certificate_ref.key_encipherment_key_id too long");
            }
        }
        return null;
    }

    /**
     * ACK Import Attestation 结构门禁（CR-008 §7）。
     * <p>
     * 只校验每用途字段、算法 enum、固定长度与 legacy string；proof bytes 内容对 VAGW 不透明。
     * </p>
     */
    private Failure validateAckStructure(KeyProvisioningAck ack) {
        if (isBlank(ack.getRequestId()) || exceeds(ack.getRequestId(), MAX_REQUEST_ID_BYTES)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "ack.request_id missing or too long");
        }
        if (ack.getItemsCount() != 2) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack.items must contain exactly two purpose items");
        }
        Set<Integer> purposes = new HashSet<>();
        for (KeyImportItemResult item : ack.getItemsList()) {
            purposes.add(item.getPurposeValue());
            Failure itemFailure = validateAckItem(item);
            if (itemFailure != null) {
                return itemFailure;
            }
        }
        if (!purposes.equals(Set.of(UPLINK_PURPOSE, DOWNLINK_PURPOSE))) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack.items must be exactly UPLINK_AEAD and DOWNLINK_AEAD without duplicates");
        }
        return null;
    }

    private Failure validateAckItem(KeyImportItemResult item) {
        if (item.getPurposeValue() != UPLINK_PURPOSE && item.getPurposeValue() != DOWNLINK_PURPOSE) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack item purpose unknown/UNSPECIFIED");
        }
        if (isBlank(item.getKeyId()) || exceeds(item.getKeyId(), MAX_REQUEST_ID_BYTES)) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED, "ack item key_id missing or too long");
        }
        if (!item.getProofAlgorithm().isEmpty()) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack item legacy proof_algorithm must be empty");
        }
        int result = item.getResultValue();
        if (result == IMPORTED_RESULT) {
            return validateImportedAckItem(item);
        }
        if (result == FAILED_RESULT || result == UNKNOWN_RESULT) {
            return validateNonImportedAckItem(item);
        }
        return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                "ack item result unknown/UNSPECIFIED");
    }

    private Failure validateImportedAckItem(KeyImportItemResult item) {
        if (item.getImportProofAlgorithmValue() != 1) {
            return new Failure(KEYPROV_ACK_ALGORITHM_INVALID,
                    "ack item import_proof_algorithm must be 1");
        }
        if (item.getImportProof().size() != IMPORT_PROOF_BYTES) {
            return new Failure(KEYPROV_ACK_PROOF_INVALID,
                    "ack item import_proof must be exactly 64 bytes, actual="
                            + item.getImportProof().size());
        }
        if (item.getRecipientCertificateDigest().size() != RECIPIENT_CERTIFICATE_DIGEST_BYTES) {
            return new Failure(KEYPROV_ACK_CERTIFICATE_DIGEST_INVALID,
                    "ack item recipient_certificate_digest must be exactly 32 bytes, actual="
                            + item.getRecipientCertificateDigest().size());
        }
        if (item.getEnvelopeDigest().size() != ENVELOPE_DIGEST_BYTES) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack item envelope_digest must be exactly 32 bytes, actual="
                            + item.getEnvelopeDigest().size());
        }
        if (item.getKeyCheckValue().size() != KEY_CHECK_VALUE_BYTES) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack item key_check_value must be exactly 3 bytes, actual="
                            + item.getKeyCheckValue().size());
        }
        if (item.getEnvelopeFormatValue() != ECDH_ES_P256_V1_FORMAT) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack item envelope_format must be ECDH-ES P256 v1 for an IMPORTED item");
        }
        if (item.getBusinessKeyVersion() <= 0) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "ack item business_key_version must be in 1..2^63-1 for an IMPORTED item");
        }
        return null;
    }

    private Failure validateNonImportedAckItem(KeyImportItemResult item) {
        if (item.getImportProofAlgorithmValue() != 0) {
            return new Failure(KEYPROV_ACK_ALGORITHM_INVALID,
                    "non-imported ack item must not declare an import proof algorithm");
        }
        if (!item.getImportProof().isEmpty()) {
            return new Failure(KEYPROV_ACK_PROOF_INVALID,
                    "non-imported ack item must not carry import_proof");
        }
        if (!item.getRecipientCertificateDigest().isEmpty()) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "non-imported ack item must not carry recipient_certificate_digest");
        }
        if (item.getEnvelopeDigest().size() != 0 && item.getEnvelopeDigest().size() != ENVELOPE_DIGEST_BYTES) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "non-imported ack item envelope_digest must be empty or 32 bytes");
        }
        if (item.getKeyCheckValue().size() != 0 && item.getKeyCheckValue().size() != KEY_CHECK_VALUE_BYTES) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "non-imported ack item key_check_value must be empty or 3 bytes");
        }
        int format = item.getEnvelopeFormatValue();
        if (format != 0 && format != ECDH_ES_P256_V1_FORMAT) {
            return new Failure(KEYPROV_IMPORT_ATTESTATION_REJECTED,
                    "non-imported ack item envelope_format unknown/UNSPECIFIED value");
        }
        return null;
    }

    private Failure validateStatusRequestStructure(KeyProvisioningStatusRequest request) {
        if (isBlank(request.getRequestId()) || exceeds(request.getRequestId(), MAX_REQUEST_ID_BYTES)) {
            return new Failure(KEYPROV_PAYLOAD_INVALID, "status_request.request_id missing or too long");
        }
        return null;
    }

    /** 未注入 guard 的单元测试/降级场景：只接受协议兼容时的 Status。 */
    private KeyprovReadiness fallbackReadiness() {
        boolean compatible = contractGuard.compatible();
        return new KeyprovReadiness(compatible, compatible ? null : KEYPROV_PROTOCOL_UNSUPPORTED,
                compatible ? null : contractGuard.incompatibilityReason(),
                compatible, compatible, compatible, compatible, compatible, compatible, compatible,
                compatible, compatible);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static boolean exceeds(String s, int maxBytes) {
        return s != null && s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maxBytes;
    }

    public record Failure(KeyprovReason reason, String message) {
    }
}
