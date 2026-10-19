package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import com.google.protobuf.ByteString;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovReadiness;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.security.keyprov.v1.KeyEnvelopeFormat;
import net.hwyz.iov.security.keyprov.v1.KeyImportItemResult;
import net.hwyz.iov.security.keyprov.v1.KeyImportResult;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningAck;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningStatusRequest;
import net.hwyz.iov.security.keyprov.v1.KeyPurpose;
import net.hwyz.iov.security.keyprov.v1.WrappingSuite;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vehicle.common.v1.Envelope;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * keyprov canonical 协议校验测试（EDD-VAGW-DSN-CR-011 §3.3/§11-3，由 EDD-VAGW-DSN-CR-017 §5/§7/§12 修订）。
 * <p>
 * 覆盖 Request 的 suite/format/purpose/domain 结构门禁、CR-008 ACK Import Attestation 算法/长度/
 * legacy string/字段组合，以及 readiness 对 Request/Ack/Status 的差异化放行。
 * </p>
 */
class KeyprovProtocolValidatorTest {

    private static final String SERVICE = "security.keyprov";
    private static final String PROTOCOL = "keyprov-v1";
    private static final String REQ_TYPE = "security.keyprov.v1.KeyProvisioningRequest";
    private static final String ACK_TYPE = "security.keyprov.v1.KeyProvisioningAck";
    private static final String STATUS_REQ_TYPE = "security.keyprov.v1.KeyProvisioningStatusRequest";
    private static final String RESP_TYPE = "security.keyprov.v1.KeyProvisioningResponse";

    private KeyprovProperties properties;
    private KeyprovContractGuard guard;
    private KeyprovProtocolValidator validator;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        guard = mock(KeyprovContractGuard.class);
        when(guard.readiness()).thenReturn(readyReadiness());
        validator = new KeyprovProtocolValidator(properties, guard);
    }

    private static KeyprovReadiness readyReadiness() {
        return new KeyprovReadiness(true, null, null,
                true, true, true, true, true, true, true, true, true);
    }

    private static KeyprovReadiness notReady(KeyprovReason reason, boolean protocolApiCompatible) {
        return new KeyprovReadiness(false, reason, "drift",
                false, false, protocolApiCompatible, protocolApiCompatible, protocolApiCompatible,
                protocolApiCompatible, protocolApiCompatible, false, false);
    }

    private BrokerAuthenticatedPublishContext context() {
        return new BrokerAuthenticatedPublishContext(KeyprovContextSource.ACL_SESSION_CORRELATED,
                "DEVICE001", "client001", "cert-1", null, 3L, "client001", Instant.now());
    }

    private Envelope.VehicleMessageEnvelope.Builder baseEnvelope() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setService(SERVICE)
                .setProtocolVersion(PROTOCOL)
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001")
                .setMessageId("msg-001")
                .setRequestId("req-001");
    }

    private static KeyProvisioningRequest.Builder validRequest() {
        return KeyProvisioningRequest.newBuilder()
                .setRequestId("req-001")
                .setIdempotencyKey("idem-001")
                .setBusinessDomain("V2C_COMM")
                .addPurposes(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                .addPurposes(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD)
                .addSupportedWrappingSuites(
                        WrappingSuite.WRAPPING_SUITE_ECDH_ES_P256_HKDF_SHA256_AES256GCM)
                .addSupportedEnvelopeFormats(KeyEnvelopeFormat.KEY_ENVELOPE_FORMAT_ECDH_ES_P256_V1);
    }

    private static KeyImportItemResult.Builder item(KeyPurpose purpose, KeyImportResult result) {
        return KeyImportItemResult.newBuilder()
                .setPurpose(purpose)
                .setKeyId("key-" + purpose.getNumber())
                .setBusinessKeyVersion(1L)
                .setResult(result);
    }

    private static KeyImportItemResult.Builder importedItem(KeyPurpose purpose) {
        return item(purpose, KeyImportResult.KEY_IMPORT_RESULT_IMPORTED)
                .setImportProofAlgorithmValue(1)
                .setImportProof(ByteString.copyFrom(new byte[64]))
                .setRecipientCertificateDigest(ByteString.copyFrom(new byte[32]))
                .setEnvelopeDigest(ByteString.copyFrom(new byte[32]))
                .setKeyCheckValue(ByteString.copyFrom(new byte[3]))
                .setEnvelopeFormat(KeyEnvelopeFormat.KEY_ENVELOPE_FORMAT_ECDH_ES_P256_V1);
    }

    private static KeyProvisioningAck validAck() {
        return KeyProvisioningAck.newBuilder()
                .setRequestId("req-001")
                .addItems(importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD))
                .addItems(importedItem(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD))
                .build();
    }

    private Envelope.VehicleMessageEnvelope requestEnvelope(KeyProvisioningRequest request) {
        return baseEnvelope().setPayloadType(REQ_TYPE).setPayload(request.toByteString()).build();
    }

    private Envelope.VehicleMessageEnvelope ackEnvelope(KeyProvisioningAck ack) {
        return baseEnvelope().setPayloadType(ACK_TYPE).setPayload(ack.toByteString()).build();
    }

    private Envelope.VehicleMessageEnvelope statusEnvelope(KeyProvisioningStatusRequest request) {
        return baseEnvelope().setPayloadType(STATUS_REQ_TYPE).setPayload(request.toByteString()).build();
    }

    // ------------------------------------------------------------------
    // Request / Envelope 基础校验
    // ------------------------------------------------------------------

    @Test
    void validateUplink_validRequest_shouldPass() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build());
        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));
    }

    @Test
    void validateUplink_wrongService_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setService("vehicle.fota").build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_wrongProtocolVersion_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setProtocolVersion("fota-v1").build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_wrongMessageKind_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setMessageKind(Envelope.MessageKind.MESSAGE_KIND_RESPONSE).build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_deviceIdMismatch_shouldRejectAsIdentityMismatch() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setDeviceId("OTHER").build();
        assertEquals(KeyprovReason.IDENTITY_MISMATCH,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_unknownPayloadType_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayloadType("KeyProvisioningRequest")
                .setPayload(validRequest().build().toByteString()).build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_privateProtoAlias_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayloadType("security.keyprov.v1.KeyProvisioningRequestAlias")
                .setPayload(validRequest().build().toByteString()).build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_downlinkTypeOnUplink_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayloadType(RESP_TYPE)
                .setPayload(validRequest().build().toByteString()).build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_payloadTooLarge_shouldReject() {
        byte[] big = new byte[16385];
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayloadType(REQ_TYPE)
                .setPayload(ByteString.copyFrom(big)).build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_TOO_LARGE,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_expired_shouldRejectAsMessageExpired() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setExpireAtMs(System.currentTimeMillis() - 1000).build();
        assertEquals(KeyprovReason.KEYPROV_MESSAGE_EXPIRED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_expired_shouldReportNonSensitiveTimingSkew() {
        long deviceTimestamp = System.currentTimeMillis() - 120_000;
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder()
                .setTimestampMs(deviceTimestamp)
                .setExpireAtMs(deviceTimestamp + 11_000)
                .build();

        KeyprovProtocolValidator.Failure failure =
                validator.validateUplink(envelope, envelope.toByteArray(), context());

        assertEquals(KeyprovReason.KEYPROV_MESSAGE_EXPIRED, failure.reason());
        assertTrue(failure.message().contains("expiredByMs="), failure.message());
        assertTrue(failure.message().contains("deviceTimestampSkewMs="), failure.message());
        assertTrue(failure.message().contains("deviceTimestampSkewMs=1"), failure.message());
    }

    @Test
    void validateUplink_futureExpiry_shouldPass() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setExpireAtMs(System.currentTimeMillis() + 60_000).build();
        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));
    }

    @Test
    void validateUplink_missingMessageId_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setMessageId("").build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_missingRequestId_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build())
                .toBuilder().setRequestId("").build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_invalidCanonicalPayload_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayloadType(REQ_TYPE)
                .setPayload(ByteString.copyFrom(new byte[]{0x0A, (byte) 0xFF}))
                .build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_missingContext_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build());
        assertEquals(KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING,
                validator.validateUplink(envelope, envelope.toByteArray(), null).reason());
    }

    // ------------------------------------------------------------------
    // CR-017 Request 结构门禁
    // ------------------------------------------------------------------

    @Test
    void request_wrongBusinessDomain_shouldRejectWithoutRewriting() {
        KeyProvisioningRequest request = validRequest().setBusinessDomain("v2c_comm").build();
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(request);
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
        assertArrayEquals(request.toByteArray(), envelope.getPayload().toByteArray(),
                "VAGW 不得改写 Request payload");
    }

    @Test
    void request_missingOrDuplicatePurpose_shouldReject() {
        KeyProvisioningRequest missing = validRequest()
                .clearPurposes()
                .addPurposes(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                .build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(requestEnvelope(missing), null, context()).reason());

        KeyProvisioningRequest duplicate = validRequest()
                .clearPurposes()
                .addPurposes(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                .addPurposes(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                .build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(requestEnvelope(duplicate), null, context()).reason());
    }

    @Test
    void request_unspecifiedWrappingSuite_shouldFailClosed() {
        KeyProvisioningRequest request = validRequest()
                .clearSupportedWrappingSuites()
                .addSupportedWrappingSuitesValue(0)
                .build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(requestEnvelope(request), null, context()).reason());
    }

    @Test
    void request_unknownEnvelopeFormat_shouldFailClosedWithoutDowngrade() {
        KeyProvisioningRequest request = validRequest()
                .clearSupportedEnvelopeFormats()
                .addSupportedEnvelopeFormatsValue(99)
                .build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(requestEnvelope(request), null, context()).reason());
    }

    @Test
    void request_missingIdempotencyKey_shouldReject() {
        KeyProvisioningRequest request = validRequest().setIdempotencyKey("").build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(requestEnvelope(request), null, context()).reason());
    }

    @Test
    void request_canonicalPayloadBytes_shouldRemainUntouched() {
        KeyProvisioningRequest request = validRequest().build();
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(request);
        byte[] before = envelope.getPayload().toByteArray();

        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));

        assertArrayEquals(before, envelope.getPayload().toByteArray(),
                "校验必须只读，不得重序列化或改写 canonical payload");
    }

    // ------------------------------------------------------------------
    // CR-017 ACK Import Attestation 结构门禁
    // ------------------------------------------------------------------

    @Test
    void ack_validImportAttestation_shouldPassAndKeepProofBytes() {
        KeyProvisioningAck ack = validAck();
        Envelope.VehicleMessageEnvelope envelope = ackEnvelope(ack);
        byte[] before = envelope.getPayload().toByteArray();

        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));

        assertArrayEquals(before, envelope.getPayload().toByteArray(),
                "ACK canonical bytes/proof 必须逐字节保持");
        assertEquals(64, ack.getItems(0).getImportProof().size());
    }

    @Test
    void ack_algorithmZeroOrUnknown_shouldFailClosed() {
        KeyProvisioningAck zero = validAck().toBuilder()
                .setItems(0, importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                        .setImportProofAlgorithmValue(0).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_ALGORITHM_INVALID,
                validator.validateUplink(ackEnvelope(zero), null, context()).reason());

        KeyProvisioningAck unknown = validAck().toBuilder()
                .setItems(1, importedItem(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD)
                        .setImportProofAlgorithmValue(99).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_ALGORITHM_INVALID,
                validator.validateUplink(ackEnvelope(unknown), null, context()).reason());
    }

    @Test
    void ack_proofLength63Or65_shouldFailClosed() {
        KeyProvisioningAck shortProof = validAck().toBuilder()
                .setItems(0, importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                        .setImportProof(ByteString.copyFrom(new byte[63])).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_PROOF_INVALID,
                validator.validateUplink(ackEnvelope(shortProof), null, context()).reason());

        KeyProvisioningAck longProof = validAck().toBuilder()
                .setItems(1, importedItem(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD)
                        .setImportProof(ByteString.copyFrom(new byte[65])).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_PROOF_INVALID,
                validator.validateUplink(ackEnvelope(longProof), null, context()).reason());
    }

    @Test
    void ack_recipientCertificateDigestWrongLength_shouldFailClosed() {
        KeyProvisioningAck ack = validAck().toBuilder()
                .setItems(0, importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                        .setRecipientCertificateDigest(ByteString.copyFrom(new byte[31])).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_CERTIFICATE_DIGEST_INVALID,
                validator.validateUplink(ackEnvelope(ack), null, context()).reason());
    }

    @Test
    void ack_envelopeDigestOrKcvWrongLength_shouldFailClosed() {
        KeyProvisioningAck digest = validAck().toBuilder()
                .setItems(0, importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                        .setEnvelopeDigest(ByteString.copyFrom(new byte[31])).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED,
                validator.validateUplink(ackEnvelope(digest), null, context()).reason());

        KeyProvisioningAck kcv = validAck().toBuilder()
                .setItems(1, importedItem(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD)
                        .setKeyCheckValue(ByteString.copyFrom(new byte[2])).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED,
                validator.validateUplink(ackEnvelope(kcv), null, context()).reason());
    }

    @Test
    void ack_legacyProofAlgorithmNonEmpty_shouldFailClosed() {
        KeyProvisioningAck ack = validAck().toBuilder()
                .setItems(0, importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                        .setProofAlgorithm("SHA256withECDSA").build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED,
                validator.validateUplink(ackEnvelope(ack), null, context()).reason());
    }

    @Test
    void ack_duplicateOrMissingPurpose_shouldFailClosed() {
        KeyProvisioningAck duplicate = KeyProvisioningAck.newBuilder()
                .setRequestId("req-001")
                .addItems(importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD))
                .addItems(importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD))
                .build();
        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED,
                validator.validateUplink(ackEnvelope(duplicate), null, context()).reason());

        KeyProvisioningAck missing = KeyProvisioningAck.newBuilder()
                .setRequestId("req-001")
                .addItems(importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD))
                .build();
        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED,
                validator.validateUplink(ackEnvelope(missing), null, context()).reason());
    }

    @Test
    void ack_importedWithUnspecifiedFormat_shouldFailClosed() {
        KeyProvisioningAck ack = validAck().toBuilder()
                .setItems(0, importedItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                        .setEnvelopeFormatValue(0).build())
                .build();
        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED,
                validator.validateUplink(ackEnvelope(ack), null, context()).reason());
    }

    @Test
    void ack_nonImportedItemWithProof_shouldFailClosed() {
        KeyProvisioningAck ack = KeyProvisioningAck.newBuilder()
                .setRequestId("req-001")
                .addItems(item(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD, KeyImportResult.KEY_IMPORT_RESULT_FAILED)
                        .setImportProof(ByteString.copyFrom(new byte[64])))
                .addItems(item(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD, KeyImportResult.KEY_IMPORT_RESULT_UNKNOWN))
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_PROOF_INVALID,
                validator.validateUplink(ackEnvelope(ack), null, context()).reason());
    }

    @Test
    void ack_nonImportedItemWithAlgorithm_shouldFailClosed() {
        KeyProvisioningAck ack = KeyProvisioningAck.newBuilder()
                .setRequestId("req-001")
                .addItems(item(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD, KeyImportResult.KEY_IMPORT_RESULT_FAILED)
                        .setImportProofAlgorithmValue(1))
                .addItems(item(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD, KeyImportResult.KEY_IMPORT_RESULT_UNKNOWN))
                .build();
        assertEquals(KeyprovReason.KEYPROV_ACK_ALGORITHM_INVALID,
                validator.validateUplink(ackEnvelope(ack), null, context()).reason());
    }

    // ------------------------------------------------------------------
    // Readiness 与 Status 只读路径
    // ------------------------------------------------------------------

    @Test
    void request_whenNotReady_shouldRejectWithLockReason() {
        when(guard.readiness()).thenReturn(notReady(KeyprovReason.KEYPROV_CONTRACT_RELEASE_MISMATCH, true));
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build());
        assertEquals(KeyprovReason.KEYPROV_CONTRACT_RELEASE_MISMATCH,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void ack_whenNotReady_shouldRejectWithLockReason() {
        when(guard.readiness()).thenReturn(notReady(KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH, true));
        assertEquals(KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH,
                validator.validateUplink(ackEnvelope(validAck()), null, context()).reason());
    }

    @Test
    void statusRequest_whenProtocolApiCompatible_shouldRemainAvailable() {
        when(guard.readiness()).thenReturn(notReady(KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH, true));
        KeyProvisioningStatusRequest status = KeyProvisioningStatusRequest.newBuilder()
                .setRequestId("req-001").build();
        assertNull(validator.validateUplink(statusEnvelope(status), null, context()),
                "release/golden 漂移时 Status 只读对账必须保留");
    }

    @Test
    void statusRequest_whenProtocolApiIncompatible_shouldReject() {
        when(guard.readiness()).thenReturn(notReady(KeyprovReason.KEYPROV_VMD_API_INCOMPATIBLE, false));
        KeyProvisioningStatusRequest status = KeyProvisioningStatusRequest.newBuilder()
                .setRequestId("req-001").build();
        assertEquals(KeyprovReason.KEYPROV_VMD_API_INCOMPATIBLE,
                validator.validateUplink(statusEnvelope(status), null, context()).reason());
    }

    @Test
    void statusRequest_missingRequestId_shouldReject() {
        KeyProvisioningStatusRequest status = KeyProvisioningStatusRequest.newBuilder()
                .setRequestId("").build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(statusEnvelope(status), null, context()).reason());
    }

    @Test
    void validateUplink_fallbackWhenGuardHasNoReadiness_shouldUseCompatible() {
        when(guard.readiness()).thenReturn(null);
        when(guard.compatible()).thenReturn(true);
        Envelope.VehicleMessageEnvelope envelope = requestEnvelope(validRequest().build());
        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));
        assertNotNull(envelope.getPayload());
    }

    @Test
    void isAllowedDownlinkPayloadType_shouldOnlyAllowThreeDownlinkTypes() {
        assertTrue(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.KeyProvisioningResponse"));
        assertTrue(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.KeyProvisioningAckResult"));
        assertTrue(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.KeyProvisioningStatusResponse"));
        assertEquals(false, validator.isAllowedDownlinkPayloadType(REQ_TYPE));
        assertEquals(false, validator.isAllowedDownlinkPayloadType("security.keyprov.v1.Unknown"));
    }
}
