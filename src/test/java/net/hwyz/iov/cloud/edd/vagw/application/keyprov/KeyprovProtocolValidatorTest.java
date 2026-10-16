package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vehicle.common.v1.Envelope;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * keyprov canonical 协议校验测试（EDD-VAGW-DSN-CR-011 §3.3/§11-3）。
 */
class KeyprovProtocolValidatorTest {

    private static final String SERVICE = "security.keyprov";
    private static final String PROTOCOL = "keyprov-v1";
    private static final String REQ_TYPE = "security.keyprov.v1.KeyProvisioningRequest";
    private static final String RESP_TYPE = "security.keyprov.v1.KeyProvisioningResponse";

    private KeyprovProperties properties;
    private KeyprovContractGuard guard;
    private KeyprovProtocolValidator validator;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        guard = org.mockito.Mockito.mock(KeyprovContractGuard.class);
        org.mockito.Mockito.when(guard.compatible()).thenReturn(true);
        validator = new KeyprovProtocolValidator(properties, guard);
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
                .setRequestId("req-001")
                .setPayloadType(REQ_TYPE)
                .setPayload(KeyProvisioningRequest.newBuilder().setRequestId("req-001").build().toByteString());
    }

    @Test
    void validateUplink_validRequest_shouldPass() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().build();
        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));
    }

    @Test
    void validateUplink_wrongService_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setService("vehicle.fota").build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_wrongProtocolVersion_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setProtocolVersion("fota-v1").build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_wrongMessageKind_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope =
                baseEnvelope().setMessageKind(Envelope.MessageKind.MESSAGE_KIND_RESPONSE).build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_deviceIdMismatch_shouldRejectAsIdentityMismatch() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setDeviceId("OTHER").build();
        assertEquals(KeyprovReason.IDENTITY_MISMATCH,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_unknownPayloadType_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setPayloadType("KeyProvisioningRequest").build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_downlinkTypeOnUplink_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setPayloadType(RESP_TYPE).build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_payloadTooLarge_shouldReject() {
        byte[] big = new byte[16385];
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayload(com.google.protobuf.ByteString.copyFrom(big)).build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_TOO_LARGE,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_expired_shouldRejectAsMessageExpired() {
        Envelope.VehicleMessageEnvelope envelope =
                baseEnvelope().setExpireAtMs(System.currentTimeMillis() - 1000).build();
        assertEquals(KeyprovReason.KEYPROV_MESSAGE_EXPIRED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_futureExpiry_shouldPass() {
        Envelope.VehicleMessageEnvelope envelope =
                baseEnvelope().setExpireAtMs(System.currentTimeMillis() + 60_000).build();
        assertNull(validator.validateUplink(envelope, envelope.toByteArray(), context()));
    }

    @Test
    void validateUplink_missingMessageId_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setMessageId("").build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_missingRequestId_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().setRequestId("").build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_invalidCanonicalPayload_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope()
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{0x0A, (byte) 0xFF}))
                .build();
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void validateUplink_missingContext_shouldReject() {
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().build();
        assertEquals(KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING,
                validator.validateUplink(envelope, envelope.toByteArray(), null).reason());
    }

    @Test
    void validateUplink_contractIncompatible_shouldReject() {
        org.mockito.Mockito.when(guard.compatible()).thenReturn(false);
        org.mockito.Mockito.when(guard.incompatibilityReason()).thenReturn("drift");
        Envelope.VehicleMessageEnvelope envelope = baseEnvelope().build();
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED,
                validator.validateUplink(envelope, envelope.toByteArray(), context()).reason());
    }

    @Test
    void isAllowedDownlinkPayloadType_shouldOnlyAllowThreeDownlinkTypes() {
        assertTrue(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.KeyProvisioningResponse"));
        assertTrue(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.KeyProvisioningAckResult"));
        assertTrue(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.KeyProvisioningStatusResponse"));
        assertFalse(validator.isAllowedDownlinkPayloadType(REQ_TYPE));
        assertFalse(validator.isAllowedDownlinkPayloadType("security.keyprov.v1.Unknown"));
    }
}
