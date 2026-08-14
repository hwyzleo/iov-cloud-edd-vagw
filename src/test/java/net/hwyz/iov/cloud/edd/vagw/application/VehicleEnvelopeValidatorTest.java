package net.hwyz.iov.cloud.edd.vagw.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.ProtocolContractGuard;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import vehicle.common.v1.Envelope;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 公共 Envelope 元数据校验器测试（EDD-VAGW-DSN-CR-006 §6/§7、US-011）。
 */
class VehicleEnvelopeValidatorTest {

    private static VehicleEnvelopeValidator validator;

    @BeforeAll
    static void setUp() {
        ProtocolContractGuard guard = new ProtocolContractGuard(new ObjectMapper(), 1);
        guard.verify();
        validator = new VehicleEnvelopeValidator(guard);
        // @Value 字段在单测中无 Spring 注入，显式设置有界参数
        org.springframework.test.util.ReflectionTestUtils.setField(validator, "maxEnvelopeBytes", 262144L);
        org.springframework.test.util.ReflectionTestUtils.setField(validator, "maxPayloadBytes", 262016L);
    }

    private Envelope.VehicleMessageEnvelope.Builder base(String messageId) {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId("req-1")
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion("fota-v1")
                .setDeviceId("DEVICE001")
                .setVin("VIN-A")
                .setMessageId(messageId)
                .setPayloadType("vehicle.fota.v1.TaskCheckRequest")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setService("vehicle.fota")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}));
    }

    @Test
    void validateUplink_valid_shouldPass() {
        Envelope.VehicleMessageEnvelope e = base("m-001").build();
        assertNull(validator.validateUplink(e, e.toByteArray()));
    }

    @Test
    void validateUplink_wrongService_shouldFailContractInvalid() {
        Envelope.VehicleMessageEnvelope e = base("m-002").setService("vehicle.ota").build();
        assertEquals(VehicleEnvelopeValidator.Reason.CONTRACT_INVALID,
                validator.validateUplink(e, e.toByteArray()).reason());
    }

    @Test
    void validateUplink_oldNamespacePayloadType_shouldBeDenied() {
        Envelope.VehicleMessageEnvelope e = base("m-003")
                .setPayloadType("vehicle.ota.v1.Task").build();
        assertEquals(VehicleEnvelopeValidator.Reason.PAYLOAD_TYPE_DENIED,
                validator.validateUplink(e, e.toByteArray()).reason());
    }

    @Test
    void validateUplink_unknownPayloadType_shouldBeDenied() {
        Envelope.VehicleMessageEnvelope e = base("m-004")
                .setPayloadType("vehicle.fota.v1.NotInRegistry").build();
        assertEquals(VehicleEnvelopeValidator.Reason.PAYLOAD_TYPE_DENIED,
                validator.validateUplink(e, e.toByteArray()).reason());
    }

    @Test
    void validateUplink_kindUnspecified_shouldFail() {
        Envelope.VehicleMessageEnvelope e = base("m-005")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_UNSPECIFIED).build();
        assertEquals(VehicleEnvelopeValidator.Reason.CONTRACT_INVALID,
                validator.validateUplink(e, e.toByteArray()).reason());
    }

    @Test
    void validateUplink_missingMessageId_shouldFail() {
        Envelope.VehicleMessageEnvelope e = base("").build();
        assertEquals(VehicleEnvelopeValidator.Reason.CONTRACT_INVALID,
                validator.validateUplink(e, e.toByteArray()).reason());
    }

    @Test
    void validateUplink_expired_shouldFail() {
        Envelope.VehicleMessageEnvelope e = base("m-006")
                .setExpireAtMs(System.currentTimeMillis() - 1000).build();
        assertEquals(VehicleEnvelopeValidator.Reason.MESSAGE_EXPIRED,
                validator.validateUplink(e, e.toByteArray()).reason());
    }

    @Test
    void validateUplink_futureExpiry_shouldPass() {
        Envelope.VehicleMessageEnvelope e = base("m-007")
                .setExpireAtMs(System.currentTimeMillis() + 60_000).build();
        assertNull(validator.validateUplink(e, e.toByteArray()));
    }

    @Test
    void validateDownlink_vinMismatch_shouldFailDeviceMismatch() {
        Envelope.VehicleMessageEnvelope e = base("m-008").build();
        assertEquals(VehicleEnvelopeValidator.Reason.DEVICE_MISMATCH,
                validator.validateDownlink(e, e.toByteArray(), "VIN-B").reason());
    }

    @Test
    void validateDownlink_vinMatch_shouldPass() {
        Envelope.VehicleMessageEnvelope e = base("m-009").build();
        assertNull(validator.validateDownlink(e, e.toByteArray(), "VIN-A"));
    }

    @Test
    void validateDownlink_missingVin_shouldFail() {
        Envelope.VehicleMessageEnvelope e = base("m-010").clearVin().build();
        assertEquals(VehicleEnvelopeValidator.Reason.CONTRACT_INVALID,
                validator.validateDownlink(e, e.toByteArray(), "VIN-A").reason());
    }
}
