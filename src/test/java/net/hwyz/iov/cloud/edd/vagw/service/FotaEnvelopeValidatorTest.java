package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FotaEnvelopeValidator 单元测试（EDD-VAGW-DSN-CR-005 §4）。
 */
class FotaEnvelopeValidatorTest {

    private final FotaEnvelopeValidator validator = new FotaEnvelopeValidator();

    private EnvelopeProto.Envelope validUplink() {
        return EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-001")
                .setDeviceSn("DEVICE001")
                .setService("fota")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
    }

    @Test
    void validateUplink_valid_shouldPass() {
        assertNull(validator.validateUplink(validUplink(), "DEVICE001"));
    }

    @Test
    void validateUplink_deviceMismatch_shouldFailDeviceMismatch() {
        FotaEnvelopeValidator.FotaValidationFailure f = validator.validateUplink(validUplink(), "OTHER");
        assertNotNull(f);
        assertEquals(DeliveryReason.DEVICE_MISMATCH, f.reason());
    }

    @Test
    void validateUplink_unsupportedVersion_shouldFailContractInvalid() {
        EnvelopeProto.Envelope e = validUplink().toBuilder().setVer(99).build();
        assertEquals(DeliveryReason.CONTRACT_INVALID,
                validator.validateUplink(e, "DEVICE001").reason());
    }

    @Test
    void validateUplink_wrongService_shouldFailContractInvalid() {
        EnvelopeProto.Envelope e = validUplink().toBuilder().setService("remotecontrol").build();
        assertEquals(DeliveryReason.CONTRACT_INVALID,
                validator.validateUplink(e, "DEVICE001").reason());
    }

    @Test
    void validateUplink_missingMessageId_shouldFailContractInvalid() {
        EnvelopeProto.Envelope e = validUplink().toBuilder().setMsgId("").build();
        assertEquals(DeliveryReason.CONTRACT_INVALID,
                validator.validateUplink(e, "DEVICE001").reason());
    }

    @Test
    void validateUplink_missingPayload_shouldFailContractInvalid() {
        EnvelopeProto.Envelope e = validUplink().toBuilder().clearPayload().build();
        assertEquals(DeliveryReason.CONTRACT_INVALID,
                validator.validateUplink(e, "DEVICE001").reason());
    }

    @Test
    void validateUplink_missingDeviceSn_shouldFailContractInvalid() {
        EnvelopeProto.Envelope e = validUplink().toBuilder().clearDeviceSn().build();
        assertEquals(DeliveryReason.CONTRACT_INVALID,
                validator.validateUplink(e, "DEVICE001").reason());
    }

    @Test
    void validateDownlink_ttlExpired_shouldFailMessageExpired() {
        EnvelopeProto.Envelope e = validUplink().toBuilder()
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setTs(System.currentTimeMillis() - 60_000)
                .setTtlMs(10_000)
                .build();
        FotaEnvelopeValidator.FotaValidationFailure f = validator.validateDownlink(e, "VIN-A");
        assertNotNull(f);
        assertEquals(DeliveryReason.MESSAGE_EXPIRED, f.reason());
    }

    @Test
    void validateDownlink_vinMismatch_shouldFailDeviceMismatch() {
        EnvelopeProto.Envelope e = validUplink().toBuilder()
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setVin("VIN-A")
                .build();
        FotaEnvelopeValidator.FotaValidationFailure f = validator.validateDownlink(e, "VIN-B");
        assertNotNull(f);
        assertEquals(DeliveryReason.DEVICE_MISMATCH, f.reason());
    }

    @Test
    void validateDownlink_vinConsistent_shouldPass() {
        EnvelopeProto.Envelope e = validUplink().toBuilder()
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setVin("VIN-A")
                .setTtlMs(0)
                .build();
        assertNull(validator.validateDownlink(e, "VIN-A"));
    }
}
