package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.kafka.UplinkKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UplinkServiceTest {

    @Mock
    private RouteService routeService;

    @Mock
    private UplinkKafkaProducer kafkaProducer;

    @InjectMocks
    private UplinkService uplinkService;

    @Test
    void processUplink_validEnvelopeUpAck_shouldRouteToAckTopic() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-001")
                .setVin("LSGJA52U7YA000001")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_ACK)
                .setTs(System.currentTimeMillis())
                .build();

        when(routeService.getTopic("remotecontrol")).thenReturn("iov.vagw.up.remotecontrol");

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "LSGJA52U7YA000001");

        assertTrue(result.success());
        verify(kafkaProducer).send(eq("iov.vagw.up.remotecontrol.ack"), eq("LSGJA52U7YA000001"), any(), eq("remotecontrol"), eq("UP_ACK"), any(), any());
    }

    @Test
    void processUplink_validEnvelopeUpData_shouldRouteToBaseTopic() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-002")
                .setVin("LSGJA52U7YA000001")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .build();

        when(routeService.getTopic("remotecontrol")).thenReturn("iov.vagw.up.remotecontrol");

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "LSGJA52U7YA000001");

        assertTrue(result.success());
        verify(kafkaProducer).send(eq("iov.vagw.up.remotecontrol"), eq("LSGJA52U7YA000001"), any(), eq("remotecontrol"), eq("UP_DATA"), any(), any());
    }

    @Test
    void processUplink_vinMismatch_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVin("VIN001")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .build();

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "VIN002");

        assertFalse(result.success());
        assertEquals(ErrorCode.VIN_MISMATCH, result.errorCode());
    }

    @Test
    void processUplink_invalidPayload_shouldFail() {
        UplinkService.ProcessResult result = uplinkService.processUplink(
                new byte[]{0x00, 0x01}, "VIN001");

        assertFalse(result.success());
        assertEquals(ErrorCode.INVALID_ENVELOPE, result.errorCode());
    }

    @Test
    void processUplink_noRoute_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVin("VIN001")
                .setService("unknown")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .build();

        when(routeService.getTopic("unknown")).thenReturn(null);

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "VIN001");

        assertFalse(result.success());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE, result.errorCode());
    }
}
