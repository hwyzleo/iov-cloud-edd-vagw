package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.kafka.UplinkKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UplinkServiceTest {

    @Mock
    private RouteService routeService;

    @Mock
    private UplinkKafkaProducer kafkaProducer;

    @Mock
    private BindingService bindingService;

    @InjectMocks
    private UplinkService uplinkService;

    @Test
    void processUplink_validEnvelopeUpAck_shouldRouteToAckTopic() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-001")
                .setDeviceSn("DEVICE001")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_ACK)
                .setTs(System.currentTimeMillis())
                .build();

        when(routeService.getTopic("remotecontrol")).thenReturn("iov.vagw.up.remotecontrol");
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("LSGJA52U7YA000001"));

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertTrue(result.ok());
        verify(kafkaProducer).send(eq("iov.vagw.up.remotecontrol.ack"), eq("DEVICE001"),
                eq("LSGJA52U7YA000001"), any(), eq("remotecontrol"), eq("UP_ACK"), any(), any());
    }

    @Test
    void processUplink_validEnvelopeUpData_shouldRouteToBaseTopic() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-002")
                .setDeviceSn("DEVICE001")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .build();

        when(routeService.getTopic("remotecontrol")).thenReturn("iov.vagw.up.remotecontrol");
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("LSGJA52U7YA000001"));

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertTrue(result.ok());
        verify(kafkaProducer).send(eq("iov.vagw.up.remotecontrol"), eq("DEVICE001"),
                eq("LSGJA52U7YA000001"), any(), eq("remotecontrol"), eq("UP_DATA"), any(), any());
    }

    @Test
    void processUplink_deviceSnMismatch_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setDeviceSn("DEVICE001")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .build();

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE002");

        assertFalse(result.ok());
        assertEquals(ErrorCode.IDENTITY_MISMATCH, result.errorCode());
    }

    @Test
    void processUplink_invalidPayload_shouldFail() {
        UplinkService.ProcessResult result = uplinkService.processUplink(
                new byte[]{0x00, 0x01}, "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.INVALID_ENVELOPE, result.errorCode());
    }

    @Test
    void processUplink_noRoute_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setDeviceSn("DEVICE001")
                .setService("unknown")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .build();

        when(routeService.getTopic("unknown")).thenReturn(null);

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE, result.errorCode());
    }

    @Test
    void processUplink_unboundDevice_shouldStillRouteWithNullVin() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-003")
                .setDeviceSn("DEVICE999")
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .build();

        when(routeService.getTopic("remotecontrol")).thenReturn("iov.vagw.up.remotecontrol");
        when(bindingService.resolveVin("DEVICE999")).thenReturn(Optional.empty());

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE999");

        assertTrue(result.ok());
        verify(kafkaProducer).send(eq("iov.vagw.up.remotecontrol"), eq("DEVICE999"),
                isNull(), any(), eq("remotecontrol"), eq("UP_DATA"), any(), any());
    }

    @Test
    void processUplink_missingDeviceSn_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setService("remotecontrol")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .build();

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.INVALID_ENVELOPE, result.errorCode());
    }

    @Test
    void processUplink_missingService_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setDeviceSn("DEVICE001")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .build();

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.INVALID_ENVELOPE, result.errorCode());
    }
}
