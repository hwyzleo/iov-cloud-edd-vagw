package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.kafka.UplinkKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.kms.KmsKeyProvClient;
import net.hwyz.iov.cloud.edd.vagw.kms.KmsKeyProvException;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import net.hwyz.iov.cloud.edd.vagw.proto.KeyProvProto;
import org.eclipse.paho.client.mqttv3.MqttException;
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

    @Mock
    private KmsKeyProvClient kmsKeyProvClient;

    @Mock
    private MqttClientManager mqttClientManager;

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

    @Test
    void processUplink_keyprov_validRequest_shouldCallKmsAndPublishResponse() throws Exception {
        KeyProvProto.KeyProvRequest keyProvRequest = KeyProvProto.KeyProvRequest.newBuilder()
                .setBizDomain("test-domain")
                .setUsage("test-usage")
                .build();

        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-keyprov-001")
                .setDeviceSn("DEVICE001")
                .setService("keyprov")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .setSeq(1)
                .setTtlMs(30000)
                .setPayload(com.google.protobuf.ByteString.copyFrom(keyProvRequest.toByteArray()))
                .build();

        KmsKeyProvClient.KeyProvIssueResult issueResult = new KmsKeyProvClient.KeyProvIssueResult(
                new byte[]{0x01, 0x02, 0x03}, // wrappedKey
                "key-001", // keyId
                1, // keyVersion
                "AES-256-GCM", // alg
                System.currentTimeMillis() + 86400000, // validUntil
                new byte[]{0x04, 0x05} // kdfParams
        );

        when(kmsKeyProvClient.issue(any(KmsKeyProvClient.KeyProvIssueRequest.class))).thenReturn(issueResult);

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertTrue(result.ok());
        verify(kmsKeyProvClient).issue(any(KmsKeyProvClient.KeyProvIssueRequest.class));
        verify(mqttClientManager).publish(eq("vehicle/DEVICE001/down/keyprov"), any(byte[].class), eq(1));
    }

    @Test
    void processUplink_keyprov_kmsFailure_shouldSendFailureResponse() throws Exception {
        KeyProvProto.KeyProvRequest keyProvRequest = KeyProvProto.KeyProvRequest.newBuilder()
                .setBizDomain("test-domain")
                .setUsage("test-usage")
                .build();

        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-keyprov-002")
                .setDeviceSn("DEVICE001")
                .setService("keyprov")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .setSeq(1)
                .setTtlMs(30000)
                .setPayload(com.google.protobuf.ByteString.copyFrom(keyProvRequest.toByteArray()))
                .build();

        when(kmsKeyProvClient.issue(any(KmsKeyProvClient.KeyProvIssueRequest.class)))
                .thenThrow(new KmsKeyProvException("KMS unavailable"));

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.DEPENDENCY_UNAVAILABLE, result.errorCode());
        verify(mqttClientManager).publish(eq("vehicle/DEVICE001/down/keyprov"), any(byte[].class), eq(1));
    }

    @Test
    void processUplink_keyprov_mqttPublishFailure_shouldFail() throws Exception {
        KeyProvProto.KeyProvRequest keyProvRequest = KeyProvProto.KeyProvRequest.newBuilder()
                .setBizDomain("test-domain")
                .setUsage("test-usage")
                .build();

        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-keyprov-003")
                .setDeviceSn("DEVICE001")
                .setService("keyprov")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .setSeq(1)
                .setTtlMs(30000)
                .setPayload(com.google.protobuf.ByteString.copyFrom(keyProvRequest.toByteArray()))
                .build();

        KmsKeyProvClient.KeyProvIssueResult issueResult = new KmsKeyProvClient.KeyProvIssueResult(
                new byte[]{0x01, 0x02, 0x03},
                "key-001",
                1,
                "AES-256-GCM",
                System.currentTimeMillis() + 86400000,
                new byte[]{0x04, 0x05}
        );

        when(kmsKeyProvClient.issue(any(KmsKeyProvClient.KeyProvIssueRequest.class))).thenReturn(issueResult);
        doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION))
                .when(mqttClientManager).publish(anyString(), any(byte[].class), anyInt());

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE, result.errorCode());
    }

    @Test
    void processUplink_keyprov_invalidPayload_shouldFail() {
        EnvelopeProto.Envelope envelope = EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-keyprov-004")
                .setDeviceSn("DEVICE001")
                .setService("keyprov")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .setSeq(1)
                .setTtlMs(30000)
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{0x00, 0x01}))
                .build();

        UplinkService.ProcessResult result = uplinkService.processUplink(
                envelope.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.PAYLOAD_DECODE_FAILED, result.errorCode());
    }
}
