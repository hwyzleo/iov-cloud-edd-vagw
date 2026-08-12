package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.infrastructure.FotaBridgeException;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.FotaBridgeInbox;
import net.hwyz.iov.cloud.edd.vagw.kafka.FotaDlqPublisher;
import net.hwyz.iov.cloud.edd.vagw.kafka.FotaKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.mqtt.MqttClientManager;
import net.hwyz.iov.cloud.edd.vagw.proto.EnvelopeProto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * FotaBridgeAppService 单元测试（EDD-VAGW-DSN-CR-005 §5/§6/§8）。
 */
@ExtendWith(MockitoExtension.class)
class FotaBridgeAppServiceTest {

    @Mock
    private BindingService bindingService;
    @Mock
    private SessionService sessionService;
    @Mock
    private MqttClientManager mqttClientManager;
    @Spy
    private FotaEnvelopeValidator envelopeValidator = new FotaEnvelopeValidator();
    @Mock
    private FotaDeliveryResultService deliveryResultService;
    @Mock
    private FotaKafkaProducer fotaKafkaProducer;
    @Mock
    private FotaDlqPublisher dlqPublisher;
    @Mock
    private FotaBridgeInbox fotaBridgeInbox;

    @InjectMocks
    private FotaBridgeAppService fotaBridgeAppService;

    // ---------- helpers ----------

    private EnvelopeProto.Envelope validUplink() {
        return EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId("msg-up-001")
                .setDeviceSn("DEVICE001")
                .setService("fota")
                .setMsgType(EnvelopeProto.MsgType.UP_DATA)
                .setTs(System.currentTimeMillis())
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
    }

    private EnvelopeProto.Envelope validDownlink(String messageId, String vin, String deviceSn,
                                                 long ts, int ttlMs) {
        return EnvelopeProto.Envelope.newBuilder()
                .setVer(1)
                .setMsgId(messageId)
                .setDeviceSn(deviceSn)
                .setService("fota")
                .setMsgType(EnvelopeProto.MsgType.DOWN_CMD)
                .setTs(ts)
                .setTtlMs(ttlMs)
                .setVin(vin)
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{9, 9, 9}))
                .build();
    }

    private ConsumerRecord<String, byte[]> record(String vin, byte[] value) {
        return new ConsumerRecord<>("iov.vagw.down.fota", 0, 10L, vin, value);
    }

    // ---------- 上行 ----------

    @Test
    void processFotaUplink_success_shouldBridgeToKafka() throws Exception {
        EnvelopeProto.Envelope e = validUplink();
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("VIN-A"));
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.UPLINK, "msg-up-001"))
                .thenReturn(Optional.empty());
        when(fotaKafkaProducer.sendFotaUplink(eq(FotaRouteConfig.KAFKA_UP_TOPIC), eq("VIN-A"),
                any(), any(), any())).thenReturn(new FotaKafkaProducer.SendResult(0, 5));

        FotaBridgeAppService.FotaUplinkResult result =
                fotaBridgeAppService.processFotaUplink(e, "DEVICE001");

        assertTrue(result.ok());
        verify(fotaKafkaProducer).sendFotaUplink(eq(FotaRouteConfig.KAFKA_UP_TOPIC), eq("VIN-A"),
                eq(e), any(), any());
        verify(fotaBridgeInbox).recordFinal(argThat(r -> "ACCEPTED".equals(r.getStatus())));
    }

    @Test
    void processFotaUplink_vinUnbound_shouldReject() {
        EnvelopeProto.Envelope e = validUplink();
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.empty());

        FotaBridgeAppService.FotaUplinkResult result =
                fotaBridgeAppService.processFotaUplink(e, "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.VIN_UNAUTHORIZED, result.errorCode());
        verifyNoInteractions(fotaKafkaProducer);
    }

    @Test
    void processFotaUplink_deviceMismatch_shouldRejectIdentityMismatch() {
        EnvelopeProto.Envelope e = validUplink();

        FotaBridgeAppService.FotaUplinkResult result =
                fotaBridgeAppService.processFotaUplink(e, "OTHER");

        assertFalse(result.ok());
        assertEquals(ErrorCode.IDENTITY_MISMATCH, result.errorCode());
        verifyNoInteractions(bindingService);
        verifyNoInteractions(fotaKafkaProducer);
    }

    @Test
    void processFotaUplink_produceFailure_shouldRetryThenMoveToDlq() throws Exception {
        EnvelopeProto.Envelope e = validUplink();
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("VIN-A"));
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.UPLINK, "msg-up-001"))
                .thenReturn(Optional.empty());
        when(fotaKafkaProducer.sendFotaUplink(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("kafka down"));

        FotaBridgeAppService.FotaUplinkResult result =
                fotaBridgeAppService.processFotaUplink(e, "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE, result.errorCode());
        verify(fotaKafkaProducer, times(3)).sendFotaUplink(any(), any(), any(), any(), any());
        verify(dlqPublisher).publish(eq(FotaRouteConfig.UP_DLQ_TOPIC), eq("VIN-A"), any(byte[].class), anyString());
        verify(fotaBridgeInbox).recordFinal(argThat(r -> "DLQED".equals(r.getStatus())));
    }

    @Test
    void processFotaUplink_duplicate_shouldSkip() {
        EnvelopeProto.Envelope e = validUplink();
        when(bindingService.resolveVin("DEVICE001")).thenReturn(Optional.of("VIN-A"));
        String digest = FotaBridgeInbox.digestOf(e.getPayload().toByteArray());
        FotaBridgeInbox.InboxRecord existing = FotaBridgeInbox.InboxRecord.builder()
                .status("ACCEPTED").payloadDigest(digest).build();
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.UPLINK, "msg-up-001"))
                .thenReturn(Optional.of(existing));

        FotaBridgeAppService.FotaUplinkResult result =
                fotaBridgeAppService.processFotaUplink(e, "DEVICE001");

        assertTrue(result.ok());
        verifyNoInteractions(fotaKafkaProducer);
    }

    // ---------- 下行 ----------

    @Test
    void processFotaDownlink_deliverable_shouldPublishAndAccept() throws Exception {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-001", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(bindingService.resolveDeviceSn(vin)).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-001"))
                .thenReturn(Optional.empty());

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(mqttClientManager).publish(eq("vehicle/DEVICE001/down/fota"), any(byte[].class), eq(1));
        verify(fotaBridgeInbox).recordFinal(argThat(r -> "ACCEPTED".equals(r.getStatus())));
        verifyNoInteractions(deliveryResultService);
    }

    @Test
    void processFotaDownlink_vehicleOffline_shouldReject() {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-002", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(bindingService.resolveDeviceSn(vin)).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(false);
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-002"))
                .thenReturn(Optional.empty());
        when(deliveryResultService.produceDeliveryRejected(any(), eq(vin), eq("DEVICE001"),
                eq(DeliveryReason.VEHICLE_OFFLINE))).thenReturn("result-001");

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(deliveryResultService).produceDeliveryRejected(e, vin, "DEVICE001",
                DeliveryReason.VEHICLE_OFFLINE);
        verify(fotaBridgeInbox).recordFinal(argThat(r -> "REJECTED".equals(r.getStatus())));
        verifyNoInteractions(mqttClientManager);
    }

    @Test
    void processFotaDownlink_vinUnbound_shouldReject() {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-003", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(bindingService.resolveDeviceSn(vin)).thenReturn(Optional.empty());
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-003"))
                .thenReturn(Optional.empty());
        when(deliveryResultService.produceDeliveryRejected(any(), eq(vin), isNull(),
                eq(DeliveryReason.VIN_UNBOUND))).thenReturn("result-002");

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(deliveryResultService).produceDeliveryRejected(e, vin, null, DeliveryReason.VIN_UNBOUND);
    }

    @Test
    void processFotaDownlink_ttlExpired_shouldRejectMessageExpired() {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-004", vin, "DEVICE001",
                System.currentTimeMillis() - 60_000, 10_000);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(deliveryResultService.produceDeliveryRejected(any(), eq(vin), eq("DEVICE001"),
                eq(DeliveryReason.MESSAGE_EXPIRED))).thenReturn("result-003");

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(deliveryResultService).produceDeliveryRejected(e, vin, "DEVICE001",
                DeliveryReason.MESSAGE_EXPIRED);
        verifyNoInteractions(bindingService);
    }

    @Test
    void processFotaDownlink_contractInvalid_shouldMoveToDlq() {
        ConsumerRecord<String, byte[]> rec = record("VIN-A", new byte[]{0x00, 0x01});

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(dlqPublisher).publish(eq(FotaRouteConfig.DOWN_DLQ_TOPIC), eq("VIN-A"),
                any(byte[].class), contains("contract_invalid"));
    }

    @Test
    void processFotaDownlink_duplicate_shouldSkip() {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-005", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        String digest = FotaBridgeInbox.digestOf(e.getPayload().toByteArray());
        FotaBridgeInbox.InboxRecord existing = FotaBridgeInbox.InboxRecord.builder()
                .status("ACCEPTED").payloadDigest(digest).build();
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-005"))
                .thenReturn(Optional.of(existing));

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verifyNoInteractions(bindingService, sessionService, mqttClientManager, deliveryResultService);
    }

    @Test
    void processFotaDownlink_digestConflict_shouldMoveToDlq() {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-006", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        FotaBridgeInbox.InboxRecord existing = FotaBridgeInbox.InboxRecord.builder()
                .status("ACCEPTED").payloadDigest("different-digest").build();
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-006"))
                .thenReturn(Optional.of(existing));

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(dlqPublisher).publish(eq(FotaRouteConfig.DOWN_DLQ_TOPIC), eq(vin),
                any(byte[].class), contains("digest_conflict"));
    }

    @Test
    void processFotaDownlink_mqttPublishFailure_shouldReject() throws Exception {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-007", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(bindingService.resolveDeviceSn(vin)).thenReturn(Optional.of("DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-007"))
                .thenReturn(Optional.empty());
        doThrow(new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION))
                .when(mqttClientManager).publish(anyString(), any(byte[].class), anyInt());
        when(deliveryResultService.produceDeliveryRejected(any(), eq(vin), eq("DEVICE001"),
                eq(DeliveryReason.MQTT_PUBLISH_FAILED))).thenReturn("result-004");

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertTrue(processed);
        verify(deliveryResultService).produceDeliveryRejected(e, vin, "DEVICE001",
                DeliveryReason.MQTT_PUBLISH_FAILED);
    }

    @Test
    void processFotaDownlink_deliveryRejectedProduceFailure_shouldNotCommit() {
        String vin = "VIN-A";
        EnvelopeProto.Envelope e = validDownlink("msg-dn-008", vin, "DEVICE001",
                System.currentTimeMillis(), 0);
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(bindingService.resolveDeviceSn(vin)).thenReturn(Optional.empty());
        when(fotaBridgeInbox.findFinal(FotaBridgeInbox.Direction.DOWNLINK, "msg-dn-008"))
                .thenReturn(Optional.empty());
        when(deliveryResultService.produceDeliveryRejected(any(), eq(vin), isNull(),
                eq(DeliveryReason.VIN_UNBOUND)))
                .thenThrow(new FotaBridgeException("kafka down"));

        boolean processed = fotaBridgeAppService.processFotaDownlink(rec);

        assertFalse(processed);
        verifyNoInteractions(mqttClientManager);
    }
}
