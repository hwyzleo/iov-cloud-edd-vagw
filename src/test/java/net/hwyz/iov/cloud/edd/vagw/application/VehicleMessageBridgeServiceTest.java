package net.hwyz.iov.cloud.edd.vagw.application;

import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.VehicleMessageDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.idempotency.VehicleBridgeInbox;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.EnvelopeDlqPublisher;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.EnvelopeKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.service.BindingResolution;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.InvalidateReason;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import vehicle.common.v1.Envelope;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 透明桥接编排服务测试（EDD-VAGW-DSN-CR-006 §6/§7/§9 + EDD-VAGW-DSN-CR-007 §6）。
 */
@ExtendWith(MockitoExtension.class)
class VehicleMessageBridgeServiceTest {

    @Mock
    private AccessIdentityValidator accessIdentityValidator;
    @Mock
    private VehicleEnvelopeValidator envelopeValidator;
    @Mock
    private BindingService bindingService;
    @Mock
    private SessionService sessionService;
    @Mock
    private VehicleBridgeInbox bridgeInbox;
    @Mock
    private EnvelopeKafkaProducer kafkaProducer;
    @Mock
    private EnvelopeDlqPublisher dlqPublisher;
    @Mock
    private VehicleMessageDownlinkPublisher downlinkPublisher;
    @Mock
    private GatewayDeliveryService gatewayDeliveryService;

    /** 真实路由目录（默认 CR-008 目标 Topic 名），供断言与 RouteEntry 使用 */
    @Spy
    private VehicleRouteCatalog routeCatalog = new VehicleRouteCatalog(new VagwFotaTopicProperties());

    @InjectMocks
    private VehicleMessageBridgeService bridgeService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(bridgeService, "uplinkProduceRetries", 3);
        ReflectionTestUtils.setField(bridgeService, "uplinkProduceRetryDelayMs", 0L);
    }

    // ---------- helpers ----------

    private Envelope.VehicleMessageEnvelope.Builder uplinkBase() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId("req-1")
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion("fota-v1")
                .setDeviceId("DEVICE001")
                .setVin("VIN-A")
                .setMessageId("msg-up-001")
                .setPayloadType("vehicle.fota.v1.TaskCheckRequest")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setService("vehicle.fota")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}));
    }

    private ConsumerRecord<String, byte[]> record(String vin, byte[] value) {
        return new ConsumerRecord<>(VehicleRouteCatalog.KAFKA_DOWN_TOPIC, 0, 10L, vin, value);
    }

    private Envelope.VehicleMessageEnvelope.Builder downlinkBase(String messageId) {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId("req-dn")
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion("fota-v1")
                .setDeviceId("DEVICE001")
                .setVin("VIN-A")
                .setMessageId(messageId)
                .setPayloadType("vehicle.fota.v1.TaskCheckResponse")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_RESPONSE)
                .setService("vehicle.fota")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{9, 9, 9}));
    }

    private BindingResolution resolvedBinding(String vin, String hsmUid) {
        return BindingResolution.resolved(VehicleAccessBinding.builder()
                .vin(vin)
                .hsmUid(hsmUid)
                .tboxSn("TBOX-1")
                .bindingVersion(1L)
                .bindingUpdatedAt(Instant.now())
                .source(BindingSource.ADMISSION)
                .build());
    }

    // ---------- 上行 ----------

    @Test
    void processUplink_success_shouldBridgeSameBytes() throws Exception {
        Envelope.VehicleMessageEnvelope e = uplinkBase().build();
        byte[] bytes = e.toByteArray();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(eq("DEVICE001"), eq("DEVICE001"))).thenReturn(null);
        when(bindingService.resolveByHsmUid("DEVICE001")).thenReturn(resolvedBinding("VIN-A", "DEVICE001"));
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.UPLINK, "msg-up-001"))
                .thenReturn(Optional.empty());
        when(kafkaProducer.sendUplink(any(), any(), any(), any(), any()))
                .thenReturn(new EnvelopeKafkaProducer.SendResult(0, 5));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(bytes, "DEVICE001");

        assertTrue(result.ok());
        verify(kafkaProducer).sendUplink(eq(routeCatalog.kafkaUpTopic()), eq("VIN-A"),
                eq(bytes), any(), any());
        verify(bridgeInbox).recordFinal(argThat(r ->
                "ACCEPTED".equals(r.getState()) && "msg-up-001".equals(r.getMessageId())));
    }

    @Test
    void processUplink_parseFailure_shouldMoveToUpDlq() {
        byte[] bad = new byte[]{0x00, 0x01, 0x02};

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(bad, "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.INVALID_ENVELOPE, result.errorCode());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq("DEVICE001"),
                eq(bad), contains("contract_invalid"));
        verifyNoInteractions(bindingService, kafkaProducer);
    }

    @Test
    void processUplink_identityMismatch_shouldMoveToUpDlq() {
        Envelope.VehicleMessageEnvelope e = uplinkBase().build();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(eq("OTHER"), eq("DEVICE001")))
                .thenReturn(new AccessIdentityValidator.Failure(
                        AccessIdentityValidator.Reason.DEVICE_MISMATCH, "identity mismatch"));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(e.toByteArray(), "OTHER");

        assertFalse(result.ok());
        assertEquals(ErrorCode.IDENTITY_MISMATCH, result.errorCode());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq("OTHER"),
                any(byte[].class), contains("identity_mismatch"));
        verifyNoInteractions(bindingService, kafkaProducer);
    }

    @Test
    void processUplink_bindingContextMissing_shouldMoveToUpDlq() {
        Envelope.VehicleMessageEnvelope e = uplinkBase().build();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(any(), any())).thenReturn(null);
        when(bindingService.resolveByHsmUid("DEVICE001"))
                .thenReturn(BindingResolution.contextMissing("no session binding"));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(e.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.BINDING_CONTEXT_MISSING, result.errorCode());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq("DEVICE001"),
                any(byte[].class), contains("binding_context_missing"));
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void processUplink_bindingDependencyUnavailable_shouldMoveToUpDlq() {
        Envelope.VehicleMessageEnvelope e = uplinkBase().build();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(any(), any())).thenReturn(null);
        when(bindingService.resolveByHsmUid("DEVICE001"))
                .thenReturn(BindingResolution.dependencyUnavailable("DEPENDENCY_UNAVAILABLE"));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(e.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.BINDING_DEPENDENCY_UNAVAILABLE, result.errorCode());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq("DEVICE001"),
                any(byte[].class), contains("binding_dependency_unavailable"));
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void processUplink_envelopeVinMismatchBinding_shouldMoveToUpDlq() {
        Envelope.VehicleMessageEnvelope e = uplinkBase().setVin("VIN-B").build();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(any(), any())).thenReturn(null);
        when(bindingService.resolveByHsmUid("DEVICE001")).thenReturn(resolvedBinding("VIN-A", "DEVICE001"));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(e.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.IDENTITY_MISMATCH, result.errorCode());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq("DEVICE001"),
                any(byte[].class), contains("vin_mismatch_binding"));
    }

    @Test
    void processUplink_duplicateSameHash_shouldSkip() {
        Envelope.VehicleMessageEnvelope e = uplinkBase().build();
        byte[] bytes = e.toByteArray();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(any(), any())).thenReturn(null);
        when(bindingService.resolveByHsmUid("DEVICE001")).thenReturn(resolvedBinding("VIN-A", "DEVICE001"));
        String digest = VehicleBridgeInbox.digestOf(bytes);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.UPLINK, "msg-up-001"))
                .thenReturn(Optional.of(VehicleBridgeInbox.InboxRecord.builder()
                        .state("ACCEPTED").envelopeSha256(digest).build()));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(bytes, "DEVICE001");

        assertTrue(result.ok());
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    void processUplink_produceFailure_shouldRetryThenMoveToUpDlq() throws Exception {
        Envelope.VehicleMessageEnvelope e = uplinkBase().build();
        when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        when(accessIdentityValidator.validate(any(), any())).thenReturn(null);
        when(bindingService.resolveByHsmUid("DEVICE001")).thenReturn(resolvedBinding("VIN-A", "DEVICE001"));
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.UPLINK, "msg-up-001"))
                .thenReturn(Optional.empty());
        when(kafkaProducer.sendUplink(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("kafka down"));

        VehicleMessageBridgeService.UplinkResult result =
                bridgeService.processUplink(e.toByteArray(), "DEVICE001");

        assertFalse(result.ok());
        assertEquals(ErrorCode.ROUTE_UNAVAILABLE, result.errorCode());
        verify(kafkaProducer, times(3)).sendUplink(any(), any(), any(), any(), any());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq("VIN-A"),
                any(byte[].class), contains("produce_retries_exceeded"));
        verify(bridgeInbox).recordFinal(argThat(r -> "DLQED".equals(r.getState())));
    }

    // ---------- 下行 ----------

    @Test
    void processDownlink_deliverable_shouldPublishSameBytes() throws Exception {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-001").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-001"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(resolvedBinding(vin, "DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(downlinkPublisher).publish(eq("vehicle/DEVICE001/down/fota"), eq(rec.value()));
        verify(bridgeInbox).recordFinal(argThat(r ->
                "ACCEPTED".equals(r.getState()) && "OUTCOME_ACCEPTED".equals(r.getOutcome())));
        verifyNoInteractions(gatewayDeliveryService);
    }

    @Test
    void processDownlink_vehicleOffline_shouldProduceRejected() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-002").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-002"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(resolvedBinding(vin, "DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(false);
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin), eq(DeliveryReason.VEHICLE_OFFLINE)))
                .thenReturn("msg-dn-002");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.VEHICLE_OFFLINE);
        verify(bridgeInbox).recordFinal(argThat(r ->
                "REJECTED".equals(r.getState()) && "OUTCOME_REJECTED".equals(r.getOutcome())));
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    void processDownlink_vinUnbound_shouldProduceRejected() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-003").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-003"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(BindingResolution.unbound("UNBOUND"));
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin), eq(DeliveryReason.VIN_UNBOUND)))
                .thenReturn("msg-dn-003");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.VIN_UNBOUND);
    }

    @Test
    void processDownlink_bindingConflict_shouldProduceRejected() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-009").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-009"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(BindingResolution.conflict());
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin), eq(DeliveryReason.BINDING_CONFLICT)))
                .thenReturn("msg-dn-009");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.BINDING_CONFLICT);
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    void processDownlink_bindingDependencyUnavailable_shouldProduceRejected() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-010").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-010"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin))
                .thenReturn(BindingResolution.dependencyUnavailable("DEPENDENCY_UNAVAILABLE"));
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin),
                eq(DeliveryReason.BINDING_DEPENDENCY_UNAVAILABLE))).thenReturn("msg-dn-010");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.BINDING_DEPENDENCY_UNAVAILABLE);
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    void processDownlink_deviceMismatch_shouldInvalidateAndProduceRejected() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-011").setDeviceId("OTHER001").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-011"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(resolvedBinding(vin, "DEVICE001"));
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin), eq(DeliveryReason.DEVICE_MISMATCH)))
                .thenReturn("msg-dn-011");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(bindingService).invalidate(eq(vin), eq("DEVICE001"), eq(InvalidateReason.IDENTITY_MISMATCH));
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.DEVICE_MISMATCH);
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    void processDownlink_expired_shouldProduceMessageExpired() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-004").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any()))
                .thenReturn(new VehicleEnvelopeValidator.Failure(
                        VehicleEnvelopeValidator.Reason.MESSAGE_EXPIRED, "expired"));
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin), eq(DeliveryReason.MESSAGE_EXPIRED)))
                .thenReturn("msg-dn-004");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.MESSAGE_EXPIRED);
        verifyNoInteractions(bindingService, downlinkPublisher);
    }

    @Test
    void processDownlink_contractInvalid_shouldMoveToDownDlq() {
        ConsumerRecord<String, byte[]> rec = record("VIN-A", new byte[]{0x00, 0x01});
        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(dlqPublisher).publish(eq(routeCatalog.downDlqTopic()), eq("VIN-A"),
                any(byte[].class), contains("contract_invalid"));
    }

    @Test
    void processDownlink_digestConflict_shouldMoveToDownDlq() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-005").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-005"))
                .thenReturn(Optional.of(VehicleBridgeInbox.InboxRecord.builder()
                        .state("ACCEPTED").envelopeSha256("different-digest").build()));

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(dlqPublisher).publish(eq(routeCatalog.downDlqTopic()), eq(vin),
                any(byte[].class), contains("digest_conflict"));
    }

    @Test
    void processDownlink_duplicate_shouldSkip() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-006").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());
        String digest = VehicleBridgeInbox.digestOf(rec.value());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-006"))
                .thenReturn(Optional.of(VehicleBridgeInbox.InboxRecord.builder()
                        .state("ACCEPTED").envelopeSha256(digest).build()));

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verifyNoInteractions(bindingService, sessionService, downlinkPublisher, gatewayDeliveryService);
    }

    @Test
    void processDownlink_mqttPublishFailure_shouldProduceRejected() throws Exception {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-007").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-007"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(resolvedBinding(vin, "DEVICE001"));
        when(sessionService.isOnlineByDeviceSn("DEVICE001")).thenReturn(true);
        doThrow(new RuntimeException("mqtt down")).when(downlinkPublisher).publish(anyString(), any());
        when(gatewayDeliveryService.produceRejected(eq(e), eq(vin), eq(DeliveryReason.MQTT_PUBLISH_FAILED)))
                .thenReturn("msg-dn-007");

        boolean processed = bridgeService.processDownlink(rec);

        assertTrue(processed);
        verify(gatewayDeliveryService).produceRejected(e, vin, DeliveryReason.MQTT_PUBLISH_FAILED);
        verify(bridgeInbox).recordFinal(argThat(r -> "REJECTED".equals(r.getState())));
    }

    @Test
    void processDownlink_deliveryProduceFailure_shouldNotCommit() {
        String vin = "VIN-A";
        Envelope.VehicleMessageEnvelope e = downlinkBase("msg-dn-008").build();
        ConsumerRecord<String, byte[]> rec = record(vin, e.toByteArray());

        when(envelopeValidator.validateDownlink(any(), any(), any())).thenReturn(null);
        when(bridgeInbox.findFinal(VehicleBridgeInbox.Direction.DOWNLINK, "msg-dn-008"))
                .thenReturn(Optional.empty());
        when(bindingService.resolveByVin(vin)).thenReturn(BindingResolution.unbound("UNBOUND"));
        when(gatewayDeliveryService.produceRejected(any(), any(), any()))
                .thenThrow(new net.hwyz.iov.cloud.edd.vagw.infrastructure.VehicleBridgeException("kafka down"));

        boolean processed = bridgeService.processDownlink(rec);

        assertFalse(processed);
        verifyNoInteractions(downlinkPublisher);
    }
}
