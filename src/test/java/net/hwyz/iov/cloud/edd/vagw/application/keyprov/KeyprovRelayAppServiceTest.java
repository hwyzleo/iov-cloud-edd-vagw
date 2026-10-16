package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.VmdKeyProvisioningGateway;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovInbox;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * keyprov relay 编排测试（EDD-VAGW-DSN-CR-011 §3.4/§7/§11）。
 */
@ExtendWith(MockitoExtension.class)
class KeyprovRelayAppServiceTest {

    private static final String REQ_TYPE = "security.keyprov.v1.KeyProvisioningRequest";
    private static final String RESP_TYPE = "security.keyprov.v1.KeyProvisioningResponse";
    private static final String ACK_RESULT_TYPE = "security.keyprov.v1.KeyProvisioningAckResult";

    @Mock
    private KeyprovContractGuard contractGuard;
    @Mock
    private KeyprovProtocolValidator protocolValidator;
    @Mock
    private KeyprovInbox inbox;
    @Mock
    private VmdKeyProvisioningGateway vmdGateway;
    @Mock
    private KeyprovEnvelopeFactory envelopeFactory;
    @Mock
    private KeyprovMqttDownlinkPublisher downlinkPublisher;
    @Mock
    private SessionService sessionService;

    private final KeyprovMetrics metrics = new KeyprovMetrics();
    private KeyprovProperties properties;
    private KeyprovRelayAppService service;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.setEnabled(true);
        service = new KeyprovRelayAppService(properties, contractGuard, protocolValidator, inbox,
                vmdGateway, envelopeFactory, downlinkPublisher, sessionService, metrics);
    }

    private BrokerAuthenticatedPublishContext context() {
        return new BrokerAuthenticatedPublishContext(KeyprovContextSource.ACL_SESSION_CORRELATED,
                "DEVICE001", "client001", "cert-1", null, 3L, "client001", Instant.now());
    }

    private Envelope.VehicleMessageEnvelope uplink() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov")
                .setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001")
                .setMessageId("up-msg-001")
                .setRequestId("req-001")
                .setPayloadType(REQ_TYPE)
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
    }

    private void stubDeliveryPath(String downlinkType, byte[] payload) {
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(new VmdRelayResult("req-001", downlinkType, payload,
                123L, KeyprovOutcome.SUCCESS, null));
        when(protocolValidator.isAllowedDownlinkPayloadType(downlinkType)).thenReturn(true);
        when(sessionService.isGenerationCurrent("DEVICE001", 3L)).thenReturn(true);
        when(envelopeFactory.buildDownlink(any(), any(), eq(downlinkType), eq(payload), eq(123L)))
                .thenReturn(uplink());
        try {
            when(downlinkPublisher.publish(eq("DEVICE001"), any(byte[].class)))
                    .thenReturn("vehicle/DEVICE001/down/keyprov");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void relay_disabled_shouldFailClosedWithoutVmd() {
        properties.setEnabled(false);
        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.KEYPROV_DISABLED, result.reason());
        verifyNoInteractions(vmdGateway);
    }

    @Test
    void relay_contractIncompatible_shouldFailClosedWithoutVmd() {
        when(contractGuard.compatible()).thenReturn(false);
        when(contractGuard.incompatibilityReason()).thenReturn("drift");

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED, result.reason());
        verifyNoInteractions(vmdGateway);
    }

    @Test
    void relay_validatorFailure_shouldNotCallVmd() {
        when(contractGuard.compatible()).thenReturn(true);
        when(protocolValidator.validateUplink(any(), any(), any()))
                .thenReturn(new KeyprovProtocolValidator.Failure(KeyprovReason.KEYPROV_PAYLOAD_INVALID, "bad"));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID, result.reason());
        assertEquals(ErrorCode.KEYPROV_UPSTREAM_FAILURE, result.errorCode());
        verifyNoInteractions(vmdGateway);
    }

    @Test
    void relay_identityMismatch_shouldMapTo804003() {
        when(contractGuard.compatible()).thenReturn(true);
        when(protocolValidator.validateUplink(any(), any(), any()))
                .thenReturn(new KeyprovProtocolValidator.Failure(KeyprovReason.IDENTITY_MISMATCH, "mismatch"));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.IDENTITY_MISMATCH, result.reason());
        assertEquals(ErrorCode.IDENTITY_MISMATCH, result.errorCode());
    }

    @Test
    void relay_duplicateMessage_shouldSkipVmd() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.DUPLICATE);

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertTrue(result.delivered());
        verifyNoInteractions(vmdGateway);
    }

    @Test
    void relay_digestConflict_shouldFailClosed() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.CONFLICT);

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.KEYPROV_PAYLOAD_INVALID, result.reason());
        verifyNoInteractions(vmdGateway);
    }

    @Test
    void relay_requestToResponse_shouldPublishDownlink() throws Exception {
        when(contractGuard.compatible()).thenReturn(true);
        stubDeliveryPath(RESP_TYPE, new byte[]{9, 9});

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertTrue(result.delivered());
        assertEquals("vehicle/DEVICE001/down/keyprov", result.downlinkTopic());
        verify(downlinkPublisher).publish(eq("DEVICE001"), any(byte[].class));
    }

    @Test
    void relay_ackToAckResult_shouldPublishDownlink() throws Exception {
        when(contractGuard.compatible()).thenReturn(true);
        Envelope.VehicleMessageEnvelope ack = Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov").setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001").setMessageId("ack-msg-001").setRequestId("req-001")
                .setPayloadType("security.keyprov.v1.KeyProvisioningAck")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1})).build();
        when(inbox.record(eq("ack-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(new VmdRelayResult("req-001", ACK_RESULT_TYPE,
                new byte[]{7}, null, KeyprovOutcome.SUCCESS, null));
        when(protocolValidator.isAllowedDownlinkPayloadType(ACK_RESULT_TYPE)).thenReturn(true);
        when(sessionService.isGenerationCurrent("DEVICE001", 3L)).thenReturn(true);
        when(envelopeFactory.buildDownlink(any(), any(), eq(ACK_RESULT_TYPE), any(), any())).thenReturn(ack);
        when(downlinkPublisher.publish(eq("DEVICE001"), any(byte[].class))).thenReturn("vehicle/DEVICE001/down/keyprov");

        KeyprovRelayResult result = service.relay(context(), ack, ack.toByteArray());

        assertTrue(result.delivered());
        verify(downlinkPublisher).publish(eq("DEVICE001"), any(byte[].class));
    }

    @Test
    void relay_wrongDownlinkType_shouldFailClosedAndNotPublish() throws Exception {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(new VmdRelayResult("req-001", ACK_RESULT_TYPE, new byte[]{9},
                123L, KeyprovOutcome.SUCCESS, null));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED, result.reason());
        verify(downlinkPublisher, never()).publish(anyString(), any(byte[].class));
    }

    @Test
    void relay_vmdRejectedNoPayload_shouldFailWithRejected() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(VmdRelayResult.rejected("req-001", KeyprovReason.KEYPROV_VMD_REJECTED));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertFalse(result.delivered());
        assertEquals(KeyprovReason.KEYPROV_VMD_REJECTED, result.reason());
    }

    @Test
    void relay_vmdUnavailable_shouldFailClosed() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertEquals(KeyprovReason.KEYPROV_VMD_UNAVAILABLE, result.reason());
    }

    @Test
    void relay_vmdOutcomeUnknown_shouldNotCreateNewRequestId() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(VmdRelayResult.outcomeUnknown("req-001", KeyprovReason.KEYPROV_OUTCOME_UNKNOWN));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertEquals(KeyprovReason.KEYPROV_OUTCOME_UNKNOWN, result.reason());
        assertEquals("up-msg-001", result.messageId());
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    void relay_vmdThrows_shouldFailWithUnavailable() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenThrow(new RuntimeException("timeout"));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertEquals(KeyprovReason.KEYPROV_VMD_UNAVAILABLE, result.reason());
    }

    @Test
    void relay_vmdIdentityMismatchResponse_shouldKeepVmdReason() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(VmdRelayResult.rejected("req-001",
                KeyprovReason.KEYPROV_IDENTITY_MISMATCH));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertEquals(KeyprovReason.KEYPROV_IDENTITY_MISMATCH, result.reason());
    }

    @Test
    void relay_generationStale_shouldFailClosedAndNotPublish() {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(new VmdRelayResult("req-001", RESP_TYPE, new byte[]{1},
                123L, KeyprovOutcome.SUCCESS, null));
        when(protocolValidator.isAllowedDownlinkPayloadType(RESP_TYPE)).thenReturn(true);
        when(sessionService.isGenerationCurrent("DEVICE001", 3L)).thenReturn(false);

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertEquals(KeyprovReason.KEYPROV_SESSION_STALE, result.reason());
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    void relay_downlinkPublishFails_shouldFailClosed() throws Exception {
        when(contractGuard.compatible()).thenReturn(true);
        when(inbox.record(eq("up-msg-001"), anyString())).thenReturn(KeyprovInbox.State.NEW);
        when(vmdGateway.relay(any())).thenReturn(new VmdRelayResult("req-001", RESP_TYPE, new byte[]{1},
                123L, KeyprovOutcome.SUCCESS, null));
        when(protocolValidator.isAllowedDownlinkPayloadType(RESP_TYPE)).thenReturn(true);
        when(sessionService.isGenerationCurrent("DEVICE001", 3L)).thenReturn(true);
        when(envelopeFactory.buildDownlink(any(), any(), eq(RESP_TYPE), any(), any())).thenReturn(uplink());
        when(downlinkPublisher.publish(eq("DEVICE001"), any(byte[].class)))
                .thenThrow(new RuntimeException("mqtt down"));

        KeyprovRelayResult result = service.relay(context(), uplink(), uplink().toByteArray());

        assertEquals(KeyprovReason.KEYPROV_DOWNLINK_PUBLISH_FAILED, result.reason());
        verify(inbox).forget("up-msg-001");
    }
}
