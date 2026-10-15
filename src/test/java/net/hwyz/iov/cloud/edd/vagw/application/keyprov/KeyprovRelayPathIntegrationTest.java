package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttIngressAdapter;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.VmdKeyProvisioningClient;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.VmdRelayResponse;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovInbox;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResponse;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResult;
import net.hwyz.iov.security.keyprov.v1.WrappedKeyItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * keyprov relay 全链路集成测试（EDD-VAGW-DSN-CR-011 §4.4 时序）。
 * <p>
 * 串联真实组件：ingress 适配器 → 认证上下文解析器 → 协议校验器 → relay 编排 →
 * Envelope 工厂 → 下行发布；仅 mock 会话注册表、VMD SPI 与 MQTT 发布器的底层通道。
 * 覆盖 Request→Response 端到端、canonical Envelope 关联、能力关闭隔离。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class KeyprovRelayPathIntegrationTest {

    private static final String REQ_TYPE = "security.keyprov.v1.KeyProvisioningRequest";
    private static final String RESP_TYPE = "security.keyprov.v1.KeyProvisioningResponse";

    @Mock
    private SessionService sessionService;
    @Mock
    private VmdKeyProvisioningClient vmdClient;
    @Mock
    private KeyprovMqttDownlinkPublisher downlinkPublisher;

    private KeyprovProperties properties;
    private KeyprovMqttIngressAdapter adapter;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.setEnabled(true);

        KeyprovContractGuard guard = new KeyprovContractGuard();
        guard.verify();
        assertTrue(guard.compatible(), "proto-keyprov contract must be compatible in test classpath");

        AuthenticatedPublishContextResolver resolver = new AuthenticatedPublishContextResolver(sessionService);
        KeyprovProtocolValidator validator = new KeyprovProtocolValidator(properties, guard);
        KeyprovInbox inbox = new KeyprovInbox(properties);
        KeyprovEnvelopeFactory envelopeFactory = new KeyprovEnvelopeFactory(properties);
        KeyprovRelayAppService relay = new KeyprovRelayAppService(properties, guard, validator, inbox,
                vmdClient, envelopeFactory, downlinkPublisher, sessionService);
        adapter = new KeyprovMqttIngressAdapter(properties, guard, resolver, relay,
                new VagwMqttSubscriptionMetrics());
    }

    private SessionInfo onlineSession() {
        return SessionInfo.builder()
                .deviceSn("DEVICE001").clientId("client001").online(true)
                .connectionGeneration(2L).certSerial("cert-1").build();
    }

    private byte[] uplinkEnvelope() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov")
                .setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001")
                .setMessageId("up-msg-001")
                .setRequestId("req-001")
                .setPayloadType(REQ_TYPE)
                .setPayload(KeyProvisioningRequest.newBuilder()
                        .setRequestId("req-001").setIdempotencyKey("idem-001").build().toByteString())
                .build().toByteArray();
    }

    @Test
    @DisplayName("Request→Response：认证上下文校验通过后转发 VMD 并发布 canonical down/keyprov")
    void requestToResponse_endToEnd() throws Exception {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.of(onlineSession()));
        when(sessionService.isGenerationCurrent("DEVICE001", 2L)).thenReturn(true);

        byte[] wrappedKey = "WRAPPED-KEY".getBytes();
        byte[] canonicalResponse = KeyProvisioningResponse.newBuilder()
                .setRequestId("req-001")
                .setResult(KeyProvisioningResult.KEY_PROVISIONING_RESULT_SUCCEEDED)
                .addItems(WrappedKeyItem.newBuilder()
                        .setKeyId("key-1").setBusinessKeyVersion(1).setWrappedKey(
                                com.google.protobuf.ByteString.copyFrom(wrappedKey)))
                .setExpireAtMs(1900000000000L)
                .build().toByteArray();
        when(vmdClient.relay(any())).thenReturn(new VmdRelayResponse("req-001", RESP_TYPE,
                canonicalResponse, 1900000000000L, "DEVICE001", KeyprovOutcome.SUCCESS, null));

        ArgumentCaptor<byte[]> published = ArgumentCaptor.forClass(byte[].class);
        when(downlinkPublisher.publish(eq("DEVICE001"), published.capture()))
                .thenReturn("vehicle/DEVICE001/down/keyprov");

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", uplinkEnvelope());

        verify(vmdClient).relay(any());
        verify(downlinkPublisher).publish(eq("DEVICE001"), any(byte[].class));

        Envelope.VehicleMessageEnvelope down = Envelope.VehicleMessageEnvelope.parseFrom(published.getValue());
        assertEquals("security.keyprov", down.getService());
        assertEquals("keyprov-v1", down.getProtocolVersion());
        assertEquals(Envelope.MessageKind.MESSAGE_KIND_RESPONSE, down.getMessageKind());
        assertEquals("req-001", down.getRequestId());
        assertEquals("DEVICE001", down.getDeviceId());
        assertEquals("up-msg-001", down.getCorrelationId());
        assertEquals(RESP_TYPE, down.getPayloadType());
        assertEquals(1900000000000L, down.getExpireAtMs());
        assertNotEquals("up-msg-001", down.getMessageId(), "下行必须生成新 messageId");
        assertArrayEquals(canonicalResponse, down.getPayload().toByteArray(),
                "VAGW 必须原样透传 VMD canonical payload");
    }

    @Test
    @DisplayName("能力关闭：keyprov 上行不调用 VMD（与 FOTA/远控隔离）")
    void disabled_isolation() {
        properties.setEnabled(false);

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", uplinkEnvelope());

        verifyNoInteractions(vmdClient);
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    @DisplayName("身份不一致：Topic/会话与 Envelope.device_id 不一致时 VMD 调用次数为 0")
    void identityMismatch_noVmdCall() throws Exception {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.of(onlineSession()));

        byte[] forged = Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov").setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("OTHER-DEVICE")
                .setMessageId("up-msg-002").setRequestId("req-002").setPayloadType(REQ_TYPE)
                .setPayload(KeyProvisioningRequest.newBuilder().setRequestId("req-002").build().toByteString())
                .build().toByteArray();

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", forged);

        verifyNoInteractions(vmdClient);
        verifyNoInteractions(downlinkPublisher);
    }
}
