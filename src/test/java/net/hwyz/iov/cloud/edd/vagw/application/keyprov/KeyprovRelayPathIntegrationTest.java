package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttIngressAdapter;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovAdmissionBulkhead;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovExecutor;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovInbox;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import net.hwyz.iov.security.keyprov.v1.KeyEnvelopeFormat;
import net.hwyz.iov.security.keyprov.v1.KeyImportItemResult;
import net.hwyz.iov.security.keyprov.v1.KeyImportResult;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningAck;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResponse;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResult;
import net.hwyz.iov.security.keyprov.v1.KeyPurpose;
import net.hwyz.iov.security.keyprov.v1.WrappedKeyItem;
import net.hwyz.iov.security.keyprov.v1.WrappingSuite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * keyprov relay 全链路集成测试（EDD-VAGW-DSN-CR-011 §4.4 时序，由 EDD-VAGW-DSN-CR-016 增量修订）。
 * <p>
 * 串联真实组件：ingress 适配器 → 认证上下文解析器 → 协议校验器 → bulkhead → 专属执行器 →
 * relay 编排 → Envelope 工厂 → 下行发布；仅 mock 会话注册表、VMD Gateway 与 MQTT 发布器的底层通道。
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
    private VmdKeyProvisioningGateway vmdGateway;
    @Mock
    private KeyprovMqttDownlinkPublisher downlinkPublisher;

    private KeyprovProperties properties;
    private KeyprovExecutor executor;
    private KeyprovMqttIngressAdapter adapter;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.setEnabled(true);
        properties.setMaxInFlight(4);
        properties.setQueueCapacity(0);
        properties.getVmd().setDeadline(Duration.ofSeconds(2));

        KeyprovContractGuard guard = new KeyprovContractGuard();
        guard.verify();
        assertTrue(guard.compatible(), "proto-keyprov contract must be compatible in test classpath");

        KeyprovMetrics metrics = new KeyprovMetrics();
        AuthenticatedPublishContextResolver resolver = new AuthenticatedPublishContextResolver(sessionService);
        KeyprovProtocolValidator validator = new KeyprovProtocolValidator(properties, guard);
        KeyprovInbox inbox = new KeyprovInbox(properties);
        KeyprovEnvelopeFactory envelopeFactory = new KeyprovEnvelopeFactory(properties);
        KeyprovRelayAppService relay = new KeyprovRelayAppService(properties, guard, validator, inbox,
                vmdGateway, envelopeFactory, downlinkPublisher, sessionService, metrics);
        KeyprovAdmissionBulkhead bulkhead = new KeyprovAdmissionBulkhead(properties, metrics);
        executor = new KeyprovExecutor(properties, metrics);
        adapter = new KeyprovMqttIngressAdapter(properties, validator, resolver, bulkhead, executor, relay,
                new VagwMqttSubscriptionMetrics());
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    /** 等待专属执行器完成已提交任务（ingress 已改为异步提交，MQTT 回调不等待 VMD）。 */
    private void awaitExecutorIdle(long baselineCompleted) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            if (executor.activeCount() == 0 && executor.completedTaskCount() >= baselineCompleted + 1) {
                return;
            }
            Thread.sleep(10L);
        }
        fail("keyprov executor did not complete the submitted task in time");
    }

    @Test
    @DisplayName("线程隔离：VMD 调用运行在 keyprov-vmd-* 专属线程，不在 MQTT/调用方线程")
    void vmdCall_shouldRunOnDedicatedKeyprovThread() throws Exception {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.of(onlineSession()));
        AtomicReference<String> callThread = new AtomicReference<>();
        when(vmdGateway.relay(any())).thenAnswer(invocation -> {
            callThread.set(Thread.currentThread().getName());
            return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
        });

        long baseline = executor.completedTaskCount();
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", uplinkEnvelope());
        awaitExecutorIdle(baseline);

        assertNotNull(callThread.get());
        assertTrue(callThread.get().startsWith("keyprov-vmd-"),
                "VMD 调用必须在专属线程，实际: " + callThread.get());
        assertNotEquals(Thread.currentThread().getName(), callThread.get());
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
                .setPayload(validRequest().toByteString())
                .build().toByteArray();
    }

    private static KeyProvisioningRequest validRequest() {
        return KeyProvisioningRequest.newBuilder()
                .setRequestId("req-001")
                .setIdempotencyKey("idem-001")
                .setBusinessDomain("V2C_COMM")
                .addPurposes(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD)
                .addPurposes(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD)
                .addSupportedWrappingSuites(
                        WrappingSuite.WRAPPING_SUITE_ECDH_ES_P256_HKDF_SHA256_AES256GCM)
                .addSupportedEnvelopeFormats(KeyEnvelopeFormat.KEY_ENVELOPE_FORMAT_ECDH_ES_P256_V1)
                .build();
    }

    private static KeyImportItemResult ackItem(KeyPurpose purpose, byte proofByte) {
        return KeyImportItemResult.newBuilder()
                .setPurpose(purpose)
                .setKeyId("key-" + purpose.getNumber())
                .setBusinessKeyVersion(1L)
                .setResult(KeyImportResult.KEY_IMPORT_RESULT_IMPORTED)
                .setImportProofAlgorithmValue(1)
                .setImportProof(com.google.protobuf.ByteString.copyFrom(repeat(proofByte, 64)))
                .setRecipientCertificateDigest(com.google.protobuf.ByteString.copyFrom(repeat((byte) 0x22, 32)))
                .setEnvelopeDigest(com.google.protobuf.ByteString.copyFrom(repeat((byte) 0x33, 32)))
                .setKeyCheckValue(com.google.protobuf.ByteString.copyFrom(new byte[]{0x0A, 0x0B, 0x0C}))
                .setEnvelopeFormat(KeyEnvelopeFormat.KEY_ENVELOPE_FORMAT_ECDH_ES_P256_V1)
                .build();
    }

    private static byte[] repeat(byte value, int length) {
        byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, value);
        return bytes;
    }

    private static byte[] ackEnvelope() {
        KeyProvisioningAck ack = KeyProvisioningAck.newBuilder()
                .setRequestId("req-001")
                .addItems(ackItem(KeyPurpose.KEY_PURPOSE_UPLINK_AEAD, (byte) 0x41))
                .addItems(ackItem(KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD, (byte) 0x42))
                .build();
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov")
                .setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001")
                .setMessageId("up-ack-001")
                .setRequestId("req-001")
                .setPayloadType("security.keyprov.v1.KeyProvisioningAck")
                .setPayload(ack.toByteString())
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
        byte[] expectedResponse = canonicalResponse.clone();
        when(vmdGateway.relay(any())).thenReturn(new VmdRelayResult("req-001", RESP_TYPE,
                canonicalResponse, 1900000000000L, KeyprovOutcome.SUCCESS, null, false));

        ArgumentCaptor<byte[]> published = ArgumentCaptor.forClass(byte[].class);
        when(downlinkPublisher.publish(eq("DEVICE001"), published.capture()))
                .thenReturn("vehicle/DEVICE001/down/keyprov");

        long baseline = executor.completedTaskCount();
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", uplinkEnvelope());
        awaitExecutorIdle(baseline);

        verify(vmdGateway).relay(any());
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
        assertArrayEquals(expectedResponse, down.getPayload().toByteArray(),
                "VAGW 必须原样透传 VMD canonical payload");
    }

    @Test
    @DisplayName("能力关闭：keyprov 上行不调用 VMD（与 FOTA/远控隔离）")
    void disabled_isolation() throws Exception {
        properties.setEnabled(false);

        long baseline = executor.completedTaskCount();
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", uplinkEnvelope());
        Thread.sleep(100L);

        verifyNoInteractions(vmdGateway);
        verifyNoInteractions(downlinkPublisher);
        assertEquals(baseline, executor.completedTaskCount(), "disabled 时不应提交执行任务");
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
        Thread.sleep(100L);

        verifyNoInteractions(vmdGateway);
        verifyNoInteractions(downlinkPublisher);
    }

    @Test
    @DisplayName("ACK→AckResult：CR-008 proof bytes 经 MQTT→VAGW→VMD 逐字节一致")
    void ackImportAttestation_endToEnd_shouldRelayCanonicalBytesVerbatim() throws Exception {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.of(onlineSession()));
        when(sessionService.isGenerationCurrent("DEVICE001", 2L)).thenReturn(true);

        byte[] canonicalAckResult = net.hwyz.iov.security.keyprov.v1.KeyProvisioningAckResult.newBuilder()
                .setRequestId("req-001")
                .setResult(KeyProvisioningResult.KEY_PROVISIONING_RESULT_SUCCEEDED)
                .setOccurredAtMs(1900000000000L)
                .build().toByteArray();
        byte[] expectedAckResult = canonicalAckResult.clone();
        java.util.concurrent.atomic.AtomicReference<byte[]> forwardedPayload =
                new java.util.concurrent.atomic.AtomicReference<>();
        when(vmdGateway.relay(any())).thenAnswer(invocation -> {
            VmdKeyProvisioningCommand command = invocation.getArgument(0);
            // 敏感 payload 在 relay finally 会被清零；在调用点复制以验证逐字节一致
            forwardedPayload.set(command.canonicalPayload().clone());
            return new VmdRelayResult("req-001",
                    "security.keyprov.v1.KeyProvisioningAckResult", canonicalAckResult, null,
                    KeyprovOutcome.SUCCESS, null, false);
        });
        ArgumentCaptor<byte[]> published = ArgumentCaptor.forClass(byte[].class);
        when(downlinkPublisher.publish(eq("DEVICE001"), published.capture()))
                .thenReturn("vehicle/DEVICE001/down/keyprov");

        byte[] uplink = ackEnvelope();
        byte[] originalPayload = Envelope.VehicleMessageEnvelope.parseFrom(uplink)
                .getPayload().toByteArray();
        long baseline = executor.completedTaskCount();
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", uplink);
        awaitExecutorIdle(baseline);

        assertArrayEquals(originalPayload, forwardedPayload.get(),
                "VAGW 不得重序列化/改写 ACK proof 字段，必须逐字节提交 VMD");
        assertArrayEquals(originalPayload, Envelope.VehicleMessageEnvelope.parseFrom(uplink)
                        .getPayload().toByteArray(),
                "VAGW 校验后不得修改原 MQTT payload");

        Envelope.VehicleMessageEnvelope down = Envelope.VehicleMessageEnvelope.parseFrom(
                published.getValue());
        assertArrayEquals(expectedAckResult, down.getPayload().toByteArray(),
                "VMD AckResult canonical bytes 必须原样下发");
        assertEquals("security.keyprov.v1.KeyProvisioningAckResult", down.getPayloadType());
    }
}
