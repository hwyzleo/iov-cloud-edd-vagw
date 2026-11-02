package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.FeignException;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.ViamKeyProvisioningCommand;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.ViamRelayResult;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovCircuitBreaker;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovProtocolLock;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.sec.viam.api.service.ViamKeyProvisioningService;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningAckResult;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningError;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningOutcome;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningRequest;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningResponse;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningStatusResponse;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * VIAM Gateway Adapter 测试（EDD-VAGW-DSN-CR-016 §2.4/§5/§7-5/§7-6，owner 由
 * EDD-VAGW-DSN-CR-019 切换：VMD → VIAM）。
 * <p>
 * 验证 VAGW 只委托官方 sec-viam-api {@link ViamKeyProvisioningService}，按 canonical 上行类型分派
 * {@code /request}、{@code /ack}、{@code /status}，wire 携带可信上下文 + canonical bytes + digest，
 * 并完成 702xxx 稳定 reason 映射、Request 成功路径从 canonical field 4 提取 expire_at_ms（缺失/非法
 * fail-closed）、统一 deadline 有限重试、结果未知不重放与专属熔断。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class ViamKeyProvisioningGatewayAdapterTest {

    private static final String REQ_TYPE = "security.keyprov.v1.KeyProvisioningRequest";
    private static final String ACK_TYPE = "security.keyprov.v1.KeyProvisioningAck";
    private static final String STATUS_REQ_TYPE = "security.keyprov.v1.KeyProvisioningStatusRequest";
    private static final String RESP_TYPE = "security.keyprov.v1.KeyProvisioningResponse";
    private static final String ACK_RESULT_TYPE = "security.keyprov.v1.KeyProvisioningAckResult";
    private static final String STATUS_RESP_TYPE = "security.keyprov.v1.KeyProvisioningStatusResponse";
    private static final byte[] PAYLOAD = new byte[]{9, 8, 7};
    private static final long FUTURE_EXPIRE = System.currentTimeMillis() + 60_000L;

    @Mock
    private ViamKeyProvisioningService viamService;

    private KeyprovProperties properties;
    private KeyprovCircuitBreaker circuitBreaker;
    private KeyprovMetrics metrics;
    private KeyprovContractGuard guard;
    private ViamKeyProvisioningGatewayAdapter adapter;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.getViam().setDeadline(Duration.ofSeconds(2));
        properties.getCircuitBreaker().setEnabled(false);
        properties.getCircuitBreaker().setFailureWindow(Duration.ofSeconds(30));
        properties.getCircuitBreaker().setOpenDuration(Duration.ofSeconds(5));
        properties.getCircuitBreaker().setHalfOpenProbes(1);
        metrics = new KeyprovMetrics();
        circuitBreaker = new KeyprovCircuitBreaker(properties, metrics);
        guard = new KeyprovContractGuard();
        guard.verify();
        assertTrue(guard.compatible(), () -> "keyprov lock must be READY in test classpath: "
                + guard.incompatibilityReason());
        adapter = new ViamKeyProvisioningGatewayAdapter(viamService, properties, circuitBreaker, metrics, guard);
    }

    private ViamKeyProvisioningCommand command(String payloadType) {
        Envelope.VehicleMessageEnvelope envelope = Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov").setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001").setMessageId("m1").setRequestId("r1")
                .setPayloadType(payloadType)
                .setPayload(com.google.protobuf.ByteString.copyFrom(PAYLOAD))
                .build();
        BrokerAuthenticatedPublishContext context = new BrokerAuthenticatedPublishContext(
                KeyprovContextSource.ACL_SESSION_CORRELATED, "DEVICE001", "client001",
                "cert-1", "fp-1", 3L, "client001", Instant.parse("2026-10-16T00:00:00Z"));
        return new ViamKeyProvisioningCommand(context, envelope, PAYLOAD);
    }

    /** canonical KeyProvisioningResponse bytes（带有效 expire_at_ms=field 4）。 */
    private static byte[] canonicalResponseBytes(long expireAtMs) {
        return KeyProvisioningResponse.newBuilder()
                .setRequestId("r1")
                .setExpireAtMs(expireAtMs)
                .build().toByteArray();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    @Test
    void request_success_shouldDispatchRequestAndCarryCanonicalBytes() throws Exception {
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE).schemaVersion("keyprov-v1")
                .payloadBytes(canonicalResponseBytes(FUTURE_EXPIRE)).payloadDigest("d")
                .outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        ArgumentCaptor<ViamKeyProvisioningRequest> captor = ArgumentCaptor.forClass(ViamKeyProvisioningRequest.class);
        verify(viamService).request(captor.capture());
        ViamKeyProvisioningRequest request = captor.getValue();
        assertEquals("DEVICE001", request.getDeviceContext().getHsmUid());
        assertEquals("client001", request.getDeviceContext().getBrokerSessionId());
        assertEquals(3L, request.getDeviceContext().getConnectionGeneration());
        assertEquals("cert-1", request.getDeviceContext().getCertificateSerial());
        // 官方 ViamAuthenticatedDeviceContext 契约不含 fingerprint/mqttReceivedAt（CR-019 §18）
        assertEquals(REQ_TYPE, request.getPayloadType());
        assertEquals("keyprov-v1", request.getSchemaVersion());
        assertArrayEquals(PAYLOAD, request.getPayloadBytes());
        assertEquals(sha256(PAYLOAD), request.getPayloadDigest());
        KeyprovProtocolLock lock = guard.lock();
        assertEquals(lock.parProtoRelease(), request.getProtocolRelease(),
                "request 必须携带 VAGW 实际锁定的 PAR-PROTO release");
        assertEquals(lock.descriptorSetSha256(), request.getDescriptorSetHash(),
                "request 必须携带实际 DescriptorSet hash");

        assertEquals(KeyprovOutcome.SUCCESS, result.outcome());
        assertTrue(result.hasPayload());
        assertArrayEquals(canonicalResponseBytes(FUTURE_EXPIRE), result.downlinkPayload());
        assertEquals(RESP_TYPE, result.downlinkPayloadType());
        // expireAtMs 从 canonical KeyProvisioningResponse field 4 提取（与 VMD 语义一致）
        assertEquals(FUTURE_EXPIRE, result.expireAtMs());
        assertNull(result.reason());
        assertEquals(1L, metrics.get("keyprov_viam_calls_total{result=success}"));
    }

    @Test
    void request_success_missingExpireAtMs_shouldFailClosedReject() {
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE).schemaVersion("keyprov-v1")
                .payloadBytes(KeyProvisioningResponse.newBuilder()
                        .setRequestId("r1").build().toByteArray())
                .outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        // CR-019 §18：canonical payload 缺 expire_at_ms（=0 未设置）→ fail-closed，不回退默认 TTL
        assertEquals(KeyprovOutcome.UNAVAILABLE, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE, result.reason());
        assertFalse(result.hasPayload());
    }

    @Test
    void request_success_expiredExpireAtMs_shouldFailClosedReject() {
        long past = System.currentTimeMillis() - 1_000L;
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE)
                .payloadBytes(canonicalResponseBytes(past))
                .outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        // 已过期 bundle 不得下发（PAR-PROTO：expire_at_ms 早于当前可信时间时不得导入）
        assertEquals(KeyprovOutcome.REJECTED, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID, result.reason());
        assertFalse(result.hasPayload());
    }

    @Test
    void request_success_invalidCanonicalResponse_shouldFailClosedReject() {
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE)
                .payloadBytes(new byte[]{1, 2, 3}) // 非法 canonical bytes
                .outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.UNAVAILABLE, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE, result.reason());
        assertFalse(result.hasPayload());
    }

    @Test
    void ack_success_shouldDispatchAckEndpointWithoutExpiryRequirement() {
        when(viamService.ack(any())).thenReturn(ViamKeyProvisioningAckResult.builder()
                .requestId("r1").payloadType(ACK_RESULT_TYPE)
                .payloadBytes(new byte[]{6}).outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(ACK_TYPE));

        verify(viamService).ack(any());
        verify(viamService, never()).request(any());
        verify(viamService, never()).status(any());
        assertEquals(KeyprovOutcome.SUCCESS, result.outcome());
        assertEquals(ACK_RESULT_TYPE, result.downlinkPayloadType());
        // AckResult 无 expire_at_ms（proto field 4=error），不判非法（CR-019 §18）
        assertNull(result.expireAtMs());
    }

    @Test
    void statusRequest_success_shouldDispatchStatusEndpointWithoutExpiryRequirement() {
        when(viamService.status(any())).thenReturn(ViamKeyProvisioningStatusResponse.builder()
                .requestId("r1").payloadType(STATUS_RESP_TYPE)
                .payloadBytes(new byte[]{7}).outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(STATUS_REQ_TYPE));

        verify(viamService).status(any());
        verify(viamService, never()).request(any());
        verify(viamService, never()).ack(any());
        assertEquals(KeyprovOutcome.SUCCESS, result.outcome());
        assertEquals(STATUS_RESP_TYPE, result.downlinkPayloadType());
        // StatusResponse 无 expire_at_ms（proto field 4=occurred_at_ms），不判非法
        assertNull(result.expireAtMs());
    }

    @ParameterizedTest(name = "VIAM 702xxx {0} -> {1}")
    @CsvSource({
            "702101, KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH, REJECTED",
            "702102, KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH, REJECTED",
            "702103, KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH, REJECTED",
            "702104, KEYPROV_VIAM_BINDING_INVALID, REJECTED",
            "702105, KEYPROV_VIAM_CERTIFICATE_INVALID, REJECTED",
            "702106, KEYPROV_VIAM_CERTIFICATE_INVALID, REJECTED",
            "702108, KEYPROV_VIAM_BINDING_INVALID, REJECTED",
            "702109, KEYPROV_VIAM_DIRECTORY_UNAVAILABLE, UNAVAILABLE",
            "702110, KEYPROV_VIAM_CERTIFICATE_INVALID, REJECTED",
            "702301, KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH, REJECTED",
            "702302, KEYPROV_VIAM_REQUEST_INVALID, REJECTED",
            "702303, KEYPROV_VIAM_BINDING_INVALID, REJECTED",
            "702306, KEYPROV_VIAM_IDEMPOTENCY_CONFLICT, REJECTED",
            "702308, KEYPROV_VIAM_CERTIFICATE_INVALID, REJECTED",
            "702309, KEYPROV_VIAM_DIRECTORY_UNAVAILABLE, UNAVAILABLE",
            "702310, KEYPROV_VIAM_WRAP_FAILED, REJECTED",
            "702312, KEYPROV_VIAM_OUTCOME_UNKNOWN, OUTCOME_UNKNOWN",
            "702401, KEYPROV_VIAM_REQUEST_INVALID, REJECTED",
            "702402, KEYPROV_VIAM_REQUEST_INVALID, REJECTED",
            "702403, KEYPROV_VIAM_BATCH_NOT_FOUND, REJECTED",
            "702405, KEYPROV_VIAM_IDEMPOTENCY_CONFLICT, REJECTED",
            "702408, KEYPROV_VIAM_BINDING_INVALID, REJECTED",
            "702414, KEYPROV_VIAM_OUTCOME_UNKNOWN, OUTCOME_UNKNOWN",
            "702415, KEYPROV_VIAM_IDEMPOTENCY_CONFLICT, REJECTED"
    })
    void viamErrorCode_shouldMapToStableReason(String code, KeyprovReason expectedReason,
                                               KeyprovOutcome expectedOutcome) {
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1")
                .outcome(ViamKeyProvisioningOutcome.valueOf(expectedOutcome.name()))
                .error(ViamKeyProvisioningError.builder().code(code).retryable(false).build())
                .build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(expectedOutcome, result.outcome());
        assertEquals(expectedReason, result.reason());
        assertFalse(result.hasPayload());
    }

    @Test
    void unknownViamErrorCode_shouldFailClosedReject() {
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1")
                .outcome(ViamKeyProvisioningOutcome.REJECTED)
                .error(ViamKeyProvisioningError.builder().code("702999").retryable(false).build())
                .build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        // CR-019 §6：未知 VIAM 错误默认拒绝（804010 + 稳定 reason），不复制 legacy 映射表
        assertEquals(KeyprovOutcome.REJECTED, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_REJECTED, result.reason());
    }

    @ParameterizedTest(name = "Feign HTTP {0} -> {1}")
    @CsvSource({
            // 端点/方法未注册属于依赖能力不可用，不是设备业务拒绝
            "404, KEYPROV_VIAM_UNAVAILABLE, UNAVAILABLE",
            "405, KEYPROV_VIAM_UNAVAILABLE, UNAVAILABLE",
            "503, KEYPROV_VIAM_UNAVAILABLE, UNAVAILABLE",
            // 其余 4xx 仍为业务拒绝
            "400, KEYPROV_VIAM_REJECTED, REJECTED"
    })
    void feignHttpStatus_shouldDistinguishCapabilityUnavailableFromBusinessRejection(
            int status, KeyprovReason expectedReason, KeyprovOutcome expectedOutcome) {
        FeignException feignException = mock(FeignException.class);
        when(feignException.status()).thenReturn(status);
        when(viamService.request(any())).thenThrow(feignException);

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(expectedOutcome, result.outcome());
        assertEquals(expectedReason, result.reason());
        assertFalse(result.hasPayload());
    }

    @Test
    void dependencyUnavailable_shouldOpenCircuit_butBusinessRejectionShouldNot() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker breaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter guarded =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, breaker, metrics, guard);

        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .outcome(ViamKeyProvisioningOutcome.UNAVAILABLE)
                .error(ViamKeyProvisioningError.builder().code("702109").build()).build());
        guarded.relay(command(REQ_TYPE));
        assertEquals(KeyprovCircuitBreaker.State.OPEN, breaker.state(),
                "依赖不可用必须计入熔断");
    }

    @Test
    void businessRejection_shouldNotCountAsDependencyFailure() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker breaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter guarded =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, breaker, metrics, guard);

        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .outcome(ViamKeyProvisioningOutcome.REJECTED)
                .error(ViamKeyProvisioningError.builder().code("702401").build()).build());
        ViamRelayResult result = guarded.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.REJECTED, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID, result.reason());
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, breaker.state(),
                "业务拒绝不得计入依赖故障率");
    }

    @Test
    void deterministicImportAttestationRejection_shouldNotCountAsDependencyFailure() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker breaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter guarded =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, breaker, metrics, guard);

        when(viamService.ack(any())).thenReturn(ViamKeyProvisioningAckResult.builder()
                .outcome(ViamKeyProvisioningOutcome.REJECTED)
                .error(ViamKeyProvisioningError.builder().code("702405").retryable(false).build())
                .build());
        ViamRelayResult result = guarded.relay(command(ACK_TYPE));

        assertEquals(KeyprovReason.KEYPROV_VIAM_IDEMPOTENCY_CONFLICT, result.reason());
        assertFalse(result.dependencyFailure(), "确定性 proof 拒绝不得标记为依赖故障");
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, breaker.state(),
                "VIAM/FW 确定性 Import Attestation 拒绝不得计入熔断");
    }

    @Test
    void viamOutcomeUnknownBusinessResult_shouldNotCountAsDependencyFailure() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker breaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter guarded =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, breaker, metrics, guard);

        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1")
                .outcome(ViamKeyProvisioningOutcome.OUTCOME_UNKNOWN)
                .error(ViamKeyProvisioningError.builder().code("702414").retryable(true).build())
                .build());
        ViamRelayResult result = guarded.relay(command(REQ_TYPE));

        assertEquals(KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN, result.reason());
        assertFalse(result.dependencyFailure(),
                "VIAM 业务结果未知不是依赖故障，不得计入熔断");
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, breaker.state());
    }

    @Test
    void directoryUnavailable_shouldCountAsDependencyFailure() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker breaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter guarded =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, breaker, metrics, guard);

        when(viamService.ack(any())).thenReturn(ViamKeyProvisioningAckResult.builder()
                .outcome(ViamKeyProvisioningOutcome.UNAVAILABLE)
                .error(ViamKeyProvisioningError.builder().code("702309").retryable(true).build())
                .build());
        ViamRelayResult result = guarded.relay(command(ACK_TYPE));

        assertEquals(KeyprovReason.KEYPROV_VIAM_DIRECTORY_UNAVAILABLE, result.reason());
        assertTrue(result.dependencyFailure());
        assertEquals(KeyprovCircuitBreaker.State.OPEN, breaker.state(),
                "目录/KMS 暂不可用属于依赖故障，应计入熔断");
    }

    @ParameterizedTest(name = "VIAM 702xxx {0} + canonical payload -> 原样转发")
    @CsvSource({
            "702401, KEYPROV_VIAM_REQUEST_INVALID, REJECTED",
            "702109, KEYPROV_VIAM_DIRECTORY_UNAVAILABLE, UNAVAILABLE",
            "702414, KEYPROV_VIAM_OUTCOME_UNKNOWN, OUTCOME_UNKNOWN"
    })
    void nonSuccessWithCanonicalPayload_shouldRelayDownlinkInsteadOfDropping(
            String code, KeyprovReason expectedReason, KeyprovOutcome expectedOutcome) {
        byte[] downlink = new byte[]{1, 2, 3, 4};
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1")
                .payloadType(RESP_TYPE)
                .payloadBytes(downlink)
                .payloadDigest("d")
                .outcome(ViamKeyProvisioningOutcome.valueOf(expectedOutcome.name()))
                .error(ViamKeyProvisioningError.builder().code(code).retryable(false).build())
                .build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        // 非成功但带 canonical payload：VAGW 必须原样转发，否则车端收不到拒绝、只能盲重投
        assertTrue(result.hasPayload());
        assertEquals(expectedOutcome, result.outcome());
        assertEquals(expectedReason, result.reason());
        assertEquals(RESP_TYPE, result.downlinkPayloadType());
        assertArrayEquals(downlink, result.downlinkPayload());
    }

    @Test
    void businessRejectionWithPayload_shouldStillNotCountAsDependencyFailure() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker breaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter guarded =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, breaker, metrics, guard);

        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE).payloadBytes(new byte[]{1})
                .outcome(ViamKeyProvisioningOutcome.REJECTED)
                .error(ViamKeyProvisioningError.builder().code("702401").build()).build());
        ViamRelayResult result = guarded.relay(command(REQ_TYPE));

        assertTrue(result.hasPayload());
        assertEquals(KeyprovOutcome.REJECTED, result.outcome());
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, breaker.state(),
                "带 payload 的业务拒绝同样不得计入依赖故障率");
    }

    @Test
    void connectFailureBeforeSend_shouldRetryOnceWithinDeadline() {
        when(viamService.request(any()))
                .thenThrow(new RuntimeException(new ConnectException("connection refused")))
                .thenReturn(ViamKeyProvisioningResponse.builder()
                        .outcome(ViamKeyProvisioningOutcome.UNAVAILABLE).build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.UNAVAILABLE, result.outcome());
        verify(viamService, times(2)).request(any());
        assertEquals(1L, metrics.get("keyprov_viam_calls_total{result=retry_not_sent}"));
    }

    @Test
    void readTimeoutAfterSend_shouldNotReplay_andReportOutcomeUnknown() {
        when(viamService.request(any()))
                .thenThrow(new RuntimeException(new SocketTimeoutException("Read timed out")));

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.OUTCOME_UNKNOWN, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN, result.reason());
        assertEquals("r1", result.requestId(), "read timeout 必须保留原 requestId 供 /status 对账");
        assertTrue(result.dependencyFailure(), "传输类 timeout 应计入依赖故障");
        verify(viamService, times(1)).request(any());
    }

    @Test
    void connectTimeout_shouldBeRetriedWithinDeadline() {
        when(viamService.request(any()))
                .thenThrow(new RuntimeException(new SocketTimeoutException("connect timed out")))
                .thenThrow(new RuntimeException(new SocketTimeoutException("connect timed out")));

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.TIMEOUT, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_TIMEOUT, result.reason());
        verify(viamService, times(2)).request(any());
    }

    @Test
    void circuitOpen_shouldFailFastWithoutFeignCall() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker openBreaker = new KeyprovCircuitBreaker(properties, metrics);
        ViamKeyProvisioningGatewayAdapter openAdapter =
                new ViamKeyProvisioningGatewayAdapter(viamService, properties, openBreaker, metrics, guard);

        assertTrue(openBreaker.allowRequest());
        openBreaker.recordDependencyFailure();
        assertEquals(KeyprovCircuitBreaker.State.OPEN, openBreaker.state());

        ViamRelayResult result = openAdapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.CIRCUIT_OPEN, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_CIRCUIT_OPEN, result.reason());
        verifyNoInteractions(viamService);
    }

    @Test
    void successWithoutPayload_shouldMapUnavailable() {
        when(viamService.request(any())).thenReturn(ViamKeyProvisioningResponse.builder()
                .outcome(ViamKeyProvisioningOutcome.SUCCESS).build());

        ViamRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.UNAVAILABLE, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE, result.reason());
    }
}
