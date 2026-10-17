package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.FeignException;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.VmdKeyProvisioningCommand;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.VmdRelayResult;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovCircuitBreaker;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningAckResult;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningError;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningOutcome;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningRequest;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningResponse;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningStatusResponse;
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
 * VMD Gateway Adapter 测试（EDD-VAGW-DSN-CR-016 §2.4/§5/§7-5/§7-6，VMD-DSN-CR-059 对齐）。
 * <p>
 * 验证 VAGW 只委托官方 edd-vmd-api {@link VmdKeyProvisioningService}，按 canonical 上行类型分派
 * {@code /request}、{@code /ack}、{@code /status}，wire 携带可信上下文 + canonical bytes + digest，
 * 并完成 806090～806100 映射、统一 deadline 有限重试、结果未知不重放与专属熔断。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class VmdKeyProvisioningGatewayAdapterTest {

    private static final String REQ_TYPE = "security.keyprov.v1.KeyProvisioningRequest";
    private static final String ACK_TYPE = "security.keyprov.v1.KeyProvisioningAck";
    private static final String STATUS_REQ_TYPE = "security.keyprov.v1.KeyProvisioningStatusRequest";
    private static final String RESP_TYPE = "security.keyprov.v1.KeyProvisioningResponse";
    private static final String ACK_RESULT_TYPE = "security.keyprov.v1.KeyProvisioningAckResult";
    private static final String STATUS_RESP_TYPE = "security.keyprov.v1.KeyProvisioningStatusResponse";
    private static final byte[] PAYLOAD = new byte[]{9, 8, 7};

    @Mock
    private VmdKeyProvisioningService vmdService;

    private KeyprovProperties properties;
    private KeyprovCircuitBreaker circuitBreaker;
    private KeyprovMetrics metrics;
    private VmdKeyProvisioningGatewayAdapter adapter;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.getVmd().setDeadline(Duration.ofSeconds(2));
        properties.getCircuitBreaker().setEnabled(false);
        properties.getCircuitBreaker().setFailureWindow(Duration.ofSeconds(30));
        properties.getCircuitBreaker().setOpenDuration(Duration.ofSeconds(5));
        properties.getCircuitBreaker().setHalfOpenProbes(1);
        metrics = new KeyprovMetrics();
        circuitBreaker = new KeyprovCircuitBreaker(properties, metrics);
        adapter = new VmdKeyProvisioningGatewayAdapter(vmdService, properties, circuitBreaker, metrics);
    }

    private VmdKeyProvisioningCommand command(String payloadType) {
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
        return new VmdKeyProvisioningCommand(context, envelope, PAYLOAD);
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    @Test
    void request_success_shouldDispatchRequestAndCarryCanonicalBytes() throws Exception {
        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE).schemaVersion("keyprov-v1")
                .payloadBytes(new byte[]{4, 5}).payloadDigest("d").expireAtMs(99L)
                .outcome(VmdKeyProvisioningOutcome.SUCCESS).build());

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

        ArgumentCaptor<VmdKeyProvisioningRequest> captor = ArgumentCaptor.forClass(VmdKeyProvisioningRequest.class);
        verify(vmdService).request(captor.capture());
        VmdKeyProvisioningRequest request = captor.getValue();
        assertEquals("DEVICE001", request.getDeviceContext().getHsmUid());
        assertEquals("client001", request.getDeviceContext().getBrokerSessionId());
        assertEquals(3L, request.getDeviceContext().getConnectionGeneration());
        assertEquals("cert-1", request.getDeviceContext().getCertificateSerial());
        assertEquals("fp-1", request.getDeviceContext().getCertificateFingerprint());
        assertEquals(Instant.parse("2026-10-16T00:00:00Z").toEpochMilli(),
                request.getDeviceContext().getMqttReceivedAt());
        assertEquals(REQ_TYPE, request.getPayloadType());
        assertEquals("keyprov-v1", request.getSchemaVersion());
        assertArrayEquals(PAYLOAD, request.getPayloadBytes());
        assertEquals(sha256(PAYLOAD), request.getPayloadDigest());

        assertEquals(KeyprovOutcome.SUCCESS, result.outcome());
        assertTrue(result.hasPayload());
        assertArrayEquals(new byte[]{4, 5}, result.downlinkPayload());
        assertEquals(RESP_TYPE, result.downlinkPayloadType());
        assertEquals(99L, result.expireAtMs());
        assertNull(result.reason());
        assertEquals(1L, metrics.get("keyprov_vmd_calls_total{result=success}"));
    }

    @Test
    void ack_success_shouldDispatchAckEndpoint() {
        when(vmdService.ack(any())).thenReturn(VmdKeyProvisioningAckResult.builder()
                .requestId("r1").payloadType(ACK_RESULT_TYPE)
                .payloadBytes(new byte[]{6}).outcome(VmdKeyProvisioningOutcome.SUCCESS).build());

        VmdRelayResult result = adapter.relay(command(ACK_TYPE));

        verify(vmdService).ack(any());
        verify(vmdService, never()).request(any());
        verify(vmdService, never()).status(any());
        assertEquals(KeyprovOutcome.SUCCESS, result.outcome());
        assertEquals(ACK_RESULT_TYPE, result.downlinkPayloadType());
    }

    @Test
    void statusRequest_success_shouldDispatchStatusEndpoint() {
        when(vmdService.status(any())).thenReturn(VmdKeyProvisioningStatusResponse.builder()
                .requestId("r1").payloadType(STATUS_RESP_TYPE)
                .payloadBytes(new byte[]{7}).outcome(VmdKeyProvisioningOutcome.SUCCESS).build());

        VmdRelayResult result = adapter.relay(command(STATUS_REQ_TYPE));

        verify(vmdService).status(any());
        verify(vmdService, never()).request(any());
        verify(vmdService, never()).ack(any());
        assertEquals(KeyprovOutcome.SUCCESS, result.outcome());
        assertEquals(STATUS_RESP_TYPE, result.downlinkPayloadType());
    }

    @ParameterizedTest(name = "VMD {0} -> {1}")
    @CsvSource({
            "806090, KEYPROV_VMD_CALLER_UNAUTHORIZED, REJECTED",
            "806091, KEYPROV_IDENTITY_MISMATCH, REJECTED",
            "806092, KEYPROV_BINDING_INVALID, REJECTED",
            "806093, KEYPROV_CERTIFICATE_INVALID, REJECTED",
            "806094, KEYPROV_REQUEST_INVALID, REJECTED",
            "806095, KEYPROV_IDEMPOTENCY_CONFLICT, REJECTED",
            "806096, KEYPROV_VMD_UNAVAILABLE, UNAVAILABLE",
            "806097, KEYPROV_VMD_REJECTED, REJECTED",
            "806098, KEYPROV_OUTCOME_UNKNOWN, OUTCOME_UNKNOWN",
            "806099, KEYPROV_BATCH_NOT_FOUND, REJECTED",
            "806100, KEYPROV_VMD_UNAVAILABLE, UNAVAILABLE"
    })
    void vmdErrorCode_shouldMapToStableReason(String code, KeyprovReason expectedReason,
                                              KeyprovOutcome expectedOutcome) {
        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .requestId("r1")
                .outcome(VmdKeyProvisioningOutcome.valueOf(expectedOutcome.name()))
                .error(VmdKeyProvisioningError.builder().code(code).retryable(false).build())
                .build());

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(expectedOutcome, result.outcome());
        assertEquals(expectedReason, result.reason());
        assertFalse(result.hasPayload());
    }

    @ParameterizedTest(name = "Feign HTTP {0} -> {1}")
    @CsvSource({
            // 端点/方法未注册属于依赖能力不可用，不是设备业务拒绝
            "404, KEYPROV_VMD_UNAVAILABLE, UNAVAILABLE",
            "405, KEYPROV_VMD_UNAVAILABLE, UNAVAILABLE",
            "503, KEYPROV_VMD_UNAVAILABLE, UNAVAILABLE",
            // 其余 4xx 仍为业务拒绝
            "400, KEYPROV_VMD_REJECTED, REJECTED"
    })
    void feignHttpStatus_shouldDistinguishCapabilityUnavailableFromBusinessRejection(
            int status, KeyprovReason expectedReason, KeyprovOutcome expectedOutcome) {
        FeignException feignException = mock(FeignException.class);
        when(feignException.status()).thenReturn(status);
        when(vmdService.request(any())).thenThrow(feignException);

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

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
        VmdKeyProvisioningGatewayAdapter guarded =
                new VmdKeyProvisioningGatewayAdapter(vmdService, properties, breaker, metrics);

        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .outcome(VmdKeyProvisioningOutcome.UNAVAILABLE)
                .error(VmdKeyProvisioningError.builder().code("806096").build()).build());
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
        VmdKeyProvisioningGatewayAdapter guarded =
                new VmdKeyProvisioningGatewayAdapter(vmdService, properties, breaker, metrics);

        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .outcome(VmdKeyProvisioningOutcome.REJECTED)
                .error(VmdKeyProvisioningError.builder().code("806094").build()).build());
        VmdRelayResult result = guarded.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.REJECTED, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_REQUEST_INVALID, result.reason());
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, breaker.state(),
                "业务拒绝不得计入依赖故障率");
    }

    @ParameterizedTest(name = "VMD {0} + canonical payload -> 原样转发")
    @CsvSource({
            "806094, KEYPROV_REQUEST_INVALID, REJECTED",
            "806096, KEYPROV_VMD_UNAVAILABLE, UNAVAILABLE",
            "806098, KEYPROV_OUTCOME_UNKNOWN, OUTCOME_UNKNOWN"
    })
    void nonSuccessWithCanonicalPayload_shouldRelayDownlinkInsteadOfDropping(
            String code, KeyprovReason expectedReason, KeyprovOutcome expectedOutcome) {
        byte[] downlink = new byte[]{1, 2, 3, 4};
        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .requestId("r1")
                .payloadType(RESP_TYPE)
                .payloadBytes(downlink)
                .payloadDigest("d")
                .outcome(VmdKeyProvisioningOutcome.valueOf(expectedOutcome.name()))
                .error(VmdKeyProvisioningError.builder().code(code).retryable(false).build())
                .build());

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

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
        VmdKeyProvisioningGatewayAdapter guarded =
                new VmdKeyProvisioningGatewayAdapter(vmdService, properties, breaker, metrics);

        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .requestId("r1").payloadType(RESP_TYPE).payloadBytes(new byte[]{1})
                .outcome(VmdKeyProvisioningOutcome.REJECTED)
                .error(VmdKeyProvisioningError.builder().code("806094").build()).build());
        VmdRelayResult result = guarded.relay(command(REQ_TYPE));

        assertTrue(result.hasPayload());
        assertEquals(KeyprovOutcome.REJECTED, result.outcome());
        assertEquals(KeyprovCircuitBreaker.State.CLOSED, breaker.state(),
                "带 payload 的业务拒绝同样不得计入依赖故障率");
    }

    @Test
    void connectFailureBeforeSend_shouldRetryOnceWithinDeadline() {
        when(vmdService.request(any()))
                .thenThrow(new RuntimeException(new ConnectException("connection refused")))
                .thenReturn(VmdKeyProvisioningResponse.builder()
                        .outcome(VmdKeyProvisioningOutcome.UNAVAILABLE).build());

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.UNAVAILABLE, result.outcome());
        verify(vmdService, times(2)).request(any());
        assertEquals(1L, metrics.get("keyprov_vmd_calls_total{result=retry_not_sent}"));
    }

    @Test
    void readTimeoutAfterSend_shouldNotReplay_andReportOutcomeUnknown() {
        when(vmdService.request(any()))
                .thenThrow(new RuntimeException(new SocketTimeoutException("Read timed out")));

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.OUTCOME_UNKNOWN, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_OUTCOME_UNKNOWN, result.reason());
        verify(vmdService, times(1)).request(any());
    }

    @Test
    void connectTimeout_shouldBeRetriedWithinDeadline() {
        when(vmdService.request(any()))
                .thenThrow(new RuntimeException(new SocketTimeoutException("connect timed out")))
                .thenThrow(new RuntimeException(new SocketTimeoutException("connect timed out")));

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.TIMEOUT, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VMD_TIMEOUT, result.reason());
        verify(vmdService, times(2)).request(any());
    }

    @Test
    void circuitOpen_shouldFailFastWithoutFeignCall() {
        properties.getCircuitBreaker().setEnabled(true);
        properties.getCircuitBreaker().setMinimumCalls(1);
        properties.getCircuitBreaker().setFailureRateThreshold(1.0d);
        KeyprovCircuitBreaker openBreaker = new KeyprovCircuitBreaker(properties, metrics);
        VmdKeyProvisioningGatewayAdapter openAdapter =
                new VmdKeyProvisioningGatewayAdapter(vmdService, properties, openBreaker, metrics);

        assertTrue(openBreaker.allowRequest());
        openBreaker.recordDependencyFailure();
        assertEquals(KeyprovCircuitBreaker.State.OPEN, openBreaker.state());

        VmdRelayResult result = openAdapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.CIRCUIT_OPEN, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_CIRCUIT_OPEN, result.reason());
        verifyNoInteractions(vmdService);
    }

    @Test
    void successWithoutPayload_shouldMapUnavailable() {
        when(vmdService.request(any())).thenReturn(VmdKeyProvisioningResponse.builder()
                .outcome(VmdKeyProvisioningOutcome.SUCCESS).build());

        VmdRelayResult result = adapter.relay(command(REQ_TYPE));

        assertEquals(KeyprovOutcome.UNAVAILABLE, result.outcome());
        assertEquals(KeyprovReason.KEYPROV_VMD_UNAVAILABLE, result.reason());
    }
}
