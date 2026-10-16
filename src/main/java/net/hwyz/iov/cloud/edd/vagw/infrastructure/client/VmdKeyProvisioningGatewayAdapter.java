package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.VmdKeyProvisioningCommand;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.VmdKeyProvisioningGateway;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.VmdRelayResult;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovCircuitBreaker;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VagwAuthenticatedDeviceContext;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningAckRequest;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningAckResult;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningError;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningOutcome;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningRequest;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningResponse;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningStatusRequest;
import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningStatusResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;

/**
 * VMD Key Provisioning Gateway Adapter（EDD-VAGW-DSN-CR-016 §2.4，VMD-DSN-CR-059 对齐）。
 * <p>
 * 实现 VAGW 应用层 {@link VmdKeyProvisioningGateway} Port，直接委托 edd-vmd-api 官方
 * {@link VmdKeyProvisioningService} Feign Bean，并完成：
 * <ul>
 *   <li>认证会话上下文 → {@link VagwAuthenticatedDeviceContext}；</li>
 *   <li>按 canonical 上行消息类型分派官方三端点：Request→/request、Ack→/ack、StatusRequest→/status；</li>
 *   <li>wire 携带 payloadType、schemaVersion、canonical payloadBytes 与 sha-256 payloadDigest，
 *       VAGW 不做 JSON↔proto 转换；</li>
 *   <li>官方响应 outcome／806xxx → VAGW 稳定 804010 reason 映射；</li>
 *   <li>统一 absolute deadline 下的有限重试（仅在明确证明请求未发出时、同一 requestId 内）；</li>
 *   <li>Keyprov 专属熔断治理（仅统计依赖类失败）。</li>
 * </ul>
 * 不定义第二套 {@code @FeignClient}、URL、服务名常量、wire DTO 或 fallback；不新增 per-client
 * 物理连接池；不向 VAGW 暴露 KMS 内部 keyRef，也不执行 VMD 业务校验或密钥编排。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class VmdKeyProvisioningGatewayAdapter implements VmdKeyProvisioningGateway {

    private final VmdKeyProvisioningService vmdService;
    private final KeyprovProperties properties;
    private final KeyprovCircuitBreaker circuitBreaker;
    private final KeyprovMetrics metrics;

    @Override
    public VmdRelayResult relay(VmdKeyProvisioningCommand command) {
        if (!circuitBreaker.allowRequest()) {
            metrics.vmdCall("circuit_open");
            return VmdRelayResult.circuitOpen(KeyprovReason.KEYPROV_CIRCUIT_OPEN);
        }
        long startedAt = System.nanoTime();
        try {
            VmdRelayResult result = dispatch(command);
            metrics.vmdLatency(elapsedMillis(startedAt));
            if (isDependencyFailure(result)) {
                circuitBreaker.recordDependencyFailure();
            } else {
                circuitBreaker.recordSuccess();
            }
            metrics.vmdCall(metricLabel(result));
            return result;
        } catch (RuntimeException e) {
            metrics.vmdLatency(elapsedMillis(startedAt));
            circuitBreaker.recordDependencyFailure();
            VmdRelayResult result = classify(e);
            metrics.vmdCall(metricLabel(result));
            // 只输出异常类型与稳定 reason，不输出请求/响应内容
            log.warn("Keyprov VMD call failed: outcome={}, reason={}, exception={}",
                    result.outcome(), result.reason(), e.getClass().getSimpleName());
            return result;
        }
    }

    /**
     * 按 canonical 上行消息类型分派官方 request/ack/status 端点。
     */
    private VmdRelayResult dispatch(VmdKeyProvisioningCommand command) {
        Optional<KeyprovMessageType> type = KeyprovMessageType.fromFullName(command.envelope().getPayloadType());
        if (type.isEmpty() || !type.get().isUpstream()) {
            return VmdRelayResult.rejected(command.envelope().getRequestId(),
                    KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED);
        }
        return invokeWithBoundedRetry(command, type.get());
    }

    /**
     * 统一 absolute deadline 下的有限重试：仅在明确证明请求未发出（连接失败/连接超时）且仍有
     * deadline 预算时，允许一次、同一 requestId 的重试；请求已发送后一律不重放。
     */
    private VmdRelayResult invokeWithBoundedRetry(VmdKeyProvisioningCommand command, KeyprovMessageType type) {
        long deadlineAt = System.currentTimeMillis() + deadlineMillis();
        try {
            return invoke(command, type);
        } catch (RuntimeException e) {
            if (isNotSent(e) && System.currentTimeMillis() < deadlineAt) {
                metrics.vmdCall("retry_not_sent");
                return invoke(command, type);
            }
            throw e;
        }
    }

    private VmdRelayResult invoke(VmdKeyProvisioningCommand command, KeyprovMessageType type) {
        VagwAuthenticatedDeviceContext context = toDeviceContext(command.context());
        String payloadType = command.envelope().getPayloadType();
        String schemaVersion = command.envelope().getProtocolVersion();
        byte[] payload = command.canonicalPayload();
        String digest = sha256(payload);
        return switch (type) {
            case KEY_PROVISIONING_REQUEST -> mapResponse(vmdService.request(VmdKeyProvisioningRequest.builder()
                    .deviceContext(context).payloadType(payloadType).schemaVersion(schemaVersion)
                    .payloadBytes(payload).payloadDigest(digest).build()));
            case KEY_PROVISIONING_ACK -> mapAck(vmdService.ack(VmdKeyProvisioningAckRequest.builder()
                    .deviceContext(context).payloadType(payloadType).schemaVersion(schemaVersion)
                    .payloadBytes(payload).payloadDigest(digest).build()));
            case KEY_PROVISIONING_STATUS_REQUEST -> mapStatus(vmdService.status(
                    VmdKeyProvisioningStatusRequest.builder()
                            .deviceContext(context).payloadType(payloadType).schemaVersion(schemaVersion)
                            .payloadBytes(payload).payloadDigest(digest).build()));
            default -> VmdRelayResult.rejected(command.envelope().getRequestId(),
                    KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED);
        };
    }

    private static VagwAuthenticatedDeviceContext toDeviceContext(BrokerAuthenticatedPublishContext context) {
        return VagwAuthenticatedDeviceContext.builder()
                .hsmUid(context.hsmUid())
                .brokerSessionId(context.brokerSessionId())
                .connectionGeneration(context.connectionGeneration())
                .certificateSerial(context.certSerial())
                .certificateFingerprint(context.certFingerprint())
                .mqttReceivedAt(context.receivedAt() == null
                        ? System.currentTimeMillis() : context.receivedAt().toEpochMilli())
                .build();
    }

    private static VmdRelayResult mapResponse(VmdKeyProvisioningResponse response) {
        if (response == null) {
            return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
        }
        return toResult(response.getRequestId(), response.getPayloadType(), response.getPayloadBytes(),
                response.getExpireAtMs(), response.getOutcome(), response.getError());
    }

    private static VmdRelayResult mapAck(VmdKeyProvisioningAckResult result) {
        if (result == null) {
            return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
        }
        return toResult(result.getRequestId(), result.getPayloadType(), result.getPayloadBytes(),
                null, result.getOutcome(), result.getError());
    }

    private static VmdRelayResult mapStatus(VmdKeyProvisioningStatusResponse response) {
        if (response == null) {
            return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
        }
        return toResult(response.getRequestId(), response.getPayloadType(), response.getPayloadBytes(),
                null, response.getOutcome(), response.getError());
    }

    private static VmdRelayResult toResult(String requestId, String payloadType, byte[] payloadBytes,
                                           Long expireAtMs, VmdKeyProvisioningOutcome outcome,
                                           VmdKeyProvisioningError error) {
        if (outcome == VmdKeyProvisioningOutcome.SUCCESS) {
            if (payloadType == null || payloadType.isBlank() || payloadBytes == null) {
                return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
            }
            return new VmdRelayResult(requestId, payloadType, payloadBytes, expireAtMs,
                    KeyprovOutcome.SUCCESS, null);
        }
        KeyprovReason reason = mapReason(outcome, error);
        if (outcome == VmdKeyProvisioningOutcome.OUTCOME_UNKNOWN) {
            return VmdRelayResult.outcomeUnknown(requestId, reason);
        }
        if (outcome == VmdKeyProvisioningOutcome.REJECTED) {
            return VmdRelayResult.rejected(requestId, reason);
        }
        return VmdRelayResult.unavailable(reason);
    }

    /**
     * VMD 806090～806100 → VAGW 804010 稳定 reason 精确映射（EDD-VAGW-DSN-CR-016 §5）。
     */
    private static KeyprovReason mapReason(VmdKeyProvisioningOutcome outcome, VmdKeyProvisioningError error) {
        String code = error == null ? null : error.getCode();
        if (code != null) {
            KeyprovReason mapped = switch (code) {
                case "806090" -> KeyprovReason.KEYPROV_VMD_CALLER_UNAUTHORIZED;
                case "806091" -> KeyprovReason.KEYPROV_IDENTITY_MISMATCH;
                case "806092" -> KeyprovReason.KEYPROV_BINDING_INVALID;
                case "806093" -> KeyprovReason.KEYPROV_CERTIFICATE_INVALID;
                case "806094" -> KeyprovReason.KEYPROV_REQUEST_INVALID;
                case "806095" -> KeyprovReason.KEYPROV_IDEMPOTENCY_CONFLICT;
                case "806096" -> KeyprovReason.KEYPROV_VMD_UNAVAILABLE;
                case "806097" -> KeyprovReason.KEYPROV_VMD_REJECTED;
                case "806098" -> KeyprovReason.KEYPROV_OUTCOME_UNKNOWN;
                case "806099" -> KeyprovReason.KEYPROV_BATCH_NOT_FOUND;
                case "806100" -> KeyprovReason.KEYPROV_VMD_UNAVAILABLE;
                default -> null;
            };
            if (mapped != null) {
                return mapped;
            }
        }
        VmdKeyProvisioningOutcome effective =
                outcome == null ? VmdKeyProvisioningOutcome.UNAVAILABLE : outcome;
        return switch (effective) {
            case REJECTED -> KeyprovReason.KEYPROV_VMD_REJECTED;
            case OUTCOME_UNKNOWN -> KeyprovReason.KEYPROV_OUTCOME_UNKNOWN;
            case UNAVAILABLE, SUCCESS -> KeyprovReason.KEYPROV_VMD_UNAVAILABLE;
        };
    }

    private VmdRelayResult classify(Throwable throwable) {
        Throwable root = rootCause(throwable);
        if (isConnectFailure(root)) {
            return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
        }
        if (isConnectTimeout(root)) {
            return VmdRelayResult.timeout(KeyprovReason.KEYPROV_VMD_TIMEOUT);
        }
        if (root instanceof SocketTimeoutException) {
            // 请求已发出后读取超时：结果未知，不自动重放
            return VmdRelayResult.outcomeUnknown(null, KeyprovReason.KEYPROV_OUTCOME_UNKNOWN);
        }
        if (throwable instanceof FeignException feignException) {
            int status = feignException.status();
            // 404/405：目标端点未注册或方法不支持（VMD Keyprov 能力未部署/未启用），
            // 属于依赖能力不可用而非业务拒绝，按 UNAVAILABLE 上报并计入熔断，避免误判为设备业务失败。
            if (status == 404 || status == 405) {
                return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
            }
            if (status >= 500) {
                return VmdRelayResult.unavailable(KeyprovReason.KEYPROV_VMD_UNAVAILABLE);
            }
            if (status >= 400) {
                return VmdRelayResult.rejected(null, KeyprovReason.KEYPROV_VMD_REJECTED);
            }
        }
        // 其他网络/IO 异常：无法证明请求未发出，按结果未知 fail-closed
        return VmdRelayResult.outcomeUnknown(null, KeyprovReason.KEYPROV_OUTCOME_UNKNOWN);
    }

    private static boolean isNotSent(Throwable throwable) {
        Throwable root = rootCause(throwable);
        return isConnectFailure(root) || isConnectTimeout(root);
    }

    private static boolean isConnectFailure(Throwable root) {
        return root instanceof ConnectException
                || root instanceof UnknownHostException
                || root instanceof NoRouteToHostException;
    }

    private static boolean isConnectTimeout(Throwable root) {
        if (!(root instanceof SocketTimeoutException)) {
            return false;
        }
        String message = root.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains("connect");
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        int depth = 0;
        while (current.getCause() != null && current.getCause() != current && depth++ < 10) {
            current = current.getCause();
        }
        return current;
    }

    private static boolean isDependencyFailure(VmdRelayResult result) {
        return result.outcome() == KeyprovOutcome.UNAVAILABLE
                || result.outcome() == KeyprovOutcome.TIMEOUT
                || result.outcome() == KeyprovOutcome.OUTCOME_UNKNOWN;
    }

    private String metricLabel(VmdRelayResult result) {
        return switch (result.outcome()) {
            case SUCCESS -> "success";
            case REJECTED -> "rejected";
            default -> result.outcome().name().toLowerCase(Locale.ROOT);
        };
    }

    private long deadlineMillis() {
        return properties.getVmd().getDeadline() == null
                ? 0L : Math.max(0L, properties.getVmd().getDeadline().toMillis());
    }

    private static String sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes == null ? new byte[0] : bytes));
        } catch (Exception e) {
            // SHA-256 必然可用；退化用长度避免 NPE（不输出内容）
            return "len:" + (bytes == null ? 0 : bytes.length);
        }
    }

    private static long elapsedMillis(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }
}
