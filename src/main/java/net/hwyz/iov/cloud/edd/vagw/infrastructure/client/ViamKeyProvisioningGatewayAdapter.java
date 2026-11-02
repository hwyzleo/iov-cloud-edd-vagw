package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.ViamKeyProvisioningCommand;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.ViamKeyProvisioningGateway;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.ViamRelayResult;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovCircuitBreaker;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovProtocolLock;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.sec.viam.api.service.ViamKeyProvisioningService;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamAuthenticatedDeviceContext;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningAckRequest;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningAckResult;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningError;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningOutcome;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningRequest;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningResponse;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningStatusRequest;
import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningStatusResponse;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResponse;
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
 * VIAM Key Provisioning Gateway Adapter（EDD-VAGW-DSN-CR-016 §2.4，owner 由
 * EDD-VAGW-DSN-CR-019 切换：VMD → VIAM）。
 * <p>
 * 实现 VAGW 应用层 {@link ViamKeyProvisioningGateway} Port，直接委托 sec-viam-api 官方
 * {@link ViamKeyProvisioningService} Feign Bean，并完成：
 * <ul>
 *   <li>认证会话上下文 → {@link ViamAuthenticatedDeviceContext}（官方契约不含\n
 *       fingerprint/mqttReceivedAt；证书有效性保障在 VIAM 准入 decide 完成）；</li>
 *   <li>按 canonical 上行消息类型分派官方三端点：Request→/request、Ack→/ack、StatusRequest→/status；</li>
 *   <li>wire 携带 payloadType、schemaVersion、canonical payloadBytes 与 sha-256 payloadDigest，\n
 *       VAGW 不做 JSON↔proto 转换；</li>
 *   <li>Request 成功路径的下行 TTL：从 canonical {@code KeyProvisioningResponse.expire_at_ms}\n
 *       （field 4，官方生成类型解析）提取，与 VMD 语义一致；缺失/零值/非法/已过期 fail-closed 拒绝下发，\n
 *       不回退默认 TTL、不重新起算有效期、不重编码 canonical bytes（CR-019 §18 核验结论）。\n
 *       ACK/Status 响应无该字段（不判非法），保持 null（与既有行为一致）；</li>
 *   <li>官方响应 outcome／702xxx → VAGW 稳定 reason 映射（804010，不复制\n
 *       {@code ViamLegacyCodeMapping}，未知错误保持拒绝）；</li>
 *   <li>统一 absolute deadline 下的有限重试（仅在明确证明请求未发出时、同一 requestId 内）；</li>
 *   <li>Keyprov 专属熔断治理（仅统计依赖类失败）。</li>
 * </ul>
 * 不定义第二套 {@code @FeignClient}、URL、服务名常量、wire DTO 或 fallback；不新增 per-client\n
 * 物理连接池；不向 VAGW 暴露 KMS 内部 keyRef，也不执行 VIAM 业务校验或密钥编排。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class ViamKeyProvisioningGatewayAdapter implements ViamKeyProvisioningGateway {

    private final ViamKeyProvisioningService viamService;
    private final KeyprovProperties properties;
    private final KeyprovCircuitBreaker circuitBreaker;
    private final KeyprovMetrics metrics;
    private final KeyprovContractGuard contractGuard;

    @Override
    public ViamRelayResult relay(ViamKeyProvisioningCommand command) {
        KeyprovProtocolLock lock = contractGuard.lock();
        if (lock == null) {
            metrics.viamCall("lock_not_ready");
            return ViamRelayResult.rejected(command.envelope().getRequestId(),
                    KeyprovReason.KEYPROV_LOCK_MANIFEST_INVALID);
        }
        if (!circuitBreaker.allowRequest()) {
            metrics.viamCall("circuit_open");
            return ViamRelayResult.circuitOpen(KeyprovReason.KEYPROV_CIRCUIT_OPEN);
        }
        long startedAt = System.nanoTime();
        try {
            ViamRelayResult result = dispatch(command);
            metrics.viamLatency(elapsedMillis(startedAt));
            if (isDependencyFailure(result)) {
                circuitBreaker.recordDependencyFailure();
            } else {
                circuitBreaker.recordSuccess();
            }
            metrics.viamCall(metricLabel(result));
            return result;
        } catch (RuntimeException e) {
            metrics.viamLatency(elapsedMillis(startedAt));
            ViamRelayResult result = classify(e, command.envelope().getRequestId());
            if (result.dependencyFailure()) {
                circuitBreaker.recordDependencyFailure();
            } else {
                circuitBreaker.recordSuccess();
            }
            metrics.viamCall(metricLabel(result));
            // 只输出异常类型与稳定 reason，不输出请求/响应内容
            log.warn("Keyprov VIAM call failed: outcome={}, reason={}, exception={}",
                    result.outcome(), result.reason(), e.getClass().getSimpleName());
            return result;
        }
    }

    /**
     * 按 canonical 上行消息类型分派官方 request/ack/status 端点。
     */
    private ViamRelayResult dispatch(ViamKeyProvisioningCommand command) {
        Optional<KeyprovMessageType> type = KeyprovMessageType.fromFullName(command.envelope().getPayloadType());
        if (type.isEmpty() || !type.get().isUpstream()) {
            return ViamRelayResult.rejected(command.envelope().getRequestId(),
                    KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED);
        }
        return invokeWithBoundedRetry(command, type.get());
    }

    /**
     * 统一 absolute deadline 下的有限重试：仅在明确证明请求未发出（连接失败/连接超时）且仍有
     * deadline 预算时，允许一次、同一 requestId 的重试；请求已发送后一律不重放。
     */
    private ViamRelayResult invokeWithBoundedRetry(ViamKeyProvisioningCommand command, KeyprovMessageType type) {
        long deadlineAt = System.currentTimeMillis() + deadlineMillis();
        try {
            return invoke(command, type);
        } catch (RuntimeException e) {
            if (isNotSent(e) && System.currentTimeMillis() < deadlineAt) {
                metrics.viamCall("retry_not_sent");
                return invoke(command, type);
            }
            throw e;
        }
    }

    private ViamRelayResult invoke(ViamKeyProvisioningCommand command, KeyprovMessageType type) {
        KeyprovProtocolLock lock = contractGuard.lock();
        if (lock == null) {
            return ViamRelayResult.rejected(command.envelope().getRequestId(),
                    KeyprovReason.KEYPROV_LOCK_MANIFEST_INVALID);
        }
        ViamAuthenticatedDeviceContext context = toDeviceContext(command.context());
        String payloadType = command.envelope().getPayloadType();
        String schemaVersion = command.envelope().getProtocolVersion();
        byte[] payload = command.canonicalPayload();
        String digest = sha256(payload);
        // 诊断：仅输出非敏感协议参数与摘要指纹，便于与 VIAM 门禁日志逐项比对（不输出 payload 本体）
        if (log.isDebugEnabled()) {
            log.debug("Keyprov VIAM 调用参数: type={}, payloadType={}, schemaVersion={}, payloadBytes={}, digest={}",
                    type, payloadType, schemaVersion, payload == null ? 0 : payload.length,
                    digestFingerprint(digest));
        }
        return switch (type) {
            // 可信锁版上下文只进入官方 /request（VIAM 官方 Request DTO 定义 protocolRelease/descriptorSetHash）；
            // ACK/Status 按官方契约不重复声明，VAGW 不得复制字段或另建 DTO。
            case KEY_PROVISIONING_REQUEST -> mapResponse(viamService.request(ViamKeyProvisioningRequest.builder()
                    .deviceContext(context).payloadType(payloadType).schemaVersion(schemaVersion)
                    .protocolRelease(lock.parProtoRelease())
                    .descriptorSetHash(lock.descriptorSetSha256())
                    .payloadBytes(payload).payloadDigest(digest).build()));
            case KEY_PROVISIONING_ACK -> mapAck(viamService.ack(ViamKeyProvisioningAckRequest.builder()
                    .deviceContext(context).payloadType(payloadType).schemaVersion(schemaVersion)
                    .payloadBytes(payload).payloadDigest(digest).build()));
            case KEY_PROVISIONING_STATUS_REQUEST -> mapStatus(viamService.status(
                    ViamKeyProvisioningStatusRequest.builder()
                            .deviceContext(context).payloadType(payloadType).schemaVersion(schemaVersion)
                            .payloadBytes(payload).payloadDigest(digest).build()));
            default -> ViamRelayResult.rejected(command.envelope().getRequestId(),
                    KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED);
        };
    }

    private static ViamAuthenticatedDeviceContext toDeviceContext(BrokerAuthenticatedPublishContext context) {
        // 官方 ViamAuthenticatedDeviceContext 契约不含 certificateFingerprint/mqttReceivedAt；
        // 证书有效性由 VIAM 准入 decide 全量校验，Keyprov 仅以 hsmUid/serial/generation 交叉校验。
        return ViamAuthenticatedDeviceContext.builder()
                .deviceSn(null)
                .hsmUid(context.hsmUid())
                .ecuUid(null)
                .brokerSessionId(context.brokerSessionId())
                .connectionGeneration(context.connectionGeneration())
                .certificateSerial(context.certSerial())
                .build();
    }

    private ViamRelayResult mapResponse(ViamKeyProvisioningResponse response) {
        if (response == null) {
            return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
        }
        if (response.getOutcome() == ViamKeyProvisioningOutcome.SUCCESS) {
            // SUCCESS：必须从 canonical KeyProvisioningResponse 提取 expire_at_ms（field 4）。
            // 该字段为 Wrapped Key bundle 可接受期限（下行 Envelope TTL 的安全依据），
            // 缺失/零值/解析失败 → fail-closed 拒绝下发（CR-019 §18；不回退默认 TTL）。
            Long expireAtMs = extractExpireAtMs(response);
            if (expireAtMs == null) {
                log.warn("Keyprov VIAM SUCCESS 但 canonical KeyProvisioningResponse 缺少有效 expire_at_ms，"
                        + "fail-closed 拒绝下发: requestId={}", response.getRequestId());
                return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
            }
            if (expireAtMs <= System.currentTimeMillis()) {
                log.warn("Keyprov VIAM SUCCESS 但 expire_at_ms 已过期，fail-closed 拒绝下发: requestId={}, "
                        + "expireAtMs={}", response.getRequestId(), expireAtMs);
                return ViamRelayResult.rejected(response.getRequestId(), KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID);
            }
            if (isBlank(response.getPayloadType()) || response.getPayloadBytes() == null) {
                log.warn("Keyprov VIAM SUCCESS 但缺少 canonical payload: requestId={}, payloadType={}, payloadBytes={}",
                        response.getRequestId(), response.getPayloadType(),
                        response.getPayloadBytes() == null ? 0 : response.getPayloadBytes().length);
                return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
            }
            return new ViamRelayResult(response.getRequestId(), response.getPayloadType(),
                    response.getPayloadBytes(), expireAtMs, KeyprovOutcome.SUCCESS, null, false);
        }
        return mapNonSuccess(response.getRequestId(), response.getPayloadType(), response.getPayloadBytes(),
                null, response.getOutcome(), response.getError());
    }

    /**
     * 从官方 canonical KeyProvisioningResponse 提取 expire_at_ms（field 4，int64 UTC epoch ms）。
     * <p>
     * 使用官方生成类型 {@link KeyProvisioningResponse} 解析（不手写通用字段提取器）；
     * 仅 Request 成功响应适用；AckResult/StatusResponse 不含该字段，不判非法（mapAck/mapStatus 传 null）。
     * </p>
     *
     * @return 有效期毫秒；缺失/零值/解析失败返回 null（调用方 fail-closed）
     */
    private static Long extractExpireAtMs(ViamKeyProvisioningResponse response) {
        byte[] payload = response.getPayloadBytes();
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            KeyProvisioningResponse proto = KeyProvisioningResponse.parseFrom(payload);
            // proto3 标量字段无 presence；expire_at_ms=0 表示未设置（PAR-PROTO 时间语义），
            // 仅正值视为有效有效期
            if (proto.getExpireAtMs() <= 0) {
                return null;
            }
            return proto.getExpireAtMs();
        } catch (Exception e) {
            log.warn("Keyprov VIAM canonical KeyProvisioningResponse 解析失败，fail-closed: err={}",
                    e.getClass().getSimpleName());
            return null;
        }
    }

    private ViamRelayResult mapAck(ViamKeyProvisioningAckResult result) {
        if (result == null) {
            return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
        }
        // AckResult 无 expire_at_ms（proto field 4=error），保持 null（与既有行为一致）
        return mapNonSuccess(result.getRequestId(), result.getPayloadType(), result.getPayloadBytes(),
                null, result.getOutcome(), result.getError());
    }

    private ViamRelayResult mapStatus(ViamKeyProvisioningStatusResponse response) {
        if (response == null) {
            return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
        }
        // StatusResponse 无 expire_at_ms（proto field 4=occurred_at_ms），保持 null
        return mapNonSuccess(response.getRequestId(), response.getPayloadType(), response.getPayloadBytes(),
                null, response.getOutcome(), response.getError());
    }

    private ViamRelayResult mapNonSuccess(String requestId, String payloadType, byte[] payloadBytes,
                                          Long expireAtMs, ViamKeyProvisioningOutcome outcome,
                                          ViamKeyProvisioningError error) {
        if (outcome == ViamKeyProvisioningOutcome.SUCCESS) {
            if (isBlank(payloadType) || payloadBytes == null) {
                log.warn("Keyprov VIAM 返回 SUCCESS 但缺少 canonical payload: requestId={}, payloadType={}, payloadBytes={}",
                        requestId, payloadType, payloadBytes == null ? 0 : payloadBytes.length);
                return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
            }
            return new ViamRelayResult(requestId, payloadType, payloadBytes, expireAtMs,
                    KeyprovOutcome.SUCCESS, null, false);
        }
        // 诊断：VAGW 对外只上报稳定 reason，这里额外保留 VIAM 登记的 702xxx 与脱敏 message，
        // 便于按 requestId 与 VIAM 侧日志关联定位（不含 payload / Wrapped Key）
        log.warn("Keyprov VIAM 返回非成功: requestId={}, outcome={}, code={}, retryable={}, action={}, message={}",
                requestId, outcome,
                error == null ? null : error.getCode(),
                error == null ? null : error.isRetryable(),
                error == null ? null : error.getAction(),
                error == null ? null : error.getMessage());
        KeyprovReason reason = mapReason(outcome, error);
        KeyprovOutcome relayOutcome = toRelayOutcome(outcome);
        // 仅 VIAM 明确返回依赖不可用（UNAVAILABLE/依赖类 retryable）计入熔断；
        // 确定性拒绝/结果未知不计入依赖故障率（未知结果以原 requestId 对账）。
        boolean dependencyFailure = relayOutcome == KeyprovOutcome.UNAVAILABLE;
        // VIAM 在非成功结果上同样返回 canonical 下行 payload（ProtocolError）：VAGW 原样转发，
        // 车端据 result/error.retryable 收敛（BLOCKED 或按原 requestId 重试），不再盲重投
        if (!isBlank(payloadType) && payloadBytes != null) {
            log.info("Keyprov VIAM 非成功但返回 canonical 下行 payload: requestId={}, outcome={}, "
                            + "downlinkPayloadType={}, payloadBytes={}",
                    requestId, relayOutcome, payloadType, payloadBytes.length);
            return new ViamRelayResult(requestId, payloadType, payloadBytes, null, relayOutcome, reason,
                    dependencyFailure);
        }
        if (relayOutcome == KeyprovOutcome.OUTCOME_UNKNOWN) {
            return ViamRelayResult.outcomeUnknown(requestId, reason, false);
        }
        if (relayOutcome == KeyprovOutcome.REJECTED) {
            return ViamRelayResult.rejected(requestId, reason);
        }
        return ViamRelayResult.unavailable(reason);
    }

    private static KeyprovOutcome toRelayOutcome(ViamKeyProvisioningOutcome outcome) {
        if (outcome == null) {
            return KeyprovOutcome.UNAVAILABLE;
        }
        return switch (outcome) {
            case SUCCESS -> KeyprovOutcome.SUCCESS;
            case REJECTED -> KeyprovOutcome.REJECTED;
            case OUTCOME_UNKNOWN -> KeyprovOutcome.OUTCOME_UNKNOWN;
            case UNAVAILABLE -> KeyprovOutcome.UNAVAILABLE;
        };
    }

    /**
     * VIAM 702xxx → VAGW 稳定 reason 精确映射（EDD-VAGW-DSN-CR-019 §4.3）。
     * <p>
     * 确定性协议/签名/绑定拒绝不得包装成依赖故障；依赖类（702309/702414/702210 等）保留原
     * requestId 且计入熔断；未登记/未知错误一律 fail-closed（INTERNAL_UNAVAILABLE → 804010）。
     * VAGW 不复制 {@code ViamLegacyCodeMapping}（702xxx↔806xxx 属 VIAM 内部迁移分类）。
     * </p>
     */
    private static KeyprovReason mapReason(ViamKeyProvisioningOutcome outcome, ViamKeyProvisioningError error) {
        String code = error == null ? null : error.getCode();
        if (code != null) {
            KeyprovReason mapped = switch (code) {
                case "702101", "702102", "702103", "702301" -> KeyprovReason.KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH;
                case "702104", "702108" -> KeyprovReason.KEYPROV_VIAM_BINDING_INVALID;
                case "702105", "702106", "702110", "702308" -> KeyprovReason.KEYPROV_VIAM_CERTIFICATE_INVALID;
                case "702109" -> KeyprovReason.KEYPROV_VIAM_DIRECTORY_UNAVAILABLE;
                case "702201", "702202" -> KeyprovReason.KEYPROV_VIAM_CERTIFICATE_INVALID;
                case "702203", "702204", "702205", "702206", "702207" -> KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID;
                case "702208" -> KeyprovReason.KEYPROV_VIAM_DIRECTORY_UNAVAILABLE;
                case "702209", "702414" -> KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN;
                case "702210" -> KeyprovReason.KEYPROV_VIAM_REJECTED;
                case "702302" -> KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID;
                case "702303", "702304" -> KeyprovReason.KEYPROV_VIAM_BINDING_INVALID;
                case "702306", "702405", "702411", "702415" -> KeyprovReason.KEYPROV_VIAM_IDEMPOTENCY_CONFLICT;
                case "702307" -> KeyprovReason.KEYPROV_VIAM_IDEMPOTENCY_CONFLICT;
                case "702309" -> KeyprovReason.KEYPROV_VIAM_DIRECTORY_UNAVAILABLE;
                case "702310", "702313" -> KeyprovReason.KEYPROV_VIAM_WRAP_FAILED;
                case "702312" -> KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN;
                case "702314" -> KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID;
                case "702401", "702402", "702404", "702406" -> KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID;
                case "702403" -> KeyprovReason.KEYPROV_VIAM_BATCH_NOT_FOUND;
                case "702407", "702408" -> KeyprovReason.KEYPROV_VIAM_BINDING_INVALID;
                case "702409", "702410", "702413" -> KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID;
                case "702412" -> KeyprovReason.KEYPROV_VIAM_REJECTED;
                case "702501", "702502", "702503", "702504", "702505" -> KeyprovReason.KEYPROV_VIAM_REQUEST_INVALID;
                default -> null;
            };
            if (mapped != null) {
                return mapped;
            }
        }
        ViamKeyProvisioningOutcome effective =
                outcome == null ? ViamKeyProvisioningOutcome.UNAVAILABLE : outcome;
        return switch (effective) {
            case REJECTED -> KeyprovReason.KEYPROV_VIAM_REJECTED;
            case OUTCOME_UNKNOWN -> KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN;
            case UNAVAILABLE, SUCCESS -> KeyprovReason.KEYPROV_VIAM_UNAVAILABLE;
        };
    }

    private ViamRelayResult classify(Throwable throwable, String requestId) {
        Throwable root = rootCause(throwable);
        if (isConnectFailure(root)) {
            return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
        }
        if (isConnectTimeout(root)) {
            return ViamRelayResult.timeout(KeyprovReason.KEYPROV_VIAM_TIMEOUT);
        }
        if (root instanceof SocketTimeoutException) {
            // 请求已发出后读取超时：结果未知，不自动重放；保留原 requestId 供 /status 对账
            return ViamRelayResult.outcomeUnknown(requestId, KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN, true);
        }
        if (throwable instanceof FeignException feignException) {
            int status = feignException.status();
            // 404/405：目标端点未注册或方法不支持（VIAM Keyprov 能力未部署/未启用），
            // 属于依赖能力不可用而非业务拒绝，按 UNAVAILABLE 上报并计入熔断。
            if (status == 404 || status == 405) {
                return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
            }
            if (status >= 500) {
                return ViamRelayResult.unavailable(KeyprovReason.KEYPROV_VIAM_UNAVAILABLE);
            }
            if (status >= 400) {
                return ViamRelayResult.rejected(requestId, KeyprovReason.KEYPROV_VIAM_REJECTED);
            }
        }
        // 其他网络/IO 异常：无法证明请求未发出，按结果未知 fail-closed；保留原 requestId
        return ViamRelayResult.outcomeUnknown(requestId, KeyprovReason.KEYPROV_VIAM_OUTCOME_UNKNOWN, true);
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

    private static boolean isDependencyFailure(ViamRelayResult result) {
        return result.dependencyFailure();
    }

    private String metricLabel(ViamRelayResult result) {
        return switch (result.outcome()) {
            case SUCCESS -> "success";
            case REJECTED -> "rejected";
            default -> result.outcome().name().toLowerCase(Locale.ROOT);
        };
    }

    private long deadlineMillis() {
        return properties.getViam().getDeadline() == null
                ? 0L : Math.max(0L, properties.getViam().getDeadline().toMillis());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
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

    /**
     * 摘要指纹：仅保留前 16 位便于端到端比对，属不可逆哈希。
     */
    private static String digestFingerprint(String digest) {
        if (digest == null || digest.isBlank()) {
            return "<missing>";
        }
        String value = digest.trim();
        return value.length() <= 16 ? value : value.substring(0, 16) + "…(len=" + value.length() + ")";
    }

    private static long elapsedMillis(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000L;
    }
}
