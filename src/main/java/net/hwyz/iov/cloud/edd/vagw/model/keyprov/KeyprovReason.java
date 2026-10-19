package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

/**
 * keyprov 稳定失败/审计 reason（EDD-VAGW-DSN-CR-011 §7）。
 * <p>
 * 全部 keyprov 失败统一映射网关错误码 804017（KEYPROV_UPSTREAM_FAILURE），
 * 具体原因由本枚举表达；身份不一致单独使用 804003。
 * </p>
 */
public enum KeyprovReason {
    /** 能力未启用（keyprov.enabled=false）。 */
    KEYPROV_DISABLED,
    /** service/protocol_version/message_kind/payload_type/方向不匹配精确契约。 */
    KEYPROV_PROTOCOL_UNSUPPORTED,
    /** payload 结构非法或不可 canonical 解码。 */
    KEYPROV_PAYLOAD_INVALID,
    /** payload 超过 16 KiB。 */
    KEYPROV_PAYLOAD_TOO_LARGE,
    /** 上行 Envelope 的 expire_at_ms 已过期（TTL 超时，未进入 VMD 调用）。 */
    KEYPROV_MESSAGE_EXPIRED,
    /** 会话/认证上下文缺失（无活动会话或无法关联认证身份）。 */
    KEYPROV_SESSION_CONTEXT_MISSING,
    /** 会话已失效或 connection generation 过期。 */
    KEYPROV_SESSION_STALE,
    /** VMD 明确拒绝且未返回 canonical 下行 payload。 */
    KEYPROV_VMD_REJECTED,
    /** VMD 不可达（超时/连接失败/熔断），或端点未部署/未启用（HTTP 404/405）。 */
    KEYPROV_VMD_UNAVAILABLE,
    /** VMD 结果未知（已发送未收到确定结果）；不得自动更换 requestId 重试。 */
    KEYPROV_OUTCOME_UNKNOWN,
    /** MQTT 下行发布失败。 */
    KEYPROV_DOWNLINK_PUBLISH_FAILED,
    /** Topic/会话/Envelope 设备身份不一致（804003）。 */
    IDENTITY_MISMATCH,
    /** Keyprov 并发许可耗尽（有界 bulkhead 饱和），未调用 VMD（EDD-VAGW-DSN-CR-016 §5）。 */
    KEYPROV_OVERLOADED,
    /** 专属执行器拒绝提交（容量/线程耗尽），未调用 VMD。 */
    KEYPROV_EXECUTION_REJECTED,
    /** Keyprov 专属熔断器 OPEN，快速失败且不占用 HTTP 连接。 */
    KEYPROV_CIRCUIT_OPEN,
    /** VMD 连接建立阶段超时（请求未发出，属于依赖不可用）。 */
    KEYPROV_VMD_TIMEOUT,
    // ------------------------------------------------------------------
    // EDD-VAGW-DSN-CR-016（VMD CR059 对齐）：VMD 官方 806xxx 稳定业务 reason 映射
    // ------------------------------------------------------------------
    /** VMD 调用方身份未授权（806090）。 */
    KEYPROV_VMD_CALLER_UNAUTHORIZED,
    /** 可信设备上下文与 VMD 记录不一致（806091）。 */
    KEYPROV_IDENTITY_MISMATCH,
    /** VMD 设备绑定失效或不可用（806092）。 */
    KEYPROV_BINDING_INVALID,
    /** VMD 设备证书失效或不可用（806093）。 */
    KEYPROV_CERTIFICATE_INVALID,
    /** VMD 请求协议或内容非法（806094）。 */
    KEYPROV_REQUEST_INVALID,
    /** VMD 幂等键冲突（806095）。 */
    KEYPROV_IDEMPOTENCY_CONFLICT,
    /** VMD 开通批次不存在（806099）。 */
    KEYPROV_BATCH_NOT_FOUND,
    // ------------------------------------------------------------------
    // EDD-VAGW-DSN-CR-017：协议锁版、CR-008 Import Attestation 与 VMD CR-062
    // ------------------------------------------------------------------
    /** KeyprovProtocolLock 缺失、不可读或自相矛盾（部署错误，Keyprov NOT_READY）。 */
    KEYPROV_LOCK_MANIFEST_INVALID,
    /** PAR-PROTO release 与制品/清单不一致（Keyprov NOT_READY，不进入熔断统计）。 */
    KEYPROV_CONTRACT_RELEASE_MISMATCH,
    /** DescriptorSet SHA-256 与实际制品不一致（Keyprov NOT_READY，不进入熔断统计）。 */
    KEYPROV_DESCRIPTOR_HASH_MISMATCH,
    /** canonical payload-type registry 与清单不一致（Keyprov NOT_READY）。 */
    KEYPROV_REGISTRY_MISMATCH,
    /** 生成器/protobuf runtime 与清单不一致（Keyprov NOT_READY）。 */
    KEYPROV_RUNTIME_MISMATCH,
    /** edd-vmd-api 缺少 CR-062 字段/语义（Keyprov NOT_READY）。 */
    KEYPROV_VMD_API_INCOMPATIBLE,
    /** golden vector set digest 不匹配或缺失（Keyprov NOT_READY）。 */
    KEYPROV_GOLDEN_VECTOR_SET_MISMATCH,
    /** ACK item 的 import_proof_algorithm 为 0/未知（确定性拒绝，不熔断、不重试）。 */
    KEYPROV_ACK_ALGORITHM_INVALID,
    /** ACK item 的 import_proof 非恰好 64B，或非 IMPORTED 项伪造 proof（确定性拒绝）。 */
    KEYPROV_ACK_PROOF_INVALID,
    /** ACK item 的 recipient_certificate_digest 非恰好 32B（确定性拒绝）。 */
    KEYPROV_ACK_CERTIFICATE_DIGEST_INVALID,
    /** ACK 结构/字段组合非法，或 VMD/FW 返回确定性 Import Attestation 拒绝（806111）。 */
    KEYPROV_IMPORT_ATTESTATION_REJECTED,
    /** VMD/FW ImportProofVerifier 或 Provider 暂不可用（806105，依赖类，可重试）。 */
    KEYPROV_IMPORT_PROOF_VERIFIER_UNAVAILABLE,
    /** 证书材料/OCSP/CRL 暂不可用（806101，依赖类，可重试）。 */
    KEYPROV_CERTIFICATE_MATERIAL_UNAVAILABLE
}
