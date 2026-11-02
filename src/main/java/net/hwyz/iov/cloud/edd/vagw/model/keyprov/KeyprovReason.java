package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

/**
 * keyprov 稳定失败/审计 reason（EDD-VAGW-DSN-CR-011 §7，owner 由 EDD-VAGW-DSN-CR-019 切换 VIAM）。
 * <p>
 * 全部 keyprov 失败统一映射网关错误码 804017（KEYPROV_UPSTREAM_FAILURE），
 * 具体原因由本枚举表达；身份不一致单独使用 804003。
 * </p>
 * <p>
 * CR-019 旧值 → 新值映射（指标/告警迁移登记，不静默改名）：
 * <ul>
 *   <li>KEYPROV_VMD_REJECTED → KEYPROV_VIAM_REJECTED</li>
 *   <li>KEYPROV_VMD_UNAVAILABLE → KEYPROV_VIAM_UNAVAILABLE</li>
 *   <li>KEYPROV_VMD_TIMEOUT → KEYPROV_VIAM_TIMEOUT</li>
 *   <li>KEYPROV_VMD_API_INCOMPATIBLE → KEYPROV_VIAM_API_INCOMPATIBLE</li>
 *   <li>806090 → KEYPROV_VIAM_CALLER_UNAUTHORIZED（CALLER_UNAUTHORIZED）</li>
 *   <li>806091 → KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH（DEVICE_CONTEXT_MISMATCH）</li>
 *   <li>806092/806106 → KEYPROV_VIAM_BINDING_INVALID（BINDING_INVALID）</li>
 *   <li>806093/806102/806103/806104 → KEYPROV_VIAM_CERTIFICATE_INVALID（CERTIFICATE_INVALID）</li>
 *   <li>806094 → KEYPROV_VIAM_REQUEST_INVALID（REQUEST_INVALID）</li>
 *   <li>806095 → KEYPROV_VIAM_IDEMPOTENCY_CONFLICT（IDEMPOTENCY_CONFLICT）</li>
 *   <li>806096/806100/806107/702309 → KEYPROV_VIAM_DIRECTORY_UNAVAILABLE（DIRECTORY_UNAVAILABLE）</li>
 *   <li>806097 → KEYPROV_VIAM_REJECTED</li>
 *   <li>806098/702312/702414 → KEYPROV_VIAM_OUTCOME_UNKNOWN（OUTCOME_UNKNOWN）</li>
 *   <li>806099/702403 → KEYPROV_VIAM_BATCH_NOT_FOUND（BATCH_NOT_FOUND）</li>
 *   <li>702310 → KEYPROV_VIAM_WRAP_FAILED（WRAP_FAILED）</li>
 *   <li>其余未登记/未知 → KEYPROV_VIAM_INTERNAL_UNAVAILABLE（INTERNAL_UNAVAILABLE）</li>
 * </ul>
 * 对外稳定 reason 不静默改名：迁移窗口内指标/告警按本映射登记；VAGW 不复制
 * {@code ViamLegacyCodeMapping}（702xxx↔806xxx 属 VIAM 内部迁移分类）。
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
    /** 上行 Envelope 的 expire_at_ms 已过期（TTL 超时，未进入 VIAM 调用）。 */
    KEYPROV_MESSAGE_EXPIRED,
    /** 会话/认证上下文缺失（无活动会话或无法关联认证身份）。 */
    KEYPROV_SESSION_CONTEXT_MISSING,
    /** 会话已失效或 connection generation 过期。 */
    KEYPROV_SESSION_STALE,
    /** VIAM 明确拒绝且未返回 canonical 下行 payload。 */
    KEYPROV_VIAM_REJECTED,
    /** VIAM 不可达（超时/连接失败/熔断），或端点未部署/未启用（HTTP 404/405）。 */
    KEYPROV_VIAM_UNAVAILABLE,
    /** MQTT 下行发布失败。 */
    KEYPROV_DOWNLINK_PUBLISH_FAILED,
    /** Topic/会话/Envelope 设备身份不一致（804003）。 */
    IDENTITY_MISMATCH,
    /** Keyprov 并发许可耗尽（有界 bulkhead 饱和），未调用 VIAM（EDD-VAGW-DSN-CR-016 §5）。 */
    KEYPROV_OVERLOADED,
    /** 专属执行器拒绝提交（容量/线程耗尽），未调用 VIAM。 */
    KEYPROV_EXECUTION_REJECTED,
    /** Keyprov 专属熔断器 OPEN，快速失败且不占用 HTTP 连接。 */
    KEYPROV_CIRCUIT_OPEN,
    /** VIAM 连接建立阶段超时（请求未发出，属于依赖不可用）。 */
    KEYPROV_VIAM_TIMEOUT,
    // ------------------------------------------------------------------
    // EDD-VAGW-DSN-CR-019：VIAM 官方稳定语义分类（→ 804010，不复制 legacy 映射表）
    // ------------------------------------------------------------------
    /** VIAM 调用方身份未授权（CALLER_UNAUTHORIZED）。 */
    KEYPROV_VIAM_CALLER_UNAUTHORIZED,
    /** 可信设备上下文与 VIAM 记录不一致（DEVICE_CONTEXT_MISMATCH）。 */
    KEYPROV_VIAM_DEVICE_CONTEXT_MISMATCH,
    /** VIAM 设备绑定失效或不可用（BINDING_INVALID）。 */
    KEYPROV_VIAM_BINDING_INVALID,
    /** VIAM 设备证书失效或不可用（CERTIFICATE_INVALID）。 */
    KEYPROV_VIAM_CERTIFICATE_INVALID,
    /** VIAM 请求协议或内容非法（REQUEST_INVALID）。 */
    KEYPROV_VIAM_REQUEST_INVALID,
    /** VIAM 幂等键冲突（IDEMPOTENCY_CONFLICT）。 */
    KEYPROV_VIAM_IDEMPOTENCY_CONFLICT,
    /** VIAM 业务密钥目录/依赖暂不可用（DIRECTORY_UNAVAILABLE，依赖类，可重试）。 */
    KEYPROV_VIAM_DIRECTORY_UNAVAILABLE,
    /** VIAM 设备封装失败（WRAP_FAILED，确定性拒绝）。 */
    KEYPROV_VIAM_WRAP_FAILED,
    /** VIAM 密钥操作结果未知，须以原 requestId 对账（OUTCOME_UNKNOWN）。 */
    KEYPROV_VIAM_OUTCOME_UNKNOWN,
    /** VIAM 开通批次不存在（BATCH_NOT_FOUND）。 */
    KEYPROV_VIAM_BATCH_NOT_FOUND,
    /** VIAM 未登记/未知稳定错误（INTERNAL_UNAVAILABLE，fail-closed）。 */
    KEYPROV_VIAM_INTERNAL_UNAVAILABLE,
    // ------------------------------------------------------------------
    // EDD-VAGW-DSN-CR-017：协议锁版、CR-008 Import Attestation（保留，与 owner 无关）
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
    /** sec-viam-api 缺少协议锁版字段/语义（Keyprov NOT_READY）。 */
    KEYPROV_VIAM_API_INCOMPATIBLE,
    /** golden vector set digest 不匹配或缺失（Keyprov NOT_READY）。 */
    KEYPROV_GOLDEN_VECTOR_SET_MISMATCH,
    /** ACK item 的 import_proof_algorithm 为 0/未知（确定性拒绝，不熔断、不重试）。 */
    KEYPROV_ACK_ALGORITHM_INVALID,
    /** ACK item 的 import_proof 非恰好 64B，或非 IMPORTED 项伪造 proof（确定性拒绝）。 */
    KEYPROV_ACK_PROOF_INVALID,
    /** ACK item 的 recipient_certificate_digest 非恰好 32B（确定性拒绝）。 */
    KEYPROV_ACK_CERTIFICATE_DIGEST_INVALID,
    /** ACK 结构/字段组合非法，或 VIAM/FW 返回确定性 Import Attestation 拒绝。 */
    KEYPROV_IMPORT_ATTESTATION_REJECTED,
    /** VIAM/FW ImportProofVerifier 或 Provider 暂不可用（依赖类，可重试）。 */
    KEYPROV_IMPORT_PROOF_VERIFIER_UNAVAILABLE,
    /** 证书材料/OCSP/CRL 暂不可用（依赖类，可重试）。 */
    KEYPROV_CERTIFICATE_MATERIAL_UNAVAILABLE
}
