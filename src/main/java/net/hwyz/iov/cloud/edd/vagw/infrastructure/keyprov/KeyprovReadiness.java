package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Keyprov 联合 readiness 状态（EDD-VAGW-DSN-CR-017 §9）。
 * <p>
 * KEYPROV_READY 是代码内唯一、可查询的状态对象；MQTT 订阅、ACL、ingress 与 relay 均共享
 * 同一个实例，不允许各入口自行重复判断。任一条件不满足只关闭 Keyprov capability
 * （FOTA／远控／MQTT 连接不受影响）。
 * </p>
 * <p>
 * 状态对象不承载敏感材料，只包含布尔条件与稳定 reason。
 * </p>
 *
 * @param ready                           全部条件满足
 * @param reason                           首个失败条件对应的稳定 reason（ready 时为 {@code null}）
 * @param notReadyDetail                   非敏感诊断说明（字段名/哈希不一致项，不含 payload）
 * @param parProtoCr008ReleaseMatched      PAR-PROTO release 与清单/制品一致
 * @param descriptorSetHashMatched         DescriptorSet SHA-256 与清单一致
 * @param packageServiceProtocolRegistryMatched package/service/protocol/registry 精确匹配
 * @param generatedRuntimeSelfCheckPassed  生成类字段/枚举/runtime 自检通过
 * @param vmdCr062ApiCompatible            edd-vmd-api 含 CR-062 语义
 * @param keyprovAckCr008AdapterReady      ACK 字段 10/11 与长度门禁可用
 * @param canonicalPayloadPassthroughReady canonical payload 原字节直传路径可用
 * @param importProofSensitiveLoggingGuardReady 敏感材料日志/序列化门禁通过
 * @param goldenVectorSetDigestMatched     release-set vector bundle digest 匹配
 */
public record KeyprovReadiness(
        boolean ready,
        KeyprovReason reason,
        String notReadyDetail,
        boolean parProtoCr008ReleaseMatched,
        boolean descriptorSetHashMatched,
        boolean packageServiceProtocolRegistryMatched,
        boolean generatedRuntimeSelfCheckPassed,
        boolean vmdCr062ApiCompatible,
        boolean keyprovAckCr008AdapterReady,
        boolean canonicalPayloadPassthroughReady,
        boolean importProofSensitiveLoggingGuardReady,
        boolean goldenVectorSetDigestMatched) {

    /**
     * 协议/API 只读路径是否可用（不要求完整 READY）。
     * <p>
     * Status 对账只在 canonical 包/服务/协议/registry、生成物和 VMD CR-062 API 均兼容时保留；
     * release/descriptor/golden 漂移不阻断只读对账。
     * </p>
     */
    public boolean protocolApiCompatible() {
        return packageServiceProtocolRegistryMatched
                && generatedRuntimeSelfCheckPassed
                && vmdCr062ApiCompatible
                && keyprovAckCr008AdapterReady
                && canonicalPayloadPassthroughReady;
    }

    /**
     * 统一门控：哪些上行 PayloadType 可被接受。
     * <p>
     * Request/Ack 要求完整 READY；StatusRequest 在协议/API 兼容且只读路径可用时保留。
     * </p>
     */
    public boolean allows(KeyprovMessageType type) {
        if (type == null) {
            return false;
        }
        if (ready) {
            return true;
        }
        return type == KeyprovMessageType.KEY_PROVISIONING_STATUS_REQUEST && protocolApiCompatible();
    }

    /** 低基数 readiness 条件快照（用于 metrics 与结构化日志）。 */
    public Map<String, Boolean> snapshot() {
        Map<String, Boolean> conditions = new LinkedHashMap<>();
        conditions.put("parProtoCr008ReleaseMatched", parProtoCr008ReleaseMatched);
        conditions.put("descriptorSetHashMatched", descriptorSetHashMatched);
        conditions.put("packageServiceProtocolRegistryMatched", packageServiceProtocolRegistryMatched);
        conditions.put("generatedRuntimeSelfCheckPassed", generatedRuntimeSelfCheckPassed);
        conditions.put("vmdCr062ApiCompatible", vmdCr062ApiCompatible);
        conditions.put("keyprovAckCr008AdapterReady", keyprovAckCr008AdapterReady);
        conditions.put("canonicalPayloadPassthroughReady", canonicalPayloadPassthroughReady);
        conditions.put("importProofSensitiveLoggingGuardReady", importProofSensitiveLoggingGuardReady);
        conditions.put("goldenVectorSetDigestMatched", goldenVectorSetDigestMatched);
        return conditions;
    }
}
