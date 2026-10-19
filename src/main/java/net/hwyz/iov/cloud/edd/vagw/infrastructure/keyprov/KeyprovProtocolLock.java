package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

/**
 * Keyprov canonical 协议锁版清单（EDD-VAGW-DSN-CR-017 §4）。
 * <p>
 * 只读、可复现的构建产物：由 {@code KeyprovProtocolLockManifestGenerator} 从实际依赖制品
 * （proto-keyprov artifact 的 DescriptorSet、payload-type registry、protobuf runtime）和
 * release-set fixture 生成；仓库内副本仅作为审核基线，不是可编辑的 {@code golden=true}
 * 开关。运行时读取并逐项自检，任一漂移只使 Keyprov capability NOT_READY。
 * </p>
 * <p>
 * 本清单声明 VAGW Adapter 实际锁定的云端协议版本，不伪装为设备 capability，也不增加
 * canonical Proto 字段。
 * </p>
 *
 * @param parProtoRelease        PAR-PROTO release（proto-keyprov artifact version）
 * @param descriptorSetSha256    proto-keyprov canonical DescriptorSet SHA-256（protoc descriptor set 规范字节）
 * @param packageName           固定 {@code security.keyprov.v1}
 * @param service               固定 {@code security.keyprov}
 * @param protocolVersion       固定 {@code keyprov-v1}
 * @param registryDigest        canonical PayloadType registry 文件 SHA-256
 * @param protocVersion         生成 canonical/generated classes 使用的 protoc 版本
 * @param protobufRuntimeVersion 运行时 protobuf-java 版本
 * @param goldenVectorSetDigest  release-set fixture bundle SHA-256（只消费摘要，不解释密码学内容）
 */
public record KeyprovProtocolLock(
        String parProtoRelease,
        String descriptorSetSha256,
        String packageName,
        String service,
        String protocolVersion,
        String registryDigest,
        String protocVersion,
        String protobufRuntimeVersion,
        String goldenVectorSetDigest) {

    public static final String FIXED_PACKAGE = "security.keyprov.v1";
    public static final String FIXED_SERVICE = "security.keyprov";
    public static final String FIXED_PROTOCOL_VERSION = "keyprov-v1";

    /** 锁版字段完整性检查（缺失/空值视为不可用，不做默认值补齐）。 */
    public boolean complete() {
        return notBlank(parProtoRelease)
                && notBlank(descriptorSetSha256)
                && notBlank(packageName)
                && notBlank(service)
                && notBlank(protocolVersion)
                && notBlank(registryDigest)
                && notBlank(protocVersion)
                && notBlank(protobufRuntimeVersion)
                && notBlank(goldenVectorSetDigest);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
