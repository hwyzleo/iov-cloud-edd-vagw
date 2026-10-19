package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

/**
 * {@code keyprov-protocol-lock.json} 的只读映射（EDD-VAGW-DSN-CR-017 §4）。
 * <p>
 * 字段由构建期 {@code KeyprovProtocolLockManifestGenerator} 生成；除
 * {@code sensitiveLoggingGuardPassed}（构建期源码扫描结果）外均进入
 * {@link KeyprovProtocolLock}。不存在任何可编辑的 {@code golden=true} 开关。
 * </p>
 */
public class KeyprovProtocolLockManifest {

    public String parProtoRelease;
    public String descriptorSetSha256;
    public String packageName;
    public String service;
    public String protocolVersion;
    public String registryDigest;
    public String protocVersion;
    public String protobufRuntimeVersion;
    public String goldenVectorSetDigest;
    public Boolean sensitiveLoggingGuardPassed;

    public KeyprovProtocolLock toLock() {
        return new KeyprovProtocolLock(
                parProtoRelease,
                descriptorSetSha256,
                packageName,
                service,
                protocolVersion,
                registryDigest,
                protocVersion,
                protobufRuntimeVersion,
                goldenVectorSetDigest);
    }
}
