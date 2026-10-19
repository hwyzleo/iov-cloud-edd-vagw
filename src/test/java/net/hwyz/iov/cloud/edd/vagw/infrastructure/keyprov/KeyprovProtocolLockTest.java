package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hwyz.iov.cloud.edd.vagw.build.KeyprovProtocolLockManifestGenerator;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KeyprovProtocolLock 构建产物/readiness 测试（EDD-VAGW-DSN-CR-017 §4/§9/§12）。
 * <p>
 * 仓库内清单是审核基线；CI 重新生成后必须逐字节一致。所有 hash 均从实际依赖制品的
 * DescriptorSet/registry/runtime 计算，而不是仅对配置字符串取 hash。
 * </p>
 */
class KeyprovProtocolLockTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void checkedInBaseline_shouldEqualBuildGeneratedManifestByteForByte() throws Exception {
        byte[] generated = KeyprovProtocolLockManifestGenerator.render();
        java.nio.file.Path baselinePath = java.nio.file.Path.of(
                "src", "main", "resources", "META-INF", "vagw", "keyprov-protocol-lock.json");
        assertTrue(java.nio.file.Files.isRegularFile(baselinePath),
                "仓库内必须存在可 diff 的 KeyprovProtocolLock 审核基线");
        byte[] baseline = java.nio.file.Files.readAllBytes(baselinePath);

        assertEquals(new String(baseline, StandardCharsets.UTF_8),
                new String(generated, StandardCharsets.UTF_8),
                "仓库内 KeyprovProtocolLock 基线与当前 artifact 漂移；更新基线属于显式协议升级");
    }

    @Test
    void generation_shouldBeDeterministicAndFreeOfMachineMetadata() throws Exception {
        byte[] first = KeyprovProtocolLockManifestGenerator.render();
        byte[] second = KeyprovProtocolLockManifestGenerator.render();
        assertArrayEquals(first, second, "清单生成必须逐字节可复现");

        String json = new String(first, StandardCharsets.UTF_8);
        assertFalse(json.contains("/Users/"), "不得包含绝对路径");
        assertFalse(json.contains("user.dir"), "不得包含构建工作目录");
        assertFalse(json.contains("timestamp"), "不得包含时间戳");
        assertFalse(json.contains("golden=true"), "不得存在可编辑 golden 开关");
    }

    @Test
    void manifest_shouldCarryAllGovernedLockFieldsDerivedFromArtifacts() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        KeyprovProtocolLock lock = manifest.toLock();

        assertTrue(lock.complete());
        assertEquals(KeyprovProtocolLock.FIXED_PACKAGE, lock.packageName());
        assertEquals(KeyprovProtocolLock.FIXED_SERVICE, lock.service());
        assertEquals(KeyprovProtocolLock.FIXED_PROTOCOL_VERSION, lock.protocolVersion());
        assertEquals("proto-keyprov", artifactProps().getProperty("artifactId"));
        assertEquals(artifactProps().getProperty("version"), lock.parProtoRelease());
        assertEquals(ProtobufRuntimeVersion.detect(), lock.protobufRuntimeVersion());
        assertNotNull(lock.protocVersion());
        assertFalse(lock.protocVersion().isBlank());

        String descriptorHash = KeyprovProtocolLockManifestGenerator.sha256(
                net.hwyz.iov.cloud.edd.vagw.infrastructure.ProtocolContractGuard.descriptorSetBytes(
                        net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest.getDescriptor().getFile()));
        assertEquals(descriptorHash, lock.descriptorSetSha256(),
                "DescriptorSet hash 必须来自实际生成类的 FileDescriptor，而不是配置字符串");
        assertEquals(KeyprovProtocolLockManifestGenerator.sha256(
                        readBytes("META-INF/vagw/keyprov-payload-type-registry.txt")),
                lock.registryDigest());
        assertEquals(KeyprovProtocolLockManifestGenerator.sha256(
                        readBytes("META-INF/vagw/keyprov-golden-vector-set.txt")),
                lock.goldenVectorSetDigest());
        assertTrue(Boolean.TRUE.equals(manifest.sensitiveLoggingGuardPassed),
                "构建期敏感日志扫描必须记录为通过");
    }

    @Test
    void readiness_shouldBeReadyWhenAllLockConditionsMatch() throws Exception {
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), readManifest());

        assertTrue(readiness.ready(), () -> "expected READY, reason=" + readiness.reason()
                + ", detail=" + readiness.notReadyDetail() + ", conditions=" + readiness.snapshot());
        assertNull(readiness.reason());
        assertTrue(readiness.parProtoCr008ReleaseMatched());
        assertTrue(readiness.descriptorSetHashMatched());
        assertTrue(readiness.packageServiceProtocolRegistryMatched());
        assertTrue(readiness.generatedRuntimeSelfCheckPassed());
        assertTrue(readiness.vmdCr062ApiCompatible());
        assertTrue(readiness.keyprovAckCr008AdapterReady());
        assertTrue(readiness.canonicalPayloadPassthroughReady());
        assertTrue(readiness.importProofSensitiveLoggingGuardReady());
        assertTrue(readiness.goldenVectorSetDigestMatched());
    }

    @Test
    void readiness_releaseDrift_shouldOnlyDisableKeyprov() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.parProtoRelease = "0.0.0-other";
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), manifest);

        assertFalse(readiness.ready());
        assertEquals(KeyprovReason.KEYPROV_CONTRACT_RELEASE_MISMATCH, readiness.reason());
        assertFalse(readiness.allows(KeyprovMessageType.KEY_PROVISIONING_REQUEST));
        assertFalse(readiness.allows(KeyprovMessageType.KEY_PROVISIONING_ACK));
        assertTrue(readiness.allows(KeyprovMessageType.KEY_PROVISIONING_STATUS_REQUEST),
                "release 漂移时 Status 只读对账仍可用");
    }

    @Test
    void readiness_descriptorDrift_shouldReportDescriptorHashMismatch() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.descriptorSetSha256 = zeroHash();
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), manifest);

        assertEquals(KeyprovReason.KEYPROV_DESCRIPTOR_HASH_MISMATCH, readiness.reason());
        assertFalse(readiness.descriptorSetHashMatched());
    }

    @Test
    void readiness_registryOrPackageDrift_shouldReportRegistryMismatch() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.registryDigest = zeroHash();
        assertEquals(KeyprovReason.KEYPROV_REGISTRY_MISMATCH,
                guard().evaluate(artifactProps(), readRegistryBytes(), manifest).reason());

        manifest = readManifest();
        manifest.packageName = "security.keyprov.v2";
        assertEquals(KeyprovReason.KEYPROV_REGISTRY_MISMATCH,
                guard().evaluate(artifactProps(), readRegistryBytes(), manifest).reason());

        manifest = readManifest();
        manifest.service = "security.keyprov.other";
        assertEquals(KeyprovReason.KEYPROV_REGISTRY_MISMATCH,
                guard().evaluate(artifactProps(), readRegistryBytes(), manifest).reason());
    }

    @Test
    void readiness_runtimeDrift_shouldReportRuntimeMismatch() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.protobufRuntimeVersion = "0.0.0";
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), manifest);

        assertEquals(KeyprovReason.KEYPROV_RUNTIME_MISMATCH, readiness.reason());
        assertFalse(readiness.generatedRuntimeSelfCheckPassed());
    }

    @Test
    void readiness_goldenVectorDrift_shouldReportGoldenMismatch() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.goldenVectorSetDigest = zeroHash();
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), manifest);

        assertEquals(KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH, readiness.reason());
        assertFalse(readiness.goldenVectorSetDigestMatched());
        assertFalse(readiness.allows(KeyprovMessageType.KEY_PROVISIONING_REQUEST));
        assertTrue(readiness.allows(KeyprovMessageType.KEY_PROVISIONING_STATUS_REQUEST));
    }

    @Test
    void readiness_sensitiveGuardFailure_shouldFailClosed() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.sensitiveLoggingGuardPassed = false;
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), manifest);

        assertEquals(KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED, readiness.reason());
        assertFalse(readiness.importProofSensitiveLoggingGuardReady());
    }

    @Test
    void readiness_missingLockFields_shouldBeManifestInvalid() throws Exception {
        KeyprovProtocolLockManifest manifest = readManifest();
        manifest.descriptorSetSha256 = null;
        KeyprovReadiness readiness = guard().evaluate(artifactProps(), readRegistryBytes(), manifest);

        assertEquals(KeyprovReason.KEYPROV_LOCK_MANIFEST_INVALID, readiness.reason(),
                "不完整清单必须 fail-closed 为 LOCK_MANIFEST_INVALID");
        assertFalse(readiness.ready());
    }

    private static KeyprovContractGuard guard() {
        return new KeyprovContractGuard(new KeyprovProperties(), new KeyprovMetrics());
    }

    private static KeyprovProtocolLockManifest readManifest() throws Exception {
        return OBJECT_MAPPER.readValue(readBytes("META-INF/vagw/keyprov-protocol-lock.json"),
                KeyprovProtocolLockManifest.class);
    }

    private static Properties artifactProps() throws Exception {
        Properties props = new Properties();
        try (InputStream in = KeyprovProtocolLockTest.class.getClassLoader()
                .getResourceAsStream("META-INF/maven/net.hwyz.iov.cloud.proto/proto-keyprov/pom.properties")) {
            assertNotNull(in, "proto-keyprov pom.properties must be on classpath");
            props.load(in);
        }
        return props;
    }

    private static byte[] readRegistryBytes() throws Exception {
        return readBytes("META-INF/vagw/keyprov-payload-type-registry.txt");
    }

    private static byte[] readBytes(String path) throws Exception {
        try (InputStream in = KeyprovProtocolLockTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in, "classpath resource missing: " + path);
            return in.readAllBytes();
        }
    }

    private static String zeroHash() {
        return "0".repeat(64);
    }
}
