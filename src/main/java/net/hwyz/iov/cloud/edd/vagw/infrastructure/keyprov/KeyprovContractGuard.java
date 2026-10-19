package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.ProtocolContractGuard;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.security.keyprov.v1.ImportProofAlgorithm;
import net.hwyz.iov.security.keyprov.v1.KeyEnvelopeFormat;
import net.hwyz.iov.security.keyprov.v1.KeyImportItemResult;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningAck;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import net.hwyz.iov.security.keyprov.v1.KeyPurpose;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * keyprov 契约与协议锁版启动门禁（EDD-VAGW-DSN-CR-011 §2/§3.3，由 EDD-VAGW-DSN-CR-017 §4/§9 修订）。
 * <p>
 * 校验：
 * </p>
 * <ul>
 *   <li>PAR-PROTO proto-keyprov artifact 坐标、package、protocol version 与六类 canonical registry；</li>
 *   <li>构建期生成的 {@link KeyprovProtocolLock}：release、DescriptorSet SHA-256、registry digest、
 *       protoc/protobuf runtime、golden vector set digest；</li>
 *   <li>生成类字段/枚举自检（CR-008 ACK 字段 10/11、payload 原字节直传）；</li>
 *   <li>edd-vmd-api 是否包含 CR-062 的 {@code protocolRelease}/{@code descriptorSetHash} 语义。</li>
 * </ul>
 * <p>
 * 任一失败只把 Keyprov capability 置为 NOT_READY（FOTA／远控／MQTT 连接不受影响），
 * 不抛全局异常终止进程。{@link KeyprovReadiness} 是进程内唯一、可查询的门控状态对象，
 * ACL、共享订阅、ingress 与 relay 必须共享同一实例。
 * </p>
 * <p>
 * keyprov 契约不兼容不影响 FOTA/远控，不污染 FOTA PayloadType registry。
 * </p>
 */
@Slf4j
@Component
public class KeyprovContractGuard {

    private static final String REGISTRY_PATH = "META-INF/vagw/keyprov-payload-type-registry.txt";
    private static final String LOCK_PATH = "META-INF/vagw/keyprov-protocol-lock.json";
    private static final String VECTOR_SET_PATH = "META-INF/vagw/keyprov-golden-vector-set.txt";
    private static final String POM_PROPS =
            "META-INF/maven/net.hwyz.iov.cloud.proto/proto-keyprov/pom.properties";
    private static final String EXPECTED_PACKAGE = KeyprovProtocolLock.FIXED_PACKAGE;
    private static final String EXPECTED_ARTIFACT = "proto-keyprov";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KeyprovProperties properties;
    private final KeyprovMetrics metrics;

    private final Set<String> registry = new LinkedHashSet<>();
    private volatile boolean compatible = false;
    private volatile String incompatibilityReason = "not verified";
    private volatile KeyprovProtocolLock lock;
    private volatile KeyprovReadiness readiness = notReady(KeyprovReason.KEYPROV_LOCK_MANIFEST_INVALID,
            "not verified");

    public KeyprovContractGuard() {
        this(new KeyprovProperties(), new KeyprovMetrics());
    }

    @Autowired
    public KeyprovContractGuard(KeyprovProperties properties, KeyprovMetrics metrics) {
        this.properties = properties;
        this.metrics = metrics;
    }

    @PostConstruct
    public void verify() {
        try {
            registry.clear();
            registry.addAll(readRegistry());

            KeyprovProtocolLockManifest manifest = readLockManifest();
            this.lock = manifest == null ? null : manifest.toLock();
            this.readiness = evaluate(readPomProperties(), readBytes(REGISTRY_PATH), manifest);
        } catch (Exception e) {
            this.lock = null;
            this.readiness = notReady(KeyprovReason.KEYPROV_LOCK_MANIFEST_INVALID,
                    "keyprov protocol lock unavailable: " + e.getMessage());
        }

        this.compatible = readiness.ready();
        this.incompatibilityReason = readiness.ready() ? null : readiness.notReadyDetail();

        metrics.readiness(readiness.ready(), readiness.ready() ? "ready" : readiness.reason().name());
        if (readiness.ready()) {
            log.info("Keyprov readiness: READY, release={}, descriptorSetSha256={}, registryDigest={}, "
                            + "protocVersion={}, protobufRuntimeVersion={}, goldenVectorSetDigest={}, eddVmdApiVersion={}",
                    lock.parProtoRelease(), lock.descriptorSetSha256(), lock.registryDigest(),
                    lock.protocVersion(), lock.protobufRuntimeVersion(), lock.goldenVectorSetDigest(),
                    eddVmdApiVersion());
        } else {
            log.error("Keyprov readiness: NOT_READY, reason={}, detail={}, conditions={}, eddVmdApiVersion={}",
                    readiness.reason(), readiness.notReadyDetail(), readiness.snapshot(), eddVmdApiVersion());
        }
    }

    public boolean compatible() {
        return compatible;
    }

    public String incompatibilityReason() {
        return incompatibilityReason;
    }

    public Set<String> registry() {
        return Set.copyOf(registry);
    }

    /** 当前协议锁版（未通过自检时为 {@code null}，不得用于构造 VMD 调用）。 */
    public KeyprovProtocolLock lock() {
        return lock;
    }

    /** 进程内唯一 Keyprov readiness 状态对象。 */
    public KeyprovReadiness readiness() {
        return readiness;
    }

    /**
     * 协议/API 只读路径是否可用（Status 对账门控）。
     * <p>release/descriptor/golden 漂移时仍允许设备发布 StatusRequest 收敛原 requestId。</p>
     */
    public boolean statusPathAvailable() {
        KeyprovReadiness current = readiness;
        return current != null && current.protocolApiCompatible();
    }

    /** 是否同时满足能力开关与完整 readiness（ACL 下行订阅与 Request/Ack 的唯一门控）。 */
    public boolean active(KeyprovProperties properties) {
        return properties.isEnabled() && readiness.ready();
    }

    // ------------------------------------------------------------------
    // readiness 评估
    // ------------------------------------------------------------------

    KeyprovReadiness evaluate(Properties artifactProps, byte[] registryBytes,
                              KeyprovProtocolLockManifest manifest) {
        KeyprovProtocolLock candidate = manifest == null ? null : manifest.toLock();
        boolean lockComplete = candidate != null && candidate.complete();
        if (!lockComplete) {
            return notReady(KeyprovReason.KEYPROV_LOCK_MANIFEST_INVALID,
                    "keyprov protocol lock manifest is missing or incomplete");
        }

        boolean release = lockComplete && releaseMatched(artifactProps, candidate);
        boolean descriptor = lockComplete && descriptorHashMatched(candidate);
        boolean packageRegistry = lockComplete && packageServiceProtocolRegistryMatched(candidate, registryBytes);
        boolean generated = lockComplete && generatedRuntimeSelfCheckPassed(candidate);
        boolean vmdApi = vmdCr062ApiCompatible();
        boolean ackAdapter = keyprovAckCr008AdapterReady();
        boolean passthrough = canonicalPayloadPassthroughReady();
        boolean sensitiveGuard = lockComplete
                && Boolean.TRUE.equals(manifest.sensitiveLoggingGuardPassed);
        boolean golden = lockComplete && goldenVectorSetDigestMatched(candidate);

        KeyprovReason reason = null;
        String detail = null;
        if (!release) {
            reason = KeyprovReason.KEYPROV_CONTRACT_RELEASE_MISMATCH;
            detail = "PAR-PROTO release or proto-keyprov artifact version does not match the protocol lock";
        } else if (!descriptor) {
            reason = KeyprovReason.KEYPROV_DESCRIPTOR_HASH_MISMATCH;
            detail = "proto-keyprov DescriptorSet SHA-256 does not match the protocol lock";
        } else if (!packageRegistry) {
            reason = KeyprovReason.KEYPROV_REGISTRY_MISMATCH;
            detail = "package/service/protocol/registry digest does not match the protocol lock";
        } else if (!generated) {
            reason = KeyprovReason.KEYPROV_RUNTIME_MISMATCH;
            detail = "protoc/protobuf runtime or generated field self-check does not match the protocol lock";
        } else if (!vmdApi) {
            reason = KeyprovReason.KEYPROV_VMD_API_INCOMPATIBLE;
            detail = "edd-vmd-api is missing CR-062 protocolRelease/descriptorSetHash semantics";
        } else if (!ackAdapter) {
            reason = KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED;
            detail = "CR-008 ACK adapter self-check failed (fields 10/11 or length enums unavailable)";
        } else if (!passthrough) {
            reason = KeyprovReason.KEYPROV_PROTOCOL_UNSUPPORTED;
            detail = "canonical payload passthrough self-check failed";
        } else if (!sensitiveGuard) {
            reason = KeyprovReason.KEYPROV_IMPORT_ATTESTATION_REJECTED;
            detail = "import proof sensitive logging guard was not recorded as passed by the build";
        } else if (!golden) {
            reason = KeyprovReason.KEYPROV_GOLDEN_VECTOR_SET_MISMATCH;
            detail = "golden vector set digest does not match the protocol lock";
        }

        boolean ready = reason == null;
        return new KeyprovReadiness(
                ready,
                reason,
                detail,
                release,
                descriptor,
                packageRegistry,
                generated,
                vmdApi,
                ackAdapter,
                passthrough,
                sensitiveGuard,
                golden);
    }

    private static boolean releaseMatched(Properties artifactProps, KeyprovProtocolLock candidate) {
        return EXPECTED_ARTIFACT.equals(artifactProps.getProperty("artifactId"))
                && candidate.parProtoRelease().equals(artifactProps.getProperty("version"));
    }

    private boolean descriptorHashMatched(KeyprovProtocolLock candidate) {
        try {
            String actual = sha256(ProtocolContractGuard.descriptorSetBytes(
                    KeyProvisioningRequest.getDescriptor().getFile()));
            return candidate.descriptorSetSha256().equalsIgnoreCase(actual);
        } catch (Exception e) {
            log.error("Keyprov lock: DescriptorSet hash computation failed: {}", e.getMessage());
            return false;
        }
    }

    private boolean packageServiceProtocolRegistryMatched(KeyprovProtocolLock candidate, byte[] registryBytes) {
        if (!EXPECTED_PACKAGE.equals(candidate.packageName())
                || !KeyprovProtocolLock.FIXED_SERVICE.equals(candidate.service())
                || !KeyprovProtocolLock.FIXED_PROTOCOL_VERSION.equals(candidate.protocolVersion())) {
            return false;
        }
        if (!properties.getService().equals(candidate.service())
                || !properties.getProtocolVersion().equals(candidate.protocolVersion())) {
            return false;
        }
        Set<String> registryTypes;
        try {
            registryTypes = parseRegistry(registryBytes);
        } catch (Exception e) {
            log.error("Keyprov lock: registry parse failed: {}", e.getMessage());
            return false;
        }
        for (KeyprovMessageType type : KeyprovMessageType.values()) {
            if (!registryTypes.contains(type.fullName())) {
                return false;
            }
            if (!EXPECTED_PACKAGE.equals(packageOf(type.fullName()))) {
                return false;
            }
        }
        for (String name : registryTypes) {
            if (KeyprovMessageType.fromFullName(name).isEmpty()) {
                return false;
            }
        }
        return candidate.registryDigest().equalsIgnoreCase(sha256(registryBytes));
    }

    /**
     * 生成类与 runtime 自检：六类 parser、CR-008 请求字段、protoc/runtime 版本。
     */
    private boolean generatedRuntimeSelfCheckPassed(KeyprovProtocolLock candidate) {
        if (candidate.protocVersion() == null || candidate.protocVersion().isBlank()) {
            return false;
        }
        if (!candidate.protobufRuntimeVersion().equals(ProtobufRuntimeVersion.detect())) {
            return false;
        }
        try {
            for (KeyprovMessageType type : KeyprovMessageType.values()) {
                if (type.parser() == null) {
                    return false;
                }
                type.parser().parseFrom(new byte[0]);
            }
            Descriptors.Descriptor request = KeyProvisioningRequest.getDescriptor();
            if (!isRepeatedEnum(request, 4) || !isRepeatedEnum(request, 7) || !isRepeatedEnum(request, 11)) {
                return false;
            }
            Descriptors.Descriptor ack = KeyProvisioningAck.getDescriptor();
            Descriptors.FieldDescriptor items = ack.findFieldByNumber(2);
            return items != null && items.isRepeated()
                    && items.getType() == Descriptors.FieldDescriptor.Type.MESSAGE;
        } catch (Exception e) {
            log.error("Keyprov lock: generated runtime self-check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * CR-008 ACK Import Attestation 字段/枚举可用性（只检查 schema，不实现验签）。
     */
    private static boolean keyprovAckCr008AdapterReady() {
        try {
            Descriptors.Descriptor item = KeyImportItemResult.getDescriptor();
            Descriptors.FieldDescriptor importProof = item.findFieldByNumber(5);
            Descriptors.FieldDescriptor envelopeDigest = item.findFieldByNumber(7);
            Descriptors.FieldDescriptor kcv = item.findFieldByNumber(8);
            Descriptors.FieldDescriptor format = item.findFieldByNumber(9);
            Descriptors.FieldDescriptor algorithm = item.findFieldByNumber(10);
            Descriptors.FieldDescriptor certDigest = item.findFieldByNumber(11);
            if (importProof == null || importProof.getType() != Descriptors.FieldDescriptor.Type.BYTES
                    || envelopeDigest == null || envelopeDigest.getType() != Descriptors.FieldDescriptor.Type.BYTES
                    || kcv == null || kcv.getType() != Descriptors.FieldDescriptor.Type.BYTES
                    || format == null || format.getType() != Descriptors.FieldDescriptor.Type.ENUM
                    || algorithm == null || algorithm.getType() != Descriptors.FieldDescriptor.Type.ENUM
                    || certDigest == null || certDigest.getType() != Descriptors.FieldDescriptor.Type.BYTES) {
                return false;
            }
            if (ImportProofAlgorithm.forNumber(0) == null || ImportProofAlgorithm.forNumber(1) == null) {
                return false;
            }
            return KeyEnvelopeFormat.forNumber(0) != null && KeyEnvelopeFormat.forNumber(1) != null;
        } catch (Exception e) {
            log.error("Keyprov lock: CR-008 ACK adapter self-check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * canonical payload 原字节直传路径自检：Envelope.payload 为 bytes，六类消息方向映射完整。
     */
    private static boolean canonicalPayloadPassthroughReady() {
        try {
            Descriptors.FieldDescriptor payload = Envelope.VehicleMessageEnvelope.getDescriptor()
                    .findFieldByName("payload");
            if (payload == null || payload.getType() != Descriptors.FieldDescriptor.Type.BYTES) {
                return false;
            }
            for (KeyprovMessageType type : KeyprovMessageType.values()) {
                if (type.parser() == null) {
                    return false;
                }
                if (type.isUpstream() && type.expectedDownlink() == null) {
                    return false;
                }
            }
            return KeyPurpose.KEY_PURPOSE_UPLINK_AEAD.getNumber() == 1
                    && KeyPurpose.KEY_PURPOSE_DOWNLINK_AEAD.getNumber() == 2;
        } catch (Exception e) {
            log.error("Keyprov lock: canonical payload passthrough self-check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * edd-vmd-api 是否包含 CR-062 语义（反射检查，容忍运行时 classpath 混入旧 artifact）。
     */
    private static boolean vmdCr062ApiCompatible() {
        try {
            Class<?> request = Class.forName(
                    "net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningRequest");
            request.getMethod("getProtocolRelease");
            request.getMethod("getDescriptorSetHash");
            Class<?> builder = Class.forName(request.getName()
                    + "$VmdKeyProvisioningRequestBuilder");
            builder.getMethod("protocolRelease", String.class);
            builder.getMethod("descriptorSetHash", String.class);
            Class<?> service = Class.forName(
                    "net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService");
            service.getMethod("request", request);
            return true;
        } catch (Exception e) {
            log.error("Keyprov lock: edd-vmd-api CR-062 compatibility check failed: {}", e.getMessage());
            return false;
        }
    }

    private boolean goldenVectorSetDigestMatched(KeyprovProtocolLock candidate) {
        try {
            return candidate.goldenVectorSetDigest().equalsIgnoreCase(sha256(readBytes(VECTOR_SET_PATH)));
        } catch (Exception e) {
            log.error("Keyprov lock: golden vector set digest computation failed: {}", e.getMessage());
            return false;
        }
    }

    private static boolean isRepeatedEnum(Descriptors.Descriptor descriptor, int fieldNumber) {
        Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(fieldNumber);
        return field != null && field.isRepeated()
                && field.getType() == Descriptors.FieldDescriptor.Type.ENUM;
    }

    private static KeyprovReadiness notReady(KeyprovReason reason, String detail) {
        return new KeyprovReadiness(false, reason, detail,
                false, false, false, false, false, false, false, false, false);
    }

    // ------------------------------------------------------------------
    // 资源读取
    // ------------------------------------------------------------------

    private static String packageOf(String fullName) {
        int idx = fullName.lastIndexOf('.');
        return idx < 0 ? "" : fullName.substring(0, idx);
    }

    private static Set<String> readRegistry() throws IOException {
        return parseRegistry(readBytes(REGISTRY_PATH));
    }

    private static Set<String> parseRegistry(byte[] registryBytes) {
        List<String> lines = new String(registryBytes, StandardCharsets.UTF_8).lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty())
                .toList();
        if (lines.isEmpty()) {
            throw new IllegalStateException("keyprov payload-type registry is empty");
        }
        return new LinkedHashSet<>(lines);
    }

    private static byte[] readBytes(String path) throws IOException {
        try (InputStream in = KeyprovContractGuard.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new IOException("classpath resource not found: " + path);
            }
            return in.readAllBytes();
        }
    }

    private static KeyprovProtocolLockManifest readLockManifest() throws IOException {
        byte[] bytes = readBytes(LOCK_PATH);
        return OBJECT_MAPPER.readValue(bytes, KeyprovProtocolLockManifest.class);
    }

    private static Properties readPomProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream in = KeyprovContractGuard.class.getClassLoader().getResourceAsStream(POM_PROPS)) {
            if (in == null) {
                throw new IOException("classpath resource not found: " + POM_PROPS);
            }
            props.load(in);
        }
        return props;
    }

    /** edd-vmd-api artifact version（仅用于 readiness 结构化日志，不做版本字符串硬编码匹配）。 */
    private static String eddVmdApiVersion() {
        Properties props = new Properties();
        try (InputStream in = KeyprovContractGuard.class.getClassLoader()
                .getResourceAsStream("META-INF/maven/net.hwyz.iov.cloud.edd/edd-vmd-api/pom.properties")) {
            if (in == null) {
                return "unknown";
            }
            props.load(in);
            return props.getProperty("version", "unknown");
        } catch (IOException e) {
            return "unknown";
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            return java.util.HexFormat.of().formatHex(md.digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
