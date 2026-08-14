package net.hwyz.iov.cloud.edd.vagw.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import vehicle.common.v1.Envelope;
import vagw.v1.Delivery;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 协议契约启动门禁（EDD-VAGW-DSN-CR-006 §5/§8.3，fail-closed）。
 * <p>
 * 启动时校验：common artifact 坐标与 DescriptorSet、PayloadType registry sha、protocol major、
 * proto-vagw 坐标与 proto/DescriptorSet sha、生成 package／字段号／枚举值。任一漂移即启动失败，
 * 不从 VEH-PROTO、本地源码或缓存副本兜底。VAGW 不手写第二份业务消息目录。
 * </p>
 * <p>
 * 校验来源：classpath 内嵌的 {@code META-INF/vagw/proto-manifest.json}（PAR-PROTO 发布值 verbatim
 * 记录）与 {@code META-INF/vagw/payload-type-registry.txt}；DescriptorSet 由生成类的
 * FileDescriptor 现场构造（注入 json_name 后与原 proto 序列化一致）。
 * </p>
 */
@Slf4j
@Component
public class ProtocolContractGuard {

    private static final String MANIFEST_PATH = "META-INF/vagw/proto-manifest.json";
    private static final String REGISTRY_PATH = "META-INF/vagw/payload-type-registry.txt";
    private static final String DELIVERY_PROTO_PATH = "vagw/v1/delivery.proto";
    private static final String COMMON_POM_PROPS =
            "META-INF/maven/net.hwyz.iov.cloud.proto/proto-vehicle-common/pom.properties";
    private static final String VAGW_POM_PROPS =
            "META-INF/maven/net.hwyz.iov.cloud.proto/proto-vagw/pom.properties";

    private final ObjectMapper objectMapper;
    private final int configuredProtocolMajor;

    private Manifest manifest;
    private Set<String> payloadTypes = Set.of();

    public ProtocolContractGuard(ObjectMapper objectMapper,
                                 @Value("${vagw.fota.protocol-major:1}") int configuredProtocolMajor) {
        this.objectMapper = objectMapper;
        this.configuredProtocolMajor = configuredProtocolMajor;
    }

    @PostConstruct
    public void verify() {
        try {
            manifest = objectMapper.readValue(
                    new ClassPathResource(MANIFEST_PATH).getInputStream(), Manifest.class);
            payloadTypes = readRegistry();

            log.info("Protocol contract guard: parProtoRelease={}, protocolMajor={}",
                    manifest.parProtoRelease, manifest.protocolMajor);

            // 1. 配置 major 与 Manifest 一致
            assertValue("protocol major", configuredProtocolMajor, manifest.protocolMajor);

            // 2. common artifact 坐标
            Properties commonProps = readPomProperties(COMMON_POM_PROPS);
            assertValue("common artifactId", commonProps.getProperty("artifactId"), "proto-vehicle-common");
            assertValue("common version", commonProps.getProperty("version"), versionOf(manifest.common.coordinates));

            // 3. common DescriptorSet sha（现场构造，含 json_name）
            assertSha("common descriptor", descriptorSetBytes(Envelope.getDescriptor()),
                    manifest.common.descriptorSha256);

            // 4. PayloadType registry sha
            assertSha("payload type registry",
                    new ClassPathResource(REGISTRY_PATH).getInputStream().readAllBytes(),
                    manifest.common.payloadTypeRegistrySha256);

            // 5. proto-vagw 坐标
            Properties vagwProps = readPomProperties(VAGW_POM_PROPS);
            assertValue("proto-vagw artifactId", vagwProps.getProperty("artifactId"), "proto-vagw");
            assertValue("proto-vagw version", vagwProps.getProperty("version"), versionOf(manifest.protoVagw.coordinates));

            // 6. proto-vagw proto 源 sha（jar 内打包的 delivery.proto）
            assertSha("proto-vagw proto", readResource(DELIVERY_PROTO_PATH), manifest.protoVagw.protoSha256);

            // 7. proto-vagw DescriptorSet sha
            assertSha("proto-vagw descriptor", descriptorSetBytes(Delivery.getDescriptor()),
                    manifest.protoVagw.descriptorSha256);

            // 8. 生成类型字段号/枚举值快照校验（防止生成 package/字段号/枚举漂移）
            assertDeliveryContract();

            log.info("Protocol contract guard: ALL CHECKS PASSED (common + proto-vagw)");
        } catch (Exception e) {
            throw new IllegalStateException("Protocol contract guard FAILED (fail-closed): " + e.getMessage(), e);
        }
    }

    public Set<String> payloadTypes() {
        return payloadTypes;
    }

    public int protocolMajor() {
        return manifest.protocolMajor;
    }

    public String parProtoRelease() {
        return manifest.parProtoRelease;
    }

    // ------------------------------------------------------------------
    // 校验辅助
    // ------------------------------------------------------------------

    /** 断言实际 DescriptorSet bytes 与期望 sha 一致（fail-closed）。 */
    public static void assertSha(String label, byte[] actualBytes, String expectedSha) {
        String actual = sha256(actualBytes);
        if (!expectedSha.equalsIgnoreCase(actual)) {
            throw new IllegalStateException(
                    "descriptor/proto drift for [" + label + "]: expected sha=" + expectedSha + " actual=" + actual);
        }
    }

    private static void assertValue(String label, Object actual, Object expected) {
        if (expected == null || !expected.toString().equals(String.valueOf(actual))) {
            throw new IllegalStateException(
                    "mismatch for [" + label + "]: expected=" + expected + " actual=" + actual);
        }
    }

    private static void assertDeliveryContract() {
        if (Delivery.GatewayDeliveryStatus.getDescriptor().getFields().size() < 9) {
            throw new IllegalStateException("proto-vagw GatewayDeliveryStatus field drift (expected >= 9 fields)");
        }
        if (Delivery.Outcome.getDescriptor().getValues().size() != 4
                || Delivery.Outcome.OUTCOME_UNSPECIFIED.getNumber() != 0
                || Delivery.Outcome.OUTCOME_ACCEPTED.getNumber() != 1
                || Delivery.Outcome.OUTCOME_REJECTED.getNumber() != 2
                || Delivery.Outcome.OUTCOME_UNKNOWN.getNumber() != 3) {
            throw new IllegalStateException("proto-vagw Outcome enum drift (expected OUTCOME_UNSPECIFIED=0..OUTCOME_UNKNOWN=3)");
        }
    }

    /**
     * 由生成类 FileDescriptor 构造 DescriptorSet bytes（注入 json_name，与 protoc
     * --descriptor_set_out 序列化一致，供 sha 比对）。
     */
    public static byte[] descriptorSetBytes(Descriptors.FileDescriptor fd) {
        DescriptorProtos.FileDescriptorProto fileProto =
                injectJsonName(fd, fd.toProto());
        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(fileProto).build().toByteArray();
    }

    private static DescriptorProtos.FileDescriptorProto injectJsonName(
            Descriptors.FileDescriptor fd, DescriptorProtos.FileDescriptorProto proto) {
        DescriptorProtos.FileDescriptorProto.Builder b = proto.toBuilder();
        for (int i = 0; i < proto.getMessageTypeCount(); i++) {
            b.setMessageType(i, injectJsonNameMsg(fd.getMessageTypes().get(i), proto.getMessageType(i)));
        }
        return b.build();
    }

    private static DescriptorProtos.DescriptorProto injectJsonNameMsg(
            Descriptors.Descriptor d, DescriptorProtos.DescriptorProto proto) {
        DescriptorProtos.DescriptorProto.Builder b = proto.toBuilder();
        for (int i = 0; i < proto.getFieldCount(); i++) {
            b.setField(i, proto.getField(i).toBuilder()
                    .setJsonName(d.getFields().get(i).getJsonName()).build());
        }
        for (int i = 0; i < proto.getNestedTypeCount(); i++) {
            b.setNestedType(i, injectJsonNameMsg(d.getNestedTypes().get(i), proto.getNestedType(i)));
        }
        return b.build();
    }

    public static String sha256(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return java.util.HexFormat.of().formatHex(md.digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private Set<String> readRegistry() throws IOException {
        List<String> lines = new String(
                new ClassPathResource(REGISTRY_PATH).getInputStream().readAllBytes(),
                StandardCharsets.UTF_8).lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty())
                .toList();
        return new HashSet<>(lines);
    }

    private Properties readPomProperties(String path) throws IOException {
        Properties props = new Properties();
        try (InputStream in = readResourceStream(path)) {
            props.load(in);
        }
        return props;
    }

    private byte[] readResource(String path) throws IOException {
        try (InputStream in = readResourceStream(path)) {
            return in.readAllBytes();
        }
    }

    private InputStream readResourceStream(String path) throws IOException {
        InputStream in = getClass().getClassLoader().getResourceAsStream(path);
        if (in == null) {
            throw new IOException("classpath resource not found: " + path);
        }
        return in;
    }

    private static String versionOf(String coordinates) {
        return coordinates.substring(coordinates.lastIndexOf(':') + 1);
    }

    // ------------------------------------------------------------------
    // Manifest 结构（PAR-PROTO 发布值 verbatim 记录）
    // ------------------------------------------------------------------

    public static class Manifest {
        public String parProtoRelease;
        public int protocolMajor;
        public CommonManifest common;
        public ProtoVagwManifest protoVagw;
    }

    public static class CommonManifest {
        public String coordinates;
        public String descriptorSha256;
        public String payloadTypeRegistrySha256;
    }

    public static class ProtoVagwManifest {
        public String coordinates;
        public String protoSha256;
        public String descriptorSha256;
    }
}
