package net.hwyz.iov.cloud.edd.vagw.build;

import net.hwyz.iov.cloud.edd.vagw.infrastructure.ProtocolContractGuard;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * KeyprovProtocolLock 构建期生成器（EDD-VAGW-DSN-CR-017 §4）。
 * <p>
 * 由 {@code exec-maven-plugin} 在 {@code process-classes} 阶段调用，输出
 * {@code target/generated-resources/META-INF/vagw/keyprov-protocol-lock.json}。
 * 所有 hash 均来自实际依赖制品：
 * </p>
 * <ul>
 *   <li>{@code parProtoRelease}：proto-keyprov artifact 的 {@code pom.properties} 版本；</li>
 *   <li>{@code descriptorSetSha256}：生成类的 FileDescriptor 构造的 canonical DescriptorSet；</li>
 *   <li>{@code registryDigest}：canonical PayloadType registry 文件字节；</li>
 *   <li>{@code protobufRuntimeVersion}：protobuf-java artifact manifest；</li>
 *   <li>{@code goldenVectorSetDigest}：release-set vector bundle 字节。</li>
 * </ul>
 * <p>
 * 构建期同时扫描 keyprov 源码，禁止 {@code JsonFormat}/{@code TextFormat}/payload
 * {@code toString()}/真实签名验签实现；违规直接构建失败。输出不含时间戳、绝对路径、
 * 用户名或机器信息，保证逐字节可复现。
 * </p>
 * <p>
 * <b>本命令只写 target，不修改仓库内基线清单。</b> 更新
 * {@code src/main/resources/META-INF/vagw/keyprov-protocol-lock.json} 属于显式协议升级，
 * 必须经人工审核后提交，普通构建/CI 不得自动改写。
 * </p>
 */
public final class KeyprovProtocolLockManifestGenerator {

    public static final String OUTPUT_RESOURCE =
            "META-INF/vagw/keyprov-protocol-lock.json";
    private static final String DEFAULT_OUTPUT =
            "target/generated-resources/" + OUTPUT_RESOURCE;

    private static final String PROTO_KEYPROV_POM =
            "META-INF/maven/net.hwyz.iov.cloud.proto/proto-keyprov/pom.properties";
    private static final String REGISTRY_PATH =
            "META-INF/vagw/keyprov-payload-type-registry.txt";
    private static final String VECTOR_SET_PATH =
            "META-INF/vagw/keyprov-golden-vector-set.txt";
    private static final String SOURCE_ROOT =
            "src/main/java/net/hwyz/iov/cloud/edd/vagw";

    /** 禁止出现在 keyprov 生产源码中的敏感序列化/密码学实现符号（注释已剥离）。 */
    private static final List<String> FORBIDDEN_CODE_TOKENS = List.of(
            "JsonFormat",
            "TextFormat",
            "getPayload().toString()",
            "canonicalPayload.toString()",
            "downlinkPayload.toString()",
            "java.security.Signature",
            "Signature.getInstance",
            "proofInput",
            "P1363",
            "ECDSA",
            "low-S");

    private KeyprovProtocolLockManifestGenerator() {
    }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args.length > 0 ? args[0] : DEFAULT_OUTPUT);
        byte[] manifest = render();
        Files.createDirectories(output.toAbsolutePath().getParent());
        Files.write(output, manifest);
        System.out.println("[keyprov-lock] generated " + output + " ("
                + manifest.length + " bytes)");
    }

    /**
     * 生成 keyprov 协议锁版清单字节（确定顺序、LF、无时间戳/路径/用户信息）。
     * <p>运行时 readiness 与 CI 基线比对均复用本方法。</p>
     */
    public static byte[] render() throws IOException {
        String release = artifactVersion();
        assertNoProtoDependencies();

        String descriptorSetSha256 = sha256(ProtocolContractGuard.descriptorSetBytes(
                KeyProvisioningRequest.getDescriptor().getFile()));
        String registryDigest = sha256(readResource(REGISTRY_PATH));
        String vectorSetDigest = sha256(readResource(VECTOR_SET_PATH));
        String runtimeVersion = protobufRuntimeVersion();
        String protocVersion = System.getProperty("keyprov.protoc.version", "").trim();
        if (protocVersion.isEmpty()) {
            throw new IllegalStateException(
                    "keyprov.protoc.version build property is required for the protocol lock manifest");
        }
        boolean sensitiveGuardPassed = scanSensitiveGuard();

        StringBuilder json = new StringBuilder(512);
        json.append("{\n");
        appendField(json, "parProtoRelease", release, true);
        appendField(json, "descriptorSetSha256", descriptorSetSha256, true);
        appendField(json, "packageName", "security.keyprov.v1", true);
        appendField(json, "service", "security.keyprov", true);
        appendField(json, "protocolVersion", "keyprov-v1", true);
        appendField(json, "registryDigest", registryDigest, true);
        appendField(json, "protocVersion", protocVersion, true);
        appendField(json, "protobufRuntimeVersion", runtimeVersion, true);
        appendField(json, "goldenVectorSetDigest", vectorSetDigest, true);
        json.append("  \"sensitiveLoggingGuardPassed\": ").append(sensitiveGuardPassed).append('\n');
        json.append("}\n");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendField(StringBuilder json, String name, String value, boolean comma) {
        json.append("  \"").append(name).append("\": \"").append(escape(value)).append('"');
        if (comma) {
            json.append(',');
        }
        json.append('\n');
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void assertNoProtoDependencies() {
        int dependencyCount = KeyProvisioningRequest.getDescriptor().getFile()
                .getDependencies().size();
        if (dependencyCount != 0) {
            throw new IllegalStateException(
                    "proto-keyprov key_provisioning.proto must not depend on other proto files, found "
                            + dependencyCount + "; descriptor set hash would be incomplete");
        }
    }

    private static String artifactVersion() throws IOException {
        Properties props = new Properties();
        try (InputStream in = openResource(PROTO_KEYPROV_POM)) {
            props.load(in);
        }
        if (!"proto-keyprov".equals(props.getProperty("artifactId"))) {
            throw new IllegalStateException("proto-keyprov artifactId mismatch: "
                    + props.getProperty("artifactId"));
        }
        String version = props.getProperty("version");
        if (version == null || version.isBlank()) {
            throw new IllegalStateException("proto-keyprov version is missing from pom.properties");
        }
        return version;
    }

    /**
     * 读取 protobuf-java artifact manifest 的 Bundle-Version。
     * <p>委托 {@link net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.ProtobufRuntimeVersion}，
     * 与运行时自检使用同一探测逻辑。</p>
     */
    static String protobufRuntimeVersion() {
        return net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.ProtobufRuntimeVersion.detect();
    }

    /**
     * 扫描 keyprov 生产源码中的敏感序列化/密码学实现符号。
     *
     * @return true 表示通过
     * @throws IllegalStateException 发现违规时构建失败
     */
    static boolean scanSensitiveGuard() throws IOException {
        Path root = Path.of(SOURCE_ROOT);
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException("keyprov source root not found: " + root.toAbsolutePath());
        }
        List<String> offenders;
        try (Stream<Path> paths = Files.walk(root)) {
            offenders = paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(KeyprovProtocolLockManifestGenerator::isKeyprovSource)
                    .filter(p -> containsForbiddenToken(stripComments(read(p))))
                    .map(Path::toString)
                    .sorted()
                    .toList();
        }
        if (!offenders.isEmpty()) {
            throw new IllegalStateException(
                    "keyprov sensitive-logging/crypto guard FAILED; forbidden implementation symbols in: "
                            + offenders);
        }
        return true;
    }

    private static boolean isKeyprovSource(Path path) {
        String name = path.toString();
        return (name.contains("/keyprov/") || path.getFileName().toString().contains("Keyprov"))
                && !name.contains("/build/");
    }

    private static boolean containsForbiddenToken(String code) {
        for (String token : FORBIDDEN_CODE_TOKENS) {
            if (code.contains(token)) {
                return true;
            }
        }
        return false;
    }

    /** 移除行注释与块注释，避免仅注释提及密码学术语时误报。 */
    static String stripComments(String source) {
        StringBuilder out = new StringBuilder(source.length());
        int i = 0;
        int length = source.length();
        boolean inLineComment = false;
        boolean inBlockComment = false;
        while (i < length) {
            char current = source.charAt(i);
            if (inLineComment) {
                if (current == '\n') {
                    inLineComment = false;
                    out.append(current);
                }
                i++;
                continue;
            }
            if (inBlockComment) {
                if (current == '*' && i + 1 < length && source.charAt(i + 1) == '/') {
                    inBlockComment = false;
                    i += 2;
                } else {
                    if (current == '\n') {
                        out.append(current);
                    }
                    i++;
                }
                continue;
            }
            if (current == '/' && i + 1 < length && source.charAt(i + 1) == '/') {
                inLineComment = true;
                i += 2;
                continue;
            }
            if (current == '/' && i + 1 < length && source.charAt(i + 1) == '*') {
                inBlockComment = true;
                i += 2;
                continue;
            }
            out.append(current);
            i++;
        }
        return out.toString();
    }

    private static String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("unable to read " + path, e);
        }
    }

    private static byte[] readResource(String path) throws IOException {
        try (InputStream in = openResource(path)) {
            return in.readAllBytes();
        }
    }

    private static InputStream openResource(String path) throws IOException {
        InputStream in = KeyprovProtocolLockManifestGenerator.class
                .getClassLoader().getResourceAsStream(path);
        if (in == null) {
            throw new IOException("classpath resource not found: " + path);
        }
        return in;
    }

    public static String sha256(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
