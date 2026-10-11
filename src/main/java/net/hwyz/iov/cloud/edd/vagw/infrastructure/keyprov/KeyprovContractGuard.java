package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovMessageType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * keyprov 契约启动门禁（EDD-VAGW-DSN-CR-011 §2/§3.3）。
 * <p>
 * 校验 PAR-PROTO proto-keyprov artifact 坐标、package 固定 {@code security.keyprov.v1}、
 * 六类 canonical 消息描述符与 registry 一致。与 FOTA 的 {@code ProtocolContractGuard} 相互独立：
 * keyprov 契约不兼容只做 fail-closed，不影响 FOTA/远控；不污染 FOTA PayloadType registry。
 * </p>
 * <p>
 * 注意：这里不抛异常终止进程。当 {@code keyprov.enabled=false} 或契约不兼容时，
 * VAGW 只拒绝 keyprov capability，其他 service 不受影响（需求 US-011）。
 * </p>
 */
@Slf4j
@Component
public class KeyprovContractGuard {

    private static final String REGISTRY_PATH = "META-INF/vagw/keyprov-payload-type-registry.txt";
    private static final String POM_PROPS =
            "META-INF/maven/net.hwyz.iov.cloud.proto/proto-keyprov/pom.properties";
    private static final String EXPECTED_PACKAGE = "security.keyprov.v1";
    private static final String EXPECTED_ARTIFACT = "proto-keyprov";

    private final Set<String> registry = new LinkedHashSet<>();
    private volatile boolean compatible = false;
    private volatile String incompatibilityReason = "not verified";

    public KeyprovContractGuard() {
    }

    @PostConstruct
    public void verify() {
        try {
            registry.clear();
            registry.addAll(readRegistry());

            // 1. artifact 坐标
            Properties props = readPomProperties();
            String artifactId = props.getProperty("artifactId");
            if (!EXPECTED_ARTIFACT.equals(artifactId)) {
                throw new IllegalStateException("proto-keyprov artifactId mismatch: " + artifactId);
            }
            String version = props.getProperty("version");

            // 2. registry 必须精确覆盖六类 canonical 消息且无未知项
            for (KeyprovMessageType type : KeyprovMessageType.values()) {
                if (!registry.contains(type.fullName())) {
                    throw new IllegalStateException("keyprov registry missing canonical type: " + type.fullName());
                }
                if (!EXPECTED_PACKAGE.equals(packageOf(type.fullName()))) {
                    throw new IllegalStateException("keyprov package drift: " + type.fullName());
                }
            }
            for (String name : registry) {
                if (KeyprovMessageType.fromFullName(name).isEmpty()) {
                    throw new IllegalStateException("keyprov registry contains unknown type: " + name);
                }
            }

            this.compatible = true;
            this.incompatibilityReason = null;
            log.info("Keyprov contract guard: PASSED (proto-keyprov={}, package={}, types={})",
                    version, EXPECTED_PACKAGE, registry.size());
        } catch (Exception e) {
            this.compatible = false;
            this.incompatibilityReason = e.getMessage();
            log.error("Keyprov contract guard: FAILED (fail-closed). keyprov capability will not be granted: {}",
                    e.getMessage());
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

    private static String packageOf(String fullName) {
        int idx = fullName.lastIndexOf('.');
        return idx < 0 ? "" : fullName.substring(0, idx);
    }

    private static Set<String> readRegistry() throws IOException {
        List<String> lines = new String(
                new ClassPathResource(REGISTRY_PATH).getInputStream().readAllBytes(),
                StandardCharsets.UTF_8).lines()
                .map(String::trim)
                .filter(l -> !l.isEmpty())
                .toList();
        if (lines.isEmpty()) {
            throw new IllegalStateException("keyprov payload-type registry is empty");
        }
        return new LinkedHashSet<>(lines);
    }

    private static Properties readPomProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream in = KeyprovContractGuard.class.getClassLoader()
                .getResourceAsStream(POM_PROPS)) {
            if (in == null) {
                throw new IOException("classpath resource not found: " + POM_PROPS);
            }
            props.load(in);
        }
        return props;
    }

    /** 是否同时满足能力开关与契约兼容（ACL 与 ingress 的唯一门控入口）。 */
    public boolean active(KeyprovProperties properties) {
        return properties.isEnabled() && compatible;
    }
}
