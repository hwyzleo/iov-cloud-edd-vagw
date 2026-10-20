package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * protobuf-java runtime 版本探测测试（EDD-VAGW-DSN-CR-017 §4 实现缺陷修复）。
 * <p>
 * 覆盖两类 CodeSource location 形态：普通文件 jar（{@code file:}）与 Spring Boot fat jar
 * 嵌套 jar（{@code jar:file:...!/BOOT-INF/lib/...}）。嵌套 jar 场景用标准 JVM 手工构造
 * outer.jar 内嵌 inner.jar 模拟，不依赖 Boot loader，保证 CI 与生产行为一致。
 * </p>
 */
class ProtobufRuntimeVersionTest {

    @TempDir
    Path tempDir;

    @Test
    void detect_plainJarClasspath_shouldReturnRuntimeVersion() {
        String version = ProtobufRuntimeVersion.detect();
        assertNotNull(version, "protobuf-java 必须能从 classpath 解析出 runtime 版本");
        assertFalse(version.isBlank());
        assertTrue(version.matches("\\d+(\\.\\d+)+.*"),
                "protobuf-java runtime 版本必须是版本号形态，got: " + version);
    }

    @Test
    void resolve_plainFileJar_shouldReadBundleVersion() throws Exception {
        Path jar = buildJar(tempDir.resolve("plain.jar"), "Bundle-Version", "9.9.9");
        CodeSource codeSource = new CodeSource(jar.toUri().toURL(), (java.security.cert.Certificate[]) null);

        assertEquals("9.9.9", ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
    }

    @Test
    void resolve_nestedJar_shouldReadInnerBundleVersion() throws Exception {
        Path inner = buildJar(tempDir.resolve("inner.jar"), "Bundle-Version", "8.8.8");
        Path outer = buildOuterJar(tempDir.resolve("app.jar"), "BOOT-INF/lib/inner.jar", inner);
        CodeSource codeSource = new CodeSource(
                new URL("jar:" + outer.toUri().toURL() + "!/BOOT-INF/lib/inner.jar"), (java.security.cert.Certificate[]) null);

        assertEquals("8.8.8", ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
    }

    @Test
    void resolve_nestedJarTrailingSeparator_shouldReadInnerBundleVersion() throws Exception {
        Path inner = buildJar(tempDir.resolve("inner-trailing.jar"), "Bundle-Version", "7.7.7");
        Path outer = buildOuterJar(tempDir.resolve("app-trailing.jar"), "BOOT-INF/lib/inner-trailing.jar", inner);
        CodeSource codeSource = new CodeSource(
                new URL("jar:" + outer.toUri().toURL() + "!/BOOT-INF/lib/inner-trailing.jar!/"), (java.security.cert.Certificate[]) null);

        assertEquals("7.7.7", ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
    }

    @Test
    void resolve_jarProtocolWithoutNestedSeparator_shouldReadBundleVersion() throws Exception {
        Path jar = buildJar(tempDir.resolve("jarproto.jar"), "Bundle-Version", "6.6.6");
        CodeSource codeSource = new CodeSource(new URL("jar:" + jar.toUri().toURL() + "!/"), (java.security.cert.Certificate[]) null);

        assertEquals("6.6.6", ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
    }

    @Test
    void resolve_implementationVersionFallback_shouldBeUsedWhenBundleVersionMissing() throws Exception {
        Path jar = buildJar(tempDir.resolve("impl.jar"), "Implementation-Version", "5.5.5");
        CodeSource codeSource = new CodeSource(jar.toUri().toURL(), (java.security.cert.Certificate[]) null);

        assertEquals("5.5.5", ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
    }

    @Test
    void resolve_nestedJarMissingEntry_shouldFailClosedWithResolveError() throws Exception {
        Path inner = buildJar(tempDir.resolve("inner-missing.jar"), "Bundle-Version", "4.4.4");
        Path outer = buildOuterJar(tempDir.resolve("app-missing.jar"), "BOOT-INF/lib/inner-missing.jar", inner);
        CodeSource codeSource = new CodeSource(
                new URL("jar:" + outer.toUri().toURL() + "!/BOOT-INF/lib/not-exist.jar"), (java.security.cert.Certificate[]) null);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
        assertEquals("unable to resolve protobuf-java artifact version", e.getMessage());
    }

    @Test
    void resolve_jarWithoutVersionAttributes_shouldFailClosedWithDetectError() throws Exception {
        Path jar = buildJar(tempDir.resolve("no-version.jar"), "X-Custom", "1");
        CodeSource codeSource = new CodeSource(jar.toUri().toURL(), (java.security.cert.Certificate[]) null);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ProtobufRuntimeVersion.resolveFromCodeSource(codeSource));
        assertEquals("unable to detect protobuf-java runtime version from artifact manifest", e.getMessage());
    }

    @Test
    void resolve_nullCodeSource_shouldFailClosedWithDetectError() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ProtobufRuntimeVersion.resolveFromCodeSource(null));
        assertEquals("unable to detect protobuf-java runtime version from artifact manifest", e.getMessage());
    }

    private static Path buildJar(Path target, String versionAttribute, String versionValue) throws Exception {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue(versionAttribute, versionValue);
        try (OutputStream out = Files.newOutputStream(target);
             JarOutputStream jos = new JarOutputStream(out, manifest)) {
            jos.putNextEntry(new JarEntry("com/example/Cls.class"));
            jos.write(new byte[]{1});
            jos.closeEntry();
        }
        return target;
    }

    private static Path buildOuterJar(Path target, String innerEntryName, Path innerJar) throws Exception {
        try (OutputStream out = Files.newOutputStream(target);
             JarOutputStream jos = new JarOutputStream(out)) {
            jos.putNextEntry(new JarEntry(innerEntryName));
            jos.write(Files.readAllBytes(innerJar));
            jos.closeEntry();
        }
        return target;
    }
}
