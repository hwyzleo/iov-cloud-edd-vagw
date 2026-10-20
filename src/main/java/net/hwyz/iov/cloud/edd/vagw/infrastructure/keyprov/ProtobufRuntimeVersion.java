package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * protobuf-java runtime 版本探测（EDD-VAGW-DSN-CR-017 §4）。
 * <p>
 * 只从实际 jar 的 MANIFEST 读取 {@code Bundle-Version} / {@code Implementation-Version}，
 * 不回退到人工配置字符串。
 * </p>
 * <p>
 * 兼容两类运行形态（CR-017 实现缺陷修复）：
 * </p>
 * <ul>
 *   <li>普通文件 jar（IDE / Maven surefire / 解包 classpath）：CodeSource location 为
 *       {@code file:/.../protobuf-java-*.jar}；</li>
 *   <li>Spring Boot 可执行 fat jar（protobuf-java 位于 {@code BOOT-INF/lib/*.jar} 嵌套 jar）：
 *       CodeSource location 为 {@code jar:file:/.../app.jar!/BOOT-INF/lib/protobuf-java-*.jar}。
 *       嵌套 jar 由本类自行按 {@code jar:<outer>!/<inner>} 逐层打开，不依赖 Boot loader 的
 *       URL handler，保证 CI 单测与生产行为一致。</li>
 * </ul>
 * <p>
 * 任一路径解析失败均 fail-closed：抛 {@link IllegalStateException} 并保持稳定消息，
 * 由 {@link KeyprovContractGuard} 将 Keyprov capability 置为 NOT_READY（不影响 FOTA）。
 * </p>
 */
public final class ProtobufRuntimeVersion {

    private static final String MANIFEST_ENTRY = "META-INF/MANIFEST.MF";

    private ProtobufRuntimeVersion() {
    }

    public static String detect() {
        String version = com.google.protobuf.Message.class.getPackage().getImplementationVersion();
        if (version != null && !version.isBlank()) {
            return version.trim();
        }
        return resolveFromCodeSource(com.google.protobuf.Message.class.getProtectionDomain().getCodeSource());
    }

    /**
     * 从 CodeSource 定位 protobuf-java 所在 artifact 的 MANIFEST 版本。
     * <p>package-private，便于单测直接构造普通 jar 与嵌套 jar 两种 location 形态。</p>
     */
    static String resolveFromCodeSource(java.security.CodeSource codeSource) {
        if (codeSource == null || codeSource.getLocation() == null) {
            throw new IllegalStateException(
                    "unable to detect protobuf-java runtime version from artifact manifest");
        }
        Manifest manifest;
        try {
            manifest = manifestOf(codeSource.getLocation());
        } catch (Exception e) {
            throw new IllegalStateException("unable to resolve protobuf-java artifact version", e);
        }
        String value = manifest.getMainAttributes().getValue("Bundle-Version");
        if (value == null || value.isBlank()) {
            value = manifest.getMainAttributes().getValue("Implementation-Version");
        }
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "unable to detect protobuf-java runtime version from artifact manifest");
        }
        return value.trim();
    }

    private static Manifest manifestOf(URL location) throws IOException, java.net.URISyntaxException {
        return "jar".equals(location.getProtocol())
                ? manifestOfJarLocation(location)
                : manifestOfPlainJar(location);
    }

    /**
     * 解析 {@code jar:} location：可能是普通 jar（{@code jar:file:/.../x.jar}）或嵌套 jar
     * （{@code jar:file:/.../outer.jar!/<inner>}，允许尾部多余 {@code !/}）。嵌套 jar 由外层
     * JarFile 定位内层条目后流式读取其 MANIFEST，不依赖任何自定义 URL handler。
     */
    private static Manifest manifestOfJarLocation(URL location) throws IOException, java.net.URISyntaxException {
        String file = location.getFile();
        while (file.endsWith("!/")) {
            file = file.substring(0, file.length() - 2);
        }
        int sep = file.indexOf("!/");
        if (sep < 0) {
            // jar:file:/.../x.jar（或尾部带 !/）→ 从剥离后的 file 重建内层普通 jar URL
            return manifestOfPlainJar(new URL(file));
        }
        URL outer = new URL(file.substring(0, sep));
        String innerPath = file.substring(sep + 2);
        try (JarFile outerJar = new JarFile(Path.of(outer.toURI()).toFile())) {
            JarEntry entry = outerJar.getJarEntry(innerPath);
            if (entry == null) {
                throw new IOException("nested jar entry not found: " + innerPath + " in " + outer);
            }
            try (InputStream in = outerJar.getInputStream(entry)) {
                return manifestOfJarStream(in);
            }
        }
    }

    private static Manifest manifestOfPlainJar(URL location) throws IOException, java.net.URISyntaxException {
        try (JarFile jar = new JarFile(Path.of(location.toURI()).toFile())) {
            Manifest manifest = jar.getManifest();
            if (manifest == null) {
                throw new IOException("jar has no manifest: " + location);
            }
            return manifest;
        }
    }

    /**
     * 从内层 jar 的字节流读取 MANIFEST；MANIFEST 通常为首条目，此处额外扫描兜底
     * （JarInputStream.getManifest() 只在首条目为 MANIFEST 时返回）。
     */
    private static Manifest manifestOfJarStream(InputStream in) throws IOException {
        try (JarInputStream jis = new JarInputStream(in)) {
            Manifest manifest = jis.getManifest();
            if (manifest != null) {
                return manifest;
            }
            JarEntry entry;
            while ((entry = jis.getNextJarEntry()) != null) {
                if (MANIFEST_ENTRY.equals(entry.getName())) {
                    return new Manifest(jis);
                }
            }
        }
        throw new IOException("nested jar has no manifest");
    }
}
