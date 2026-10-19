package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * protobuf-java runtime 版本探测（EDD-VAGW-DSN-CR-017 §4）。
 * <p>
 * 只从实际 jar 的 MANIFEST 读取 {@code Bundle-Version} / {@code Implementation-Version}，
 * 不回退到人工配置字符串。
 * </p>
 */
public final class ProtobufRuntimeVersion {

    private ProtobufRuntimeVersion() {
    }

    public static String detect() {
        String version = com.google.protobuf.Message.class.getPackage().getImplementationVersion();
        if (version != null && !version.isBlank()) {
            return version.trim();
        }
        try {
            java.security.CodeSource codeSource =
                    com.google.protobuf.Message.class.getProtectionDomain().getCodeSource();
            if (codeSource != null && codeSource.getLocation() != null) {
                URI location = codeSource.getLocation().toURI();
                Path artifact = Path.of(location);
                if (Files.isRegularFile(artifact)) {
                    try (JarFile jar = new JarFile(artifact.toFile())) {
                        Manifest manifest = jar.getManifest();
                        if (manifest != null) {
                            String value = manifest.getMainAttributes().getValue("Bundle-Version");
                            if (value == null || value.isBlank()) {
                                value = manifest.getMainAttributes().getValue("Implementation-Version");
                            }
                            if (value != null && !value.isBlank()) {
                                return value.trim();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("unable to resolve protobuf-java artifact version", e);
        }
        throw new IllegalStateException(
                "unable to detect protobuf-java runtime version from artifact manifest");
    }
}
