package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import org.junit.jupiter.api.Test;
import vehicle.common.v1.Envelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * keyprov 敏感数据处理测试（EDD-VAGW-DSN-CR-011 §8/§11-7，由 EDD-VAGW-DSN-CR-016 §6/§7 增量修订）。
 * <p>
 * 覆盖：结果对象 toString 不泄露 Wrapped Key 明文；keyprov 相关源码无 JsonFormat/TextFormat
 * 或对 payload 明文 toString/序列化的日志/持久化路径。完整 heap/core dump 扫描属发布门禁，
 * 不在此单元测试范围。
 * </p>
 */
class KeyprovSensitiveDataTest {

    private static final String SECRET = "WRAPPED-KEY-PLAINTEXT-SECRET";

    @Test
    void vmdRelayResult_toString_shouldNotLeakWrappedKeyPlaintext() {
        VmdRelayResult result = new VmdRelayResult("req-1",
                "security.keyprov.v1.KeyProvisioningResponse",
                SECRET.getBytes(StandardCharsets.UTF_8), 1L,
                KeyprovOutcome.SUCCESS, null, false);

        assertFalse(result.toString().contains(SECRET));
    }

    @Test
    void relayResult_toString_shouldNotLeakSensitivePayload() {
        KeyprovRelayResult result = KeyprovRelayResult.fail(KeyprovReason.KEYPROV_DOWNLINK_PUBLISH_FAILED, "m1");
        assertFalse(result.toString().contains(SECRET));
    }

    @Test
    void vmdKeyProvisioningCommand_toString_shouldNotLeakEnvelopeOrPayloadBytes() {
        Envelope.VehicleMessageEnvelope envelope = Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov").setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001").setMessageId("m1").setRequestId("r1")
                .setPayloadType("security.keyprov.v1.KeyProvisioningRequest")
                .setPayload(com.google.protobuf.ByteString.copyFrom(SECRET.getBytes(StandardCharsets.UTF_8)))
                .build();
        VmdKeyProvisioningCommand command = new VmdKeyProvisioningCommand(
                new net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext(
                        KeyprovContextSource.ACL_SESSION_CORRELATED, "DEVICE001", "c1", null, null, 1L, "c1",
                        java.time.Instant.now()),
                envelope, SECRET.getBytes(StandardCharsets.UTF_8));

        assertFalse(command.toString().contains(SECRET),
                "VmdKeyProvisioningCommand.toString 不得输出 Envelope/payload 内容");
    }

    @Test
    void keyprovSources_shouldNotUseJsonTextFormatOrPayloadToString() throws IOException {
        Path root = Path.of("src", "main", "java", "net", "hwyz", "iov", "cloud", "edd", "vagw");
        assertTrue(Files.isDirectory(root), "source root must exist: " + root.toAbsolutePath());

        List<String> offenders;
        try (Stream<Path> paths = Files.walk(root)) {
            offenders = paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> {
                        String name = p.toString();
                        return (name.contains("/keyprov/") || name.contains("Keyprov"))
                                && !name.contains("/build/");
                    })
                    .filter(KeyprovSensitiveDataTest::containsForbiddenSensitiveSerialization)
                    .map(Path::toString)
                    .toList();
        }
        assertEquals(List.of(), offenders, "keyprov sources must not serialize sensitive payload");
    }

    @Test
    void keyprovSources_shouldNotImplementProofCanonicalizationOrSignatureVerification() throws IOException {
        Path root = Path.of("src", "main", "java", "net", "hwyz", "iov", "cloud", "edd", "vagw");
        List<String> offenders;
        try (Stream<Path> paths = Files.walk(root)) {
            offenders = paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> {
                        String name = p.toString();
                        return (name.contains("/keyprov/") || name.contains("Keyprov"))
                                && !name.contains("/build/");
                    })
                    .filter(p -> containsForbiddenCryptoImplementation(p))
                    .map(Path::toString)
                    .toList();
        }
        assertEquals(List.of(), offenders,
                "VAGW 不得实现 proofInputV1/P1363/low-S/ECDSA 验签或证书绑定");
    }

    private static boolean containsForbiddenCryptoImplementation(Path path) {
        String code = stripComments(read(path));
        return code.contains("proofInput")
                || code.contains("P1363")
                || code.contains("ECDSA")
                || code.contains("low-S")
                || code.contains("java.security.Signature")
                || code.contains("Signature.getInstance")
                || code.contains("getImportProof().toString()");
    }

    private static String stripComments(String source) {
        StringBuilder out = new StringBuilder(source.length());
        int i = 0;
        boolean line = false;
        boolean block = false;
        while (i < source.length()) {
            char current = source.charAt(i);
            if (line) {
                if (current == '\n') {
                    line = false;
                    out.append(current);
                }
                i++;
                continue;
            }
            if (block) {
                if (current == '*' && i + 1 < source.length() && source.charAt(i + 1) == '/') {
                    block = false;
                    i += 2;
                } else {
                    i++;
                }
                continue;
            }
            if (current == '/' && i + 1 < source.length() && source.charAt(i + 1) == '/') {
                line = true;
                i += 2;
                continue;
            }
            if (current == '/' && i + 1 < source.length() && source.charAt(i + 1) == '*') {
                block = true;
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
            throw new RuntimeException(e);
        }
    }

    private static boolean containsForbiddenSensitiveSerialization(Path path) {
        String content;
        try {
            content = Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return content.contains("JsonFormat")
                || content.contains("TextFormat")
                || content.contains("getPayload().toString()")
                || content.contains("canonicalPayload.toString()")
                || content.contains("downlinkPayload.toString()");
    }
}
