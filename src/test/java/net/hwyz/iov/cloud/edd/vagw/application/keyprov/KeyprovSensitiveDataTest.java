package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.VmdKeyProvisioningClient;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.VmdRelayResponse;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovOutcome;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
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
 * keyprov 敏感数据处理测试（EDD-VAGW-DSN-CR-011 §8/§11-7）。
 * <p>
 * 覆盖：结果对象 toString 不泄露 Wrapped Key 明文；keyprov 相关源码无 JsonFormat/TextFormat
 * 或对 payload 明文 toString/序列化的日志/持久化路径。完整 heap/core dump 扫描属发布门禁，
 * 不在此单元测试范围。
 * </p>
 */
class KeyprovSensitiveDataTest {

    private static final String SECRET = "WRAPPED-KEY-PLAINTEXT-SECRET";

    @Test
    void vmdRelayResponse_toString_shouldNotLeakWrappedKeyPlaintext() {
        VmdRelayResponse response = new VmdRelayResponse("req-1",
                "security.keyprov.v1.KeyProvisioningResponse",
                SECRET.getBytes(StandardCharsets.UTF_8), 1L, "DEVICE001",
                KeyprovOutcome.SUCCESS, null);

        assertFalse(response.toString().contains(SECRET));
    }

    @Test
    void relayResult_toString_shouldNotLeakSensitivePayload() {
        KeyprovRelayResult result = KeyprovRelayResult.fail(KeyprovReason.KEYPROV_DOWNLINK_PUBLISH_FAILED, "m1");
        assertFalse(result.toString().contains(SECRET));
    }

    @Test
    void vmdRelayRequest_toString_shouldNotLeakEnvelopeOrPayloadBytes() {
        Envelope.VehicleMessageEnvelope envelope = Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov").setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001").setMessageId("m1").setRequestId("r1")
                .setPayloadType("security.keyprov.v1.KeyProvisioningRequest")
                .setPayload(com.google.protobuf.ByteString.copyFrom(SECRET.getBytes(StandardCharsets.UTF_8)))
                .build();
        VmdKeyProvisioningClient.VmdRelayRequest request = new VmdKeyProvisioningClient.VmdRelayRequest(
                net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource.ACL_SESSION_CORRELATED,
                null, envelope, SECRET.getBytes(StandardCharsets.UTF_8));

        assertFalse(request.toString().contains(SECRET),
                "VmdRelayRequest.toString 不得输出 Envelope/payload 内容");
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
                        return name.contains("/keyprov/") || name.contains("Keyprov");
                    })
                    .filter(KeyprovSensitiveDataTest::containsForbiddenSensitiveSerialization)
                    .map(Path::toString)
                    .toList();
        }
        assertEquals(List.of(), offenders, "keyprov sources must not serialize sensitive payload");
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
