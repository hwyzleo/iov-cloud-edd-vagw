package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VMD 契约复用门禁测试（EDD-VAGW-DSN-CR-016 §2.4/§7-2，VMD-DSN-CR-059 对齐）。
 * <p>
 * 断言 VAGW 源码中不存在第二套 VMD 契约：无自定义 {@code @FeignClient}、无重复 relay/request/response
 * wire DTO、无 Disabled Client 兜底、无旧 {@code VmdBusinessKeyProvisioningService}；Gateway Adapter
 * 只委托官方 edd-vmd-api {@code VmdKeyProvisioningService}（request/ack/status）。
 * </p>
 */
class KeyprovContractReuseTest {

    private static final Path SOURCE_ROOT =
            Path.of("src", "main", "java", "net", "hwyz", "iov", "cloud", "edd", "vagw");

    @Test
    void vagw_shouldNotDeclareCustomFeignClient() throws IOException {
        // 仅匹配真正的注解用法（@FeignClient(...)），排除注释/错误文案中的字面量
        List<String> offenders = scan(path -> read(path).contains("@FeignClient("));
        assertEquals(List.of(), offenders, "VAGW 不得定义第二套 @FeignClient（官方契约在 edd-vmd-api）");
    }

    @Test
    void vagw_shouldNotDeclareDuplicateVmdWireDtos() throws IOException {
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT)) {
            paths.filter(p -> p.toString().endsWith(".java")).forEach(p -> {
                String content = read(p);
                if (content.contains("class KeyprovRelayRequest")
                        || content.contains("interface KeyprovRelayRequest")
                        || content.contains("class KeyprovRelayResponse")
                        || content.contains("interface KeyprovRelayResponse")
                        || content.contains("class VmdRelayRequest")
                        || content.contains("class VmdRelayResponse")
                        || content.contains("VmdBusinessKeyProvisioningService")) {
                    offenders.add(p.toString());
                }
            });
        }
        assertEquals(List.of(), offenders, "VAGW 不得复制/保留旧的 VMD 请求响应 wire DTO 或旧 Feign 契约");
    }

    @Test
    void vagw_shouldNotKeepDisabledClientPlaceholder() throws IOException {
        List<String> offenders = scan(path -> {
            String content = read(path);
            return content.contains("DisabledVmdKeyProvisioningClient")
                    || content.contains("implements VmdKeyProvisioningClient")
                    || content.contains("interface VmdKeyProvisioningClient");
        });
        assertEquals(List.of(), offenders, "VAGW 不得保留 Disabled Client 或旧 VmdKeyProvisioningClient SPI");
    }

    @Test
    void gatewayAdapter_shouldDelegateToOfficialVmdContract() throws IOException {
        Path adapter = SOURCE_ROOT.resolve(
                "infrastructure/client/VmdKeyProvisioningGatewayAdapter.java");
        assertTrue(Files.exists(adapter), "gateway adapter must exist: " + adapter);
        String content = read(adapter);
        assertTrue(content.contains("import net.hwyz.iov.cloud.edd.vmd.api.service.VmdKeyProvisioningService;"),
                "adapter 必须注入官方 VmdKeyProvisioningService");
        assertTrue(content.contains("import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningRequest;"),
                "adapter 必须使用官方 /request DTO");
        assertTrue(content.contains("import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningAckRequest;"),
                "adapter 必须使用官方 /ack DTO");
        assertTrue(content.contains(
                        "import net.hwyz.iov.cloud.edd.vmd.api.vo.keyprov.VmdKeyProvisioningStatusRequest;"),
                "adapter 必须使用官方 /status DTO");
        assertTrue(content.contains("vmdService.request(")
                        && content.contains("vmdService.ack(")
                        && content.contains("vmdService.status("),
                "adapter 必须按 canonical 类型分派 request/ack/status 三端点");
    }

    private static List<String> scan(java.util.function.Predicate<Path> predicate) throws IOException {
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT)) {
            paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(predicate)
                    .map(Path::toString)
                    .forEach(offenders::add);
        }
        return offenders;
    }

    private static String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
