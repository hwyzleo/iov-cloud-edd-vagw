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
 * VIAM 契约复用门禁测试（EDD-VAGW-DSN-CR-016 §2.4/§7-2，owner 由 EDD-VAGW-DSN-CR-019
 * 切换：VMD → VIAM；T019-06/07 相关断言）。
 * <p>
 * 断言 VAGW 源码中不存在第二套 VIAM 契约：无自定义 {@code @FeignClient}、无重复 relay/request/response
 * wire DTO、无 Disabled Client 兜底、无旧 {@code Vmd*}/{@code Tsp*} 安全 owner 残留；Gateway Adapter
 * 只委托官方 sec-viam-api {@code ViamKeyProvisioningService}（request/ack/status）。
 * </p>
 */
class KeyprovContractReuseTest {

    private static final Path SOURCE_ROOT =
            Path.of("src", "main", "java", "net", "hwyz", "iov", "cloud", "edd", "vagw");

    @Test
    void vagw_shouldNotDeclareCustomFeignClient() throws IOException {
        // 仅匹配真正的注解用法（@FeignClient(...)），排除注释/错误文案中的字面量
        List<String> offenders = scan(path -> read(path).contains("@FeignClient("));
        assertEquals(List.of(), offenders, "VAGW 不得定义第二套 @FeignClient（官方契约在 sec-viam-api）");
    }

    @Test
    void vagw_shouldNotDeclareDuplicateWireDtos() throws IOException {
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(SOURCE_ROOT)) {
            paths.filter(p -> p.toString().endsWith(".java")).forEach(p -> {
                String content = read(p);
                if (content.contains("class ViamRelayRequest")
                        || content.contains("interface ViamRelayRequest")
                        || content.contains("class ViamRelayResponse")
                        || content.contains("interface ViamRelayResponse")
                        || content.contains("class VmdRelayRequest")
                        || content.contains("class VmdRelayResponse")
                        || content.contains("VmdBusinessKeyProvisioningService")) {
                    offenders.add(p.toString());
                }
            });
        }
        assertEquals(List.of(), offenders,
                "VAGW 不得复制/保留 VIAM 请求响应 wire DTO、旧 VMD DTO 或旧 Feign 契约");
    }

    @Test
    void vagw_shouldNotKeepDisabledClientPlaceholder() throws IOException {
        List<String> offenders = scan(path -> {
            String content = read(path);
            return content.contains("DisabledViamKeyProvisioningClient")
                    || content.contains("implements ViamKeyProvisioningClient")
                    || content.contains("interface ViamKeyProvisioningClient")
                    || content.contains("DisabledVmdKeyProvisioningClient")
                    || content.contains("VmdKeyProvisioningClient");
        });
        assertEquals(List.of(), offenders,
                "VAGW 不得保留 Disabled Client 或旧 VmdKeyProvisioningClient SPI（T019-07）");
    }

    @Test
    void gatewayAdapter_shouldDelegateToOfficialViamContract() throws IOException {
        Path adapter = SOURCE_ROOT.resolve(
                "infrastructure/client/ViamKeyProvisioningGatewayAdapter.java");
        assertTrue(Files.exists(adapter), "gateway adapter must exist: " + adapter);
        String content = read(adapter);
        assertTrue(content.contains("import net.hwyz.iov.cloud.sec.viam.api.service.ViamKeyProvisioningService;"),
                "adapter 必须注入官方 ViamKeyProvisioningService");
        assertTrue(content.contains("import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningRequest;"),
                "adapter 必须使用官方 /request DTO");
        assertTrue(content.contains("import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningAckRequest;"),
                "adapter 必须使用官方 /ack DTO");
        assertTrue(content.contains(
                        "import net.hwyz.iov.cloud.sec.viam.api.vo.keyprov.ViamKeyProvisioningStatusRequest;"),
                "adapter 必须使用官方 /status DTO");
        assertTrue(content.contains("viamService.request(")
                        && content.contains("viamService.ack(")
                        && content.contains("viamService.status("),
                "adapter 必须按 canonical 类型分派 request/ack/status 三端点");
    }

    @Test
    void vagw_shouldNotReferenceLegacySecurityOwners() throws IOException {
        // CR-019 §8 / T019-12：VAGW 已迁安全链路不得残留旧 owner 引用
        // （iov-tsp-api / edd-vmd-api 源码 import；pom 依赖在零合法引用后删除，另行校验）
        List<String> offenders = scan(path -> {
            String content = read(path);
            return content.contains("import net.hwyz.iov.cloud.iov.tsp.api")
                    || content.contains("import net.hwyz.iov.cloud.edd.vmd.api");
        });
        assertEquals(List.of(), offenders,
                "VAGW 已迁安全链路源码不得残留 TSP/VMD 旧 owner import（T019-12）");
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
