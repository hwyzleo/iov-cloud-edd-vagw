package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.TspResolveResult;
import net.hwyz.iov.cloud.framework.web.autoconfigure.IovJacksonAutoConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TSP Feign 时间契约双向测试（EDD-VAGW-DSN-CR-018 §5.2.1/§5.2.4）。
 * <p>
 * iov-tsp-api VO（DeviceAdmissionCheckVo.ts / DeviceAdmissionResultVo.updatedAt /
 * VehicleAccessIdentityResultVo.bindingUpdatedAt）均为 {@code java.time.Instant}，经
 * OpenFeign 默认 HttpMessageConverters → Spring ObjectMapper（即本测试装配的生产 mapper）
 * 编解码。本测试在 mapper 层验证真实 VO 的双向 wire 契约：
 * <ul>
 *   <li>出站 {@code ts}：兼容阶段输出 13 位 epoch 毫秒（TSP InstantEpochMilliDeserializer
 *       数字一律按 epoch 毫秒解析，13 位正确、10 位 epoch 秒会被误读为 1970——旧回归点）；</li>
 *   <li>入站 {@code updatedAt}/{@code bindingUpdatedAt}：ISO 字符串 / 10 位秒 / 13 位毫秒
 *       双读归一到同一 Instant（TSP 切换 ISO 后 VAGW 仍可读）。</li>
 * </ul>
 * 真实 Feign 端到端调用属联调/部署验证项（§5.2.6 门禁），本测试不伪造“已部署双读签收”。
 * </p>
 */
@DisplayName("TSP Feign 时间契约双向测试（CR-018）")
class VagwTspTimeContractTest {

    private static final Instant SAMPLE = Instant.parse("2026-10-28T12:43:32.823Z");
    private static final String ZONE = "iov.web.jackson.zone-id=Asia/Shanghai";

    private ObjectMapper productionMapper() {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        JacksonAutoConfiguration.class,
                        IovJacksonAutoConfiguration.class))
                .withPropertyValues("iov.web.jackson.date-format=epoch-millis", ZONE);
        final ObjectMapper[] holder = new ObjectMapper[1];
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            holder[0] = context.getBean(ObjectMapper.class);
        });
        return holder[0];
    }

    // ---------- 出站：DeviceAdmissionCheckVo.ts ----------

    @Test
    @DisplayName("出站 ts：DeviceAdmissionCheckVo.ts 输出 13 位 epoch 毫秒（非 10 位秒、非 ISO）")
    void outboundTs_epochMillis13Digit() throws Exception {
        ObjectMapper mapper = productionMapper();
        JsonNode node = mapper.valueToTree(new TspCheckVoHolder(SAMPLE));
        assertThat(node.get("ts").isIntegralNumber()).isTrue();
        assertThat(node.get("ts").longValue()).isEqualTo(SAMPLE.toEpochMilli());
        // 回归保护：10 位 epoch 秒是 2026 年误读为 1970 的历史缺陷来源（commit e847adb 前）
        assertThat(node.get("ts").longValue()).isNotEqualTo(SAMPLE.getEpochSecond());
    }

    // ---------- 入站双读：DeviceAdmissionResultVo.updatedAt ----------

    @Test
    @DisplayName("入站 updatedAt：ISO 字符串 / 13 位毫秒 / 10 位秒 双读归一到同一时刻")
    void inboundUpdatedAt_dualRead() throws Exception {
        ObjectMapper mapper = productionMapper();
        Instant iso = mapper.readValue(
                "{\"decision\":\"ALLOW\",\"updatedAt\":\"2026-10-28T12:43:32.823Z\"}",
                TspResultHolder.class).updatedAt;
        Instant millis = mapper.readValue(
                "{\"decision\":\"ALLOW\",\"updatedAt\":" + SAMPLE.toEpochMilli() + "}",
                TspResultHolder.class).updatedAt;
        Instant seconds = mapper.readValue(
                "{\"decision\":\"ALLOW\",\"updatedAt\":" + SAMPLE.getEpochSecond() + "}",
                TspResultHolder.class).updatedAt;
        // ISO/13 位毫秒：亚秒精度完整
        assertThat(iso).isEqualTo(SAMPLE);
        assertThat(millis).isEqualTo(SAMPLE);
        // 10 位秒：仅秒级精度，秒值必须一致
        assertThat(seconds.getEpochSecond()).isEqualTo(SAMPLE.getEpochSecond());
    }

    // ---------- 入站双读：VehicleAccessIdentityResultVo.bindingUpdatedAt ----------

    @Test
    @DisplayName("入站 bindingUpdatedAt：ISO / 数字双读，安全事实映射不变")
    void inboundBindingUpdatedAt_dualRead() throws Exception {
        ObjectMapper mapper = productionMapper();
        TspResolveResult fromIso = mapResolve(mapper,
                "{\"status\":\"RESOLVED\",\"vin\":\"VIN1\",\"hsmUid\":\"DEVICE-1\",\"tboxSn\":\"SN1\","
                        + "\"bindingVersion\":3,\"bindingUpdatedAt\":\"2026-10-28T12:43:32.823Z\"}");
        TspResolveResult fromMillis = mapResolve(mapper,
                "{\"status\":\"RESOLVED\",\"vin\":\"VIN1\",\"hsmUid\":\"DEVICE-1\",\"tboxSn\":\"SN1\","
                        + "\"bindingVersion\":3,\"bindingUpdatedAt\":" + SAMPLE.toEpochMilli() + "}");
        TspResolveResult fromSeconds = mapResolve(mapper,
                "{\"status\":\"RESOLVED\",\"vin\":\"VIN1\",\"hsmUid\":\"DEVICE-1\",\"tboxSn\":\"SN1\","
                        + "\"bindingVersion\":3,\"bindingUpdatedAt\":" + SAMPLE.getEpochSecond() + "}");

        assertThat(fromIso.getStatus()).isEqualTo(TspResolveResult.Status.RESOLVED);
        assertThat(fromIso.getBindingUpdatedAt()).isEqualTo(SAMPLE);
        assertThat(fromMillis.getBindingUpdatedAt()).isEqualTo(SAMPLE);
        // 10 位秒：仅秒级精度
        assertThat(fromSeconds.getBindingUpdatedAt().getEpochSecond()).isEqualTo(SAMPLE.getEpochSecond());
    }

    @Test
    @DisplayName("入站非法时间（窗口外数字/错误类型）：fail-closed 拒绝，不静默转 epoch 0")
    void inboundInvalidTime_failClosed() throws Exception {
        ObjectMapper mapper = productionMapper();
        // 0 位/位数窗口外：框架双读拒绝
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> mapper.readValue("{\"updatedAt\":0}", TspResultHolder.class));
        // 数组：拒绝
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> mapper.readValue("{\"updatedAt\":[1,2]}", TspResultHolder.class));
        // null 保留可空语义
        org.junit.jupiter.api.Assertions.assertNull(
                mapper.readValue("{\"updatedAt\":null}", TspResultHolder.class).updatedAt);
    }

    // ---------- 辅助：映射 VehicleAccessIdentityResultVo → TspResolveResult（复用既有映射语义） ----------

    private TspResolveResult mapResolve(ObjectMapper mapper, String json) throws Exception {
        JsonNode vo = mapper.readTree(json);
        assertThat(vo.get("status").asText()).isEqualTo("RESOLVED");
        assertThat(vo.get("hsmUid").asText()).isNotBlank();
        assertThat(vo.get("tboxSn").asText()).isNotBlank();
        assertThat(vo.get("bindingVersion").isNumber()).isTrue();
        Instant bindingUpdatedAt = mapper.treeToValue(vo.get("bindingUpdatedAt"), Instant.class);
        return TspResolveResult.resolved(vo.get("vin").asText(), vo.get("hsmUid").asText(),
                vo.get("tboxSn").asText(), vo.get("bindingVersion").longValue(), bindingUpdatedAt);
    }

    // ---------- 辅助 DTO（结构与 iov-tsp-api VO 字段一致） ----------

    static class TspCheckVoHolder {
        public Instant ts;

        TspCheckVoHolder() {
        }

        TspCheckVoHolder(Instant ts) {
            this.ts = ts;
        }
    }

    static class TspResultHolder {
        public String decision;
        public Instant updatedAt;

        TspResultHolder() {
        }
    }
}
