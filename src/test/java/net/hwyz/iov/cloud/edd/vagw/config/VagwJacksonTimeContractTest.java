package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineEventFactory;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.framework.web.autoconfigure.IovJacksonAutoConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * VAGW 受管 JSON java.time 时间契约测试（EDD-VAGW-DSN-CR-018 §5.2.3 / FW-WEB-DSN-CR-001）。
 * <p>
 * 装配与生产一致：Boot {@link JacksonAutoConfiguration} + 框架 {@link IovJacksonAutoConfiguration}，
 * 注入生产 mapper 断言；VAGW 不再定义宽泛 ObjectMapper bean（原 AppConfig 抑制框架策略，
 * 已删除，commit 见实现记录）。
 * </p>
 * <p>
 * <b>受管 java.time 字段清单（核验于 2026-10-28，iov-cloud-edd-vagw，commit：本 CR 实现提交）：</b>
 * <table>
 *   <tr><th>DTO/Event</th><th>字段</th><th>mapper 来源</th></tr>
 *   <tr><td>DeviceOnlineEvent（vagw.device.online）</td><td>occurredAt (Instant)</td>
 *       <td>DeviceOnlineEventFactory.toJson → 注入 Spring ObjectMapper（Outbox/Kafka）</td></tr>
 *   <tr><td>DeviceAdmissionCheckVo（TSP /decide 出站）</td><td>ts (Instant)</td>
 *       <td>Feign encoder → HttpMessageConverter → Spring ObjectMapper</td></tr>
 *   <tr><td>DeviceAdmissionResultVo（TSP 入站）</td><td>updatedAt (Instant)</td>
 *       <td>Feign decoder → Spring ObjectMapper</td></tr>
 *   <tr><td>VehicleAccessIdentityResultVo（TSP resolveByVin 入站）</td><td>bindingUpdatedAt (Instant)</td>
 *       <td>Feign decoder → Spring ObjectMapper</td></tr>
 *   <tr><td>SessionInfo（Redis 会话 JSON）</td><td>connectedAt/disconnectedAt/lastSeen (Instant)</td>
 *       <td>SessionService → 注入 Spring ObjectMapper</td></tr>
 *   <tr><td>AdmissionSessionContext（Redis 快照 JSON）</td><td>admittedAt/expiresAt (Instant)</td>
 *       <td>AdmissionSessionContextRegistry → 注入 Spring ObjectMapper</td></tr>
 *   <tr><td>VehicleAccessBinding（Redis 绑定缓存 JSON）</td><td>bindingUpdatedAt (Instant)</td>
 *       <td>BindingServiceImpl → 注入 Spring ObjectMapper</td></tr>
 *   <tr><td>VehicleBridgeInbox.InboxRecord（Redis 幂等 JSON）</td><td>updatedAt (Instant)</td>
 *       <td>VehicleBridgeInbox → 注入 Spring ObjectMapper</td></tr>
 *   <tr><td>AuthenticatedIdentity（Redis JSON）</td><td>authenticatedAt (Instant)</td>
 *       <td>AuthenticatedIdentityStore → 注入 Spring ObjectMapper</td></tr>
 * </table>
 * 上述字段全部为 Instant 绝对时刻，<b>无 LocalDateTime / OffsetDateTime JSON 字段</b>。
 * {@code iov.web.jackson.zone-id: Asia/Shanghai} 为框架启动校验要求的显式业务 ZoneId；
 * 它不参与 Instant 字段的 epoch/ISO 转换，但仍是潜在 LDT/ODT 数字转换路径的实际策略——
 * <b>后续新增 LDT/ODT 字段必须重新核验时间语义，不得直接沿用本配置</b>（EDD-VAGW-DSN-CR-018 §5.2.3）。
 * </p>
 * <p>
 * 排除范围（不经本开关改写，另见 {@code VagwEmqxTimeBoundaryTest} / {@code VagwStatusPathContractTest}）：
 * EMQX ts（Long，外部固定单位）、last_online_at/last_offline_at（既有 ISO String）、Proto bytes/hash、
 * TTL、签名、Topic 路由、VMD keyprov canonical payload。
 * </p>
 */
@DisplayName("VAGW 受管 JSON java.time 时间契约测试（CR-018）")
class VagwJacksonTimeContractTest {

    private static final Instant SAMPLE = Instant.parse("2026-10-28T12:43:32.823Z");

    private TimeZone previousDefault;

    @AfterEach
    void restoreDefaultZone() {
        if (previousDefault != null) {
            TimeZone.setDefault(previousDefault);
        }
    }

    private ApplicationContextRunner runner(String... props) {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        JacksonAutoConfiguration.class,
                        IovJacksonAutoConfiguration.class));
        if (props.length > 0) {
            runner = runner.withPropertyValues(props);
        }
        return runner;
    }

    // ---------- 兼容阶段写出：epoch-millis（13 位） ----------

    @Test
    @DisplayName("epoch-millis 模式 + 显式 zone：Instant 输出 13 位 epoch 毫秒")
    void epochMillisMode_instantWrites13DigitMillis() {
        runner("iov.web.jackson.date-format=epoch-millis", "iov.web.jackson.zone-id=Asia/Shanghai")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ObjectMapper mapper = context.getBean(ObjectMapper.class);
                    assertThat(mapper.writeValueAsString(SAMPLE)).isEqualTo(String.valueOf(SAMPLE.toEpochMilli()));
                });
    }

    @Test
    @DisplayName("DeviceOnlineEvent 载荷：occurredAt 输出 13 位 epoch 毫秒（新建事件契约）")
    void deviceOnlineEvent_occurredAtWrites13DigitMillis() {
        runner("iov.web.jackson.zone-id=Asia/Shanghai")
                .run(context -> {
                    ObjectMapper mapper = context.getBean(ObjectMapper.class);
                    DeviceOnlineEventFactory factory = new DeviceOnlineEventFactory(mapper);
                    AdmissionSessionContext ctx = new AdmissionSessionContext(
                            "DEVICE-1", "VIN1", "CERT", "client1", "10.0.0.1", 5, 12L, SAMPLE, SAMPLE);
                    DeviceOnlineEvent event = new DeviceOnlineEvent(
                            "evt-1", DeviceOnlineEvent.TYPE, "DEVICE-1", "VIN1", "sess-1",
                            12L, "CERT", null, SAMPLE, DeviceOnlineEvent.TIME_SOURCE_EMQX);
                    String json = factory.toJson(event);
                    assertThat(json).contains("\"occurredAt\":" + SAMPLE.toEpochMilli());
                    assertThat(json).doesNotContain("\"occurredAt\":\"");
                });
    }

    // ---------- 双读：ISO / 10 位秒 / 13 位毫秒 → 同一 Instant ----------

    @Test
    @DisplayName("双读：ISO 字符串 / 10 位秒 / 13 位毫秒归一至同一 Instant")
    void dualRead_isoSecondsMillis_sameInstant() {
        runner("iov.web.jackson.zone-id=Asia/Shanghai")
                .run(context -> {
                    ObjectMapper mapper = context.getBean(ObjectMapper.class);
                    // ISO / 13 位毫秒：保留亚秒精度
                    assertThat(mapper.readValue("\"" + SAMPLE + "\"", Instant.class)).isEqualTo(SAMPLE);
                    assertThat(mapper.readValue(String.valueOf(SAMPLE.toEpochMilli()), Instant.class)).isEqualTo(SAMPLE);
                    // 10 位秒：仅秒级精度（亚秒信息不在 epoch 秒表示中），秒值必须一致
                    Instant fromSeconds = mapper.readValue(String.valueOf(SAMPLE.getEpochSecond()), Instant.class);
                    assertThat(fromSeconds.getEpochSecond()).isEqualTo(SAMPLE.getEpochSecond());
                });
    }

    @Test
    @DisplayName("双读：null 保持可空语义，不转换为 epoch 0")
    void dualRead_nullStaysNull() {
        runner("iov.web.jackson.zone-id=Asia/Shanghai")
                .run(context -> {
                    ObjectMapper mapper = context.getBean(ObjectMapper.class);
                    assertThat(mapper.readValue("null", Instant.class)).isNull();
                });
    }

    // ---------- 启动门禁（RD-001-1/RD-001-4，FW-WEB 探针） ----------

    @Test
    @DisplayName("epoch-millis 模式缺少 zone-id：启动探针阻断（强制核验时区语义）")
    void epochMillisMode_withoutZone_startupFails() {
        runner("iov.web.jackson.date-format=epoch-millis")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("启动探针失败");
                });
    }

    @Test
    @DisplayName("非法 date-format：属性绑定失败，启动失败")
    void invalidDateFormat_startupFails() {
        runner("iov.web.jackson.date-format=banana", "iov.web.jackson.zone-id=Asia/Shanghai")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    @DisplayName("冲突时间 serializer（epoch-millis 模式强制 ISO 字符串输出）：探针阻断启动")
    void conflictingTimeSerializer_probeBlocks() {
        runner("iov.web.jackson.zone-id=Asia/Shanghai")
                .withUserConfiguration(ConflictingTimeCustomizerConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("启动探针失败");
                });
    }

    @Test
    @DisplayName("无关 customizer 共存：框架策略生效，服务定制保留")
    void unrelatedCustomizer_coexists() {
        runner("iov.web.jackson.zone-id=Asia/Shanghai")
                .withUserConfiguration(UnrelatedCustomizerConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    ObjectMapper mapper = context.getBean(ObjectMapper.class);
                    // 框架策略仍生效：Instant 输出 epoch 毫秒数字
                    assertThat(mapper.writeValueAsString(SAMPLE)).isEqualTo(String.valueOf(SAMPLE.toEpochMilli()));
                    // 服务定制保留：String 自定义序列化器（后注册覆盖）
                    assertThat(mapper.writeValueAsString("hello")).isEqualTo("\"SVC:hello\"");
                });
    }

    // ---------- 时区独立性：不使用 systemDefault，UTC 与上海 JVM 结果一致 ----------

    @Test
    @DisplayName("JVM 默认时区 UTC 与 Asia/Shanghai：同一 Instant 的写出/读入完全一致")
    void jvmDefaultZone_utcAndShanghai_identical() {
        String[] utcResult = new String[1];
        String[] shanghaiResult = new String[1];
        Instant[] readBack = new Instant[1];

        runWithDefaultZone(TimeZone.getTimeZone("UTC"), () -> {
            runner("iov.web.jackson.zone-id=Asia/Shanghai")
                    .run(context -> {
                        ObjectMapper mapper = context.getBean(ObjectMapper.class);
                        utcResult[0] = mapper.writeValueAsString(SAMPLE);
                        readBack[0] = mapper.readValue(utcResult[0], Instant.class);
                    });
        });
        runWithDefaultZone(TimeZone.getTimeZone("Asia/Shanghai"), () -> {
            runner("iov.web.jackson.zone-id=Asia/Shanghai")
                    .run(context -> {
                        ObjectMapper mapper = context.getBean(ObjectMapper.class);
                        shanghaiResult[0] = mapper.writeValueAsString(SAMPLE);
                        assertThat(mapper.readValue(shanghaiResult[0], Instant.class)).isEqualTo(readBack[0]);
                    });
        });

        assertThat(shanghaiResult[0]).isEqualTo(utcResult[0]);
        assertThat(readBack[0]).isEqualTo(SAMPLE);
    }

    @Test
    @DisplayName("双读 ISO/数字输入：UTC 与上海 JVM 归一至同一 Instant")
    void dualRead_jvmZoneIndependent() {
        Instant[] utc = new Instant[1];
        Instant[] sh = new Instant[1];
        runWithDefaultZone(TimeZone.getTimeZone("UTC"), () -> {
            runner("iov.web.jackson.zone-id=Asia/Shanghai")
                    .run(context -> utc[0] = context.getBean(ObjectMapper.class)
                            .readValue("\"" + SAMPLE + "\"", Instant.class));
        });
        runWithDefaultZone(TimeZone.getTimeZone("Asia/Shanghai"), () -> {
            runner("iov.web.jackson.zone-id=Asia/Shanghai")
                    .run(context -> sh[0] = context.getBean(ObjectMapper.class)
                            .readValue("\"" + SAMPLE + "\"", Instant.class));
        });
        assertThat(sh[0]).isEqualTo(utc[0]).isEqualTo(SAMPLE);
    }

    // ---------- 模块回归：JavaTimeModule 默认能力保留（LocalDate 等非时间戳类型） ----------

    @Test
    @DisplayName("LocalDate 等非本开关类型保留 JavaTimeModule 默认语义")
    void nonManagedTypes_keepJavaTimeModuleDefaults() {
        runner("iov.web.jackson.zone-id=Asia/Shanghai")
                .run(context -> {
                    ObjectMapper mapper = context.getBean(ObjectMapper.class);
                    JsonNode node = mapper.valueToTree(java.time.LocalDate.of(2026, 10, 14));
                    assertThat(node.asText()).isEqualTo("2026-10-14");
                });
    }

    // ---------- 辅助 ----------

    private void runWithDefaultZone(TimeZone zone, Runnable action) {
        previousDefault = TimeZone.getDefault();
        TimeZone.setDefault(zone);
        try {
            action.run();
        } finally {
            TimeZone.setDefault(previousDefault);
            previousDefault = null;
        }
    }

    @Configuration
    static class UnrelatedCustomizerConfig {
        @org.springframework.context.annotation.Bean
        public Jackson2ObjectMapperBuilderCustomizer unrelatedCustomizer() {
            return builder -> builder.modulesToInstall(new SimpleModule("svc-string") {
                {
                    addSerializer(String.class, new com.fasterxml.jackson.databind.JsonSerializer<String>() {
                        @Override
                        public void serialize(String value,
                                com.fasterxml.jackson.core.JsonGenerator gen,
                                com.fasterxml.jackson.databind.SerializerProvider serializers) throws java.io.IOException {
                            gen.writeString("SVC:" + value);
                        }
                    });
                }
            });
        }
    }

    @Configuration
    static class ConflictingTimeCustomizerConfig {
        @org.springframework.context.annotation.Bean
        public Jackson2ObjectMapperBuilderCustomizer conflictingTimeCustomizer() {
            return builder -> builder.postConfigurer(objectMapper -> objectMapper
                    .registerModule(new SimpleModule("svc-iso-force") {
                        {
                            addSerializer(Instant.class, new com.fasterxml.jackson.databind.JsonSerializer<Instant>() {
                                @Override
                                public void serialize(Instant value,
                                        com.fasterxml.jackson.core.JsonGenerator gen,
                                        com.fasterxml.jackson.databind.SerializerProvider serializers) throws java.io.IOException {
                                    // 与 epoch-millis 模式冲突：强制输出 ISO 字符串，探针要求数字 → 阻断
                                    gen.writeString(value.toString());
                                }
                            });
                        }
                    }));
        }
    }
}
