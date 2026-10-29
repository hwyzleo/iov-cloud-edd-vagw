package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.framework.web.autoconfigure.IovJacksonAutoConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 内部 Redis JSON 缓存/快照时间契约（EDD-VAGW-DSN-CR-018 §5.2.1 "不得因全局 mapper 误改缓存"）。
 * <p>
 * SessionService（SessionInfo）、AdmissionSessionContextRegistry（准入快照）、BindingServiceImpl
 * （VehicleAccessBinding）以注入 Spring ObjectMapper 读写 Redis JSON。兼容阶段新写为 13 位
 * epoch 毫秒；历史缓存（旧 ISO 字符串，e847adb 修复期写入）经双读仍可原样读回同一时刻，
 * 无需清理/迁移，也不会被误读为 epoch 0。
 * </p>
 */
@DisplayName("内部 Redis JSON 缓存双读契约测试（CR-018）")
class VagwRedisTimeContractTest {

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

    @Test
    @DisplayName("SessionInfo：旧 ISO 缓存可读回同一 Instant，新写为 13 位毫秒并往返一致")
    void sessionInfo_dualReadAndRoundTrip() throws Exception {
        ObjectMapper mapper = productionMapper();

        // 历史缓存：e847adb 修复期写入的 ISO 字符串形式
        String legacyIso = "{\"deviceSn\":\"DEVICE-1\",\"vin\":\"VIN1\",\"online\":true,"
                + "\"connectedAt\":\"2026-10-28T12:43:32.823Z\",\"disconnectedAt\":null,"
                + "\"lastSeen\":\"2026-10-28T12:43:32.823Z\"}";
        SessionInfo fromLegacy = mapper.readValue(legacyIso, SessionInfo.class);
        assertThat(fromLegacy.getConnectedAt()).isEqualTo(SAMPLE);
        assertThat(fromLegacy.getLastSeen()).isEqualTo(SAMPLE);

        // 新写：epoch 毫秒并往返一致
        SessionInfo session = new SessionInfo();
        session.setDeviceSn("DEVICE-1");
        session.setOnline(true);
        session.setConnectedAt(SAMPLE);
        session.setLastSeen(SAMPLE);
        String written = mapper.writeValueAsString(session);
        assertThat(written).contains("\"connectedAt\":" + SAMPLE.toEpochMilli());
        SessionInfo round = mapper.readValue(written, SessionInfo.class);
        assertThat(round.getConnectedAt()).isEqualTo(SAMPLE);
        assertThat(round.getLastSeen()).isEqualTo(SAMPLE);
    }

    @Test
    @DisplayName("AdmissionSessionContext：admittedAt/expiresAt 双读与往返一致")
    void admissionContext_dualReadAndRoundTrip() throws Exception {
        ObjectMapper mapper = productionMapper();

        AdmissionSessionContext context = new AdmissionSessionContext(
                "DEVICE-1", "VIN1", "CERT", "client1", "10.0.0.1", 5, 12L, SAMPLE, SAMPLE.plusSeconds(300));

        String legacyIso = "{\"hsmUid\":\"DEVICE-1\",\"vin\":\"VIN1\",\"certSerial\":\"CERT\","
                + "\"clientId\":\"client1\",\"sourceIp\":\"10.0.0.1\",\"protoVer\":5,"
                + "\"connectionGeneration\":12,\"admittedAt\":\"2026-10-28T12:43:32.823Z\","
                + "\"expiresAt\":\"2026-10-28T12:48:32.823Z\"}";
        AdmissionSessionContext fromLegacy = mapper.readValue(legacyIso, AdmissionSessionContext.class);
        assertThat(fromLegacy.admittedAt()).isEqualTo(SAMPLE);
        assertThat(fromLegacy.expiresAt()).isEqualTo(SAMPLE.plusSeconds(300));

        String written = mapper.writeValueAsString(context);
        assertThat(written).contains("\"admittedAt\":" + SAMPLE.toEpochMilli());
        AdmissionSessionContext round = mapper.readValue(written, AdmissionSessionContext.class);
        assertThat(round.admittedAt()).isEqualTo(SAMPLE);
        assertThat(round.expiresAt()).isEqualTo(SAMPLE.plusSeconds(300));
    }

    @Test
    @DisplayName("VehicleAccessBinding：bindingUpdatedAt 双读与往返一致")
    void binding_dualReadAndRoundTrip() throws Exception {
        ObjectMapper mapper = productionMapper();

        VehicleAccessBinding binding = VehicleAccessBinding.builder()
                .vin("VIN1").hsmUid("DEVICE-1").bindingVersion(3L).bindingUpdatedAt(SAMPLE)
                .source(net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource.ADMISSION)
                .build();

        String legacyIso = "{\"vin\":\"VIN1\",\"hsmUid\":\"DEVICE-1\",\"bindingVersion\":3,"
                + "\"bindingUpdatedAt\":\"2026-10-28T12:43:32.823Z\","
                + "\"source\":\"ADMISSION\"}";
        VehicleAccessBinding fromLegacy = mapper.readValue(legacyIso, VehicleAccessBinding.class);
        assertThat(fromLegacy.getBindingUpdatedAt()).isEqualTo(SAMPLE);

        String written = mapper.writeValueAsString(binding);
        assertThat(written).contains("\"bindingUpdatedAt\":" + SAMPLE.toEpochMilli());
        VehicleAccessBinding round = mapper.readValue(written, VehicleAccessBinding.class);
        assertThat(round.getBindingUpdatedAt()).isEqualTo(SAMPLE);
    }
}
