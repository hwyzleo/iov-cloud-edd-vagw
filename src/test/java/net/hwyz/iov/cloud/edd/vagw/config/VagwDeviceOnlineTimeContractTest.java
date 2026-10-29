package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineEventFactory;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.AdmissionSessionContext;
import net.hwyz.iov.cloud.edd.vagw.model.deviceonline.DeviceOnlineEvent;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import net.hwyz.iov.cloud.framework.web.autoconfigure.IovJacksonAutoConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * vagw.device.online Outbox/Kafka 时间契约（EDD-VAGW-DSN-CR-018 §5.2.1/§5.2.3）。
 * <p>
 * 兼容阶段 DeviceOnlineEvent.occurredAt 输出 13 位 epoch 毫秒（新建事件契约）；读回数字/ISO
 * 归一至同一 Instant（TSP consumer 双读签收前的本服务侧等价验证）。eventId 由稳定连接事实
 * 派生、不含 occurredAt，格式切换不改变幂等摘要；已落 Outbox 的不可变 payload 字符串原样重试，
 * 不因开关重编码或重算 eventId。
 * </p>
 */
@DisplayName("vagw.device.online 时间契约测试（CR-018）")
class VagwDeviceOnlineTimeContractTest {

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

    private DeviceOnlineEvent sampleEvent() {
        return new DeviceOnlineEvent(
                "evt-stable-1", DeviceOnlineEvent.TYPE, "DEVICE-1", "VIN1", "sess-1",
                12L, "CERT", null, SAMPLE, DeviceOnlineEvent.TIME_SOURCE_EMQX);
    }

    @Test
    @DisplayName("新建事件：occurredAt 输出 13 位 epoch 毫秒，非 ISO 字符串")
    void newEvent_occurredAt_epochMillis() {
        ObjectMapper mapper = productionMapper();
        String json = new DeviceOnlineEventFactory(mapper).toJson(sampleEvent());
        assertThat(json).contains("\"occurredAt\":" + SAMPLE.toEpochMilli());
        assertThat(json).doesNotContain("\"occurredAt\":\"");
    }

    @Test
    @DisplayName("TSP 侧等价双读：同一 occurredAt 数字与 ISO 读回同一 Instant（业务结果一致）")
    void occurredAt_numericAndIso_sameInstantAndBusinessResult() throws Exception {
        ObjectMapper mapper = productionMapper();
        String numericJson = new DeviceOnlineEventFactory(mapper).toJson(sampleEvent());
        // 历史/未来 ISO 版本（TSP 侧双读签收后可能收到 ISO 形式的事件，本服务读回验证）
        String isoJson = numericJson.replace("\"occurredAt\":" + SAMPLE.toEpochMilli(),
                "\"occurredAt\":\"" + SAMPLE + "\"");

        DeviceOnlineEvent fromNumeric = mapper.readValue(numericJson, DeviceOnlineEvent.class);
        DeviceOnlineEvent fromIso = mapper.readValue(isoJson, DeviceOnlineEvent.class);

        assertThat(fromNumeric.occurredAt()).isEqualTo(SAMPLE);
        assertThat(fromIso.occurredAt()).isEqualTo(SAMPLE);
        // 幂等摘要（eventId）与稳定连接事实不受 occurredAt 格式影响
        assertThat(fromNumeric.eventId()).isEqualTo(fromIso.eventId());
        assertThat(fromNumeric.eventId()).isEqualTo(sampleEvent().eventId());
    }

    @Test
    @DisplayName("eventId 派生不含 occurredAt：格式/时刻变化不改变幂等摘要")
    void eventId_notDerivedFromOccurredAt() {
        AdmissionSessionContext ctx = new AdmissionSessionContext(
                "DEVICE-1", "VIN1", "CERT", "client1", "10.0.0.1", 5, 12L, SAMPLE, SAMPLE);
        ObjectMapper mapper = productionMapper();
        DeviceOnlineEventFactory factory = new DeviceOnlineEventFactory(mapper);

        String eventIdA = factory.eventId("node-a", "sess-1", "DEVICE-1", 12L);
        // 同一稳定连接事实：与 occurredAt（EMQX ts）无关，换时刻 eventId 不变
        MqttEventRequest request = request("sess-1", "node-a", null);
        DeviceOnlineEvent atT = factory.create(ctx, request, SAMPLE);
        DeviceOnlineEvent later = factory.create(ctx, request, SAMPLE.plusSeconds(60));
        assertThat(atT.eventId()).isEqualTo(eventIdA);
        assertThat(later.eventId()).isEqualTo(eventIdA);
    }

    private MqttEventRequest request(String sessionId, String node, Long ts) {
        MqttEventRequest request = new MqttEventRequest();
        request.setEvent("client.connected");
        request.setUsername("DEVICE-1");
        request.setClientId("client1");
        request.setSessionId(sessionId);
        request.setNode(node);
        request.setTimestamp(ts);
        return request;
    }

    @Test
    @DisplayName("已落 Outbox 载荷不可变：payload 字符串原样重试，不重编码")
    void sunkPayload_immutableOnRetry() {
        ObjectMapper mapper = productionMapper();
        String json = new DeviceOnlineEventFactory(mapper).toJson(sampleEvent());
        // Outbox 将 payload 作为已固化 String 存储并原样发布（DeviceOnlineOutboxPublisher 不做二次序列化）；
        // 这里锁定"固化后字符串与再次序列化结果一致"，防止开关热翻转导致已固化载荷被改写
        String jsonAfterRetry = new DeviceOnlineEventFactory(mapper).toJson(sampleEvent());
        assertThat(jsonAfterRetry).isEqualTo(json);
    }
}
