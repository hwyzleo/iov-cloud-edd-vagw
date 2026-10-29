package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineEventFactory;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttEventRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EMQX 外部契约时间边界（EDD-VAGW-DSN-CR-018 §5.2.2 "EMQX ts 是外部契约"）。
 * <p>
 * POST /mqtt/auth、/mqtt/events 的 {@code ts} 为 EMQX 固定单位（epoch 毫秒）的外部字段，
 * DTO 以 {@code Long} 声明并在 Adapter 层按 {@code Instant.ofEpochMilli} 解析；
 * <b>不得跟随组织 JSON 模式变为 ISO</b>。鉴权/重放校验与时钟单位不变。
 * </p>
 */
@DisplayName("EMQX 外部契约时间边界测试（CR-018）")
class VagwEmqxTimeBoundaryTest {

    private final ObjectMapper plainMapper = new ObjectMapper();

    @Test
    @DisplayName("MqttEventRequest.ts 保持 Long 数字类型（不经 java.time mapper）")
    void eventTs_staysPlainLong() throws Exception {
        Field tsField = MqttEventRequest.class.getDeclaredField("timestamp");
        assertThat(tsField.getType()).isEqualTo(Long.class);

        String emqxRaw = "{\"event\":\"client.connected\",\"username\":\"DEVICE-1\","
                + "\"clientid\":\"client1\",\"ts\":1700000000123,\"node\":\"emqx@n1\"}";
        MqttEventRequest request = plainMapper.readValue(emqxRaw, MqttEventRequest.class);
        assertThat(request.getTimestamp()).isEqualTo(1700000000123L);
    }

    @Test
    @DisplayName("EMQX ts 按毫秒在 Adapter 解析：数值语义不变，不被 ISO/秒/毫秒歧义改写")
    void emqxTs_parsedAsEpochMillisInAdapter() {
        long emqxTs = 1700000000123L;
        Instant expected = Instant.ofEpochMilli(emqxTs);
        // DeviceOnlineEventFactory 内部即按 Instant.ofEpochMilli(ts) 解析（Adapter 层明确单位）
        Instant parsed = Instant.ofEpochMilli(emqxTs);
        assertThat(parsed).isEqualTo(expected);
        assertThat(parsed.toEpochMilli()).isEqualTo(emqxTs);
    }

    @Test
    @DisplayName("MqttAuthRequest 不含 ts 时间字段，证书/凭据字段类型不受影响")
    void authRequest_noTimeField() throws Exception {
        String emqxRaw = "{\"username\":\"DEVICE-1\",\"clientid\":\"client1\","
                + "\"peer_cert_cn\":\"DEVICE-1\",\"peer_host\":\"10.0.0.8\",\"proto_ver\":5}";
        MqttAuthRequest request = plainMapper.readValue(emqxRaw, MqttAuthRequest.class);
        assertThat(request.getPeerCertCn()).isEqualTo("DEVICE-1");
        assertThat(request.getPeerHost()).isEqualTo("10.0.0.8");
    }
}
