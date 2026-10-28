package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AppConfig ObjectMapper wire 契约测试（EDD-VAGW-DSN-CR-013 加固）。
 * <p>
 * 锁定 {@code Instant} 序列化为 ISO-8601 字符串（非 epoch 秒数字）：
 * 对端 TSP 的 {@code InstantEpochMilliDeserializer} 将 JSON 数字一律按 epoch 毫秒解析，
 * 若本服务输出 epoch 秒数字，{@code ts} 会被误读为 1970 年导致设备准入证书有效期
 * 裁决 fail-closed 拒绝。本测试防止该回归。
 */
class AppConfigTest {

    private final ObjectMapper objectMapper = new AppConfig().objectMapper();

    @Test
    void instant_shouldSerializeToIso8601String() throws Exception {
        Instant instant = Instant.parse("2026-10-28T11:35:36.823Z");
        String json = objectMapper.writeValueAsString(instant);
        assertEquals("\"2026-10-28T11:35:36.823Z\"", json);
    }

    @Test
    void instant_shouldRoundTripThroughIsoString() throws Exception {
        Instant instant = Instant.parse("2026-10-28T11:35:36.823Z");
        String json = objectMapper.writeValueAsString(instant);
        Instant parsed = objectMapper.readValue(json, Instant.class);
        assertEquals(instant, parsed);
    }

    @Test
    void instant_shouldNotSerializeAsEpochNumber() throws Exception {
        // 回归保护：epoch 秒/毫秒数字语义在不同框架间有歧义，wire 契约禁止数字形式
        String json = objectMapper.writeValueAsString(Instant.parse("2026-10-28T11:35:36.823Z"));
        assertTrue(json.startsWith("\""), "Instant 必须以 ISO-8601 字符串输出，实际: " + json);
    }
}
