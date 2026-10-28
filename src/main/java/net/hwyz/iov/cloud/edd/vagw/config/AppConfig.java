package net.hwyz.iov.cloud.edd.vagw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 全局 ObjectMapper（EDD-VAGW-DSN-CR-013 契约加固）。
 * <p>
 * 禁用 {@link SerializationFeature#WRITE_DATES_AS_TIMESTAMPS}，强制 {@code Instant}/
 * {@code LocalDateTime} 等 java.time 类型序列化为 ISO-8601 字符串（与 Spring Boot 默认
 * ObjectMapper 行为一致），而非 Jackson JSR310 默认的 epoch 秒数字：
 * <ul>
 *   <li>HTTP Feign 调用（如 TSP 设备准入 {@code /decide} 请求体 {@code ts} 字段）统一为
 *       ISO 字符串，避免与对端“数字=epoch 毫秒”的反序列化语义错配；</li>
 *   <li>数字语义歧义（epoch 秒/毫秒在不同框架与工具间不一致）不再进入 wire 契约。</li>
 * </ul>
 * 背景：TSP 侧 2026-10-28 上线 {@code InstantEpochMilliDeserializer}（数字一律按 epoch 毫秒解析），
 * 而本服务此前输出 epoch 秒数字，导致 {@code ts} 被对端误读为 1970 年，证书有效期裁决
 * fail-closed 拒绝全部设备准入。
 *
 * @author hwyz_leo
 */
@Configuration
public class AppConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
