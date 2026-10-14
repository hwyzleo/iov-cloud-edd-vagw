package net.hwyz.iov.cloud.edd.vagw.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka 幂等 Producer 配置归一化测试（EDD-VAGW-DSN-CR-012 §3.5）。
 * <p>
 * 回归：“enable.idempotence=true 且 retries=0”会让 Producer 创建失败
 * （Must set retries to non-zero when using the idempotent producer）。
 * </p>
 */
@DisplayName("KafkaIdempotentProducerNormalizer 测试")
class KafkaIdempotentProducerNormalizerTest {

    private final KafkaIdempotentProducerNormalizer normalizer = new KafkaIdempotentProducerNormalizer();

    @Test
    @DisplayName("幂等开启且 typed retries=0 → 归一化为正数")
    void overridesTypedZeroRetries() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().getProperties().put("enable.idempotence", "true");
        properties.getProducer().setRetries(0);

        normalizer.postProcessAfterInitialization(properties, "kafkaProperties");

        assertTrue(properties.getProducer().getRetries() > 0);
    }

    @Test
    @DisplayName("幂等开启且 properties.retries=0 → 归一化为正数")
    void overridesMapZeroRetries() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().getProperties().put("enable.idempotence", "true");
        properties.getProducer().getProperties().put("retries", "0");

        normalizer.postProcessAfterInitialization(properties, "kafkaProperties");

        assertTrue(properties.getProducer().getRetries() > 0);
    }

    @Test
    @DisplayName("未显式配置幂等（3.x 默认开启）且 retries=0 → 归一化为正数")
    void overridesZeroRetriesWhenIdempotenceDefaultsOn() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().setRetries(0);

        normalizer.postProcessAfterInitialization(properties, "kafkaProperties");

        assertTrue(properties.getProducer().getRetries() > 0);
    }

    @Test
    @DisplayName("显式关闭幂等 → 保留 retries=0，不干预")
    void keepsZeroRetriesWhenIdempotenceDisabled() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().getProperties().put("enable.idempotence", "false");
        properties.getProducer().setRetries(0);

        normalizer.postProcessAfterInitialization(properties, "kafkaProperties");

        assertEquals(0, properties.getProducer().getRetries());
    }

    @Test
    @DisplayName("非 KafkaProperties Bean 原样返回")
    void leavesOtherBeansUntouched() {
        Object other = new Object();

        assertSame(other, normalizer.postProcessAfterInitialization(other, "other"));
    }

    @Test
    @DisplayName("集成装配：Boot KafkaAutoConfiguration 下 properties/typed retries=0 均被归一化")
    void normalizesWithinKafkaAutoConfiguration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withBean(KafkaIdempotentProducerNormalizer.class)
                .withPropertyValues(
                        "spring.kafka.producer.retries=0",
                        "spring.kafka.producer.properties.enable.idempotence=true")
                .run(context -> {
                    assertNull(context.getStartupFailure(), "上下文应正常启动");
                    KafkaProperties properties = context.getBean(KafkaProperties.class);
                    assertTrue(properties.getProducer().getRetries() > 0,
                            "幂等 producer 的 retries 必须被归一化为正数，否则 Producer 创建即失败");
                });
    }
}
