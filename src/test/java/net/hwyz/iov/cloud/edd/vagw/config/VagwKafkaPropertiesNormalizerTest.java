package net.hwyz.iov.cloud.edd.vagw.config;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VAGW Kafka 生产/消费属性归一化测试（EDD-VAGW-DSN-CR-006 §6/§9 + EDD-VAGW-DSN-CR-012 §3.5）。
 * <p>
 * 回归：
 * <ul>
 *   <li>Value 为 byte[] 但 {@code value.serializer=StringSerializer} →
 *       {@code Can't convert value of class [B to ... StringSerializer}；</li>
 *   <li>{@code enable.idempotence=true} 且 {@code retries=0} →
 *       {@code Must set retries to non-zero when using the idempotent producer}。</li>
 * </ul>
 */
@DisplayName("VagwKafkaPropertiesNormalizer 测试")
class VagwKafkaPropertiesNormalizerTest {

    private final VagwKafkaPropertiesNormalizer normalizer = new VagwKafkaPropertiesNormalizer();

    private KafkaProperties normalize(KafkaProperties properties) {
        normalizer.postProcessAfterInitialization(properties, "kafkaProperties");
        return properties;
    }

    @Test
    @DisplayName("Producer Value 归一化为 ByteArraySerializer（Boot 默认 StringSerializer）")
    void normalizesProducerValueSerializer() {
        KafkaProperties properties = normalize(new KafkaProperties());

        assertEquals(ByteArraySerializer.class, properties.getProducer().getValueSerializer());
        assertEquals(StringSerializer.class, properties.getProducer().getKeySerializer());
    }

    @Test
    @DisplayName("Consumer Value 归一化为 ByteArrayDeserializer（Boot 默认 StringDeserializer）")
    void normalizesConsumerValueDeserializer() {
        KafkaProperties properties = normalize(new KafkaProperties());

        assertEquals(ByteArrayDeserializer.class, properties.getConsumer().getValueDeserializer());
        assertEquals(StringDeserializer.class, properties.getConsumer().getKeyDeserializer());
    }

    @Test
    @DisplayName("幂等开启且 typed retries=0 → 归一化为正数")
    void overridesTypedZeroRetries() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().getProperties().put("enable.idempotence", "true");
        properties.getProducer().setRetries(0);

        normalize(properties);

        assertTrue(properties.getProducer().getRetries() > 0);
    }

    @Test
    @DisplayName("幂等开启且 properties.retries=0 → 归一化为正数")
    void overridesMapZeroRetries() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().getProperties().put("enable.idempotence", "true");
        properties.getProducer().getProperties().put("retries", "0");

        normalize(properties);

        assertTrue(properties.getProducer().getRetries() > 0);
    }

    @Test
    @DisplayName("未显式配置幂等（3.x 默认开启）且 retries=0 → 归一化为正数")
    void overridesZeroRetriesWhenIdempotenceDefaultsOn() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().setRetries(0);

        normalize(properties);

        assertTrue(properties.getProducer().getRetries() > 0);
    }

    @Test
    @DisplayName("显式关闭幂等 → 保留 retries=0，不干预")
    void keepsZeroRetriesWhenIdempotenceDisabled() {
        KafkaProperties properties = new KafkaProperties();
        properties.getProducer().getProperties().put("enable.idempotence", "false");
        properties.getProducer().setRetries(0);

        normalize(properties);

        assertEquals(0, properties.getProducer().getRetries());
    }

    @Test
    @DisplayName("非 KafkaProperties Bean 原样返回")
    void leavesOtherBeansUntouched() {
        Object other = new Object();

        assertSame(other, normalizer.postProcessAfterInitialization(other, "other"));
    }

    @Test
    @DisplayName("集成装配：Boot KafkaAutoConfiguration 下序列化器与幂等 retries 均被归一化")
    void normalizesWithinKafkaAutoConfiguration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withBean(VagwKafkaPropertiesNormalizer.class)
                .withPropertyValues(
                        "spring.kafka.producer.retries=0",
                        "spring.kafka.producer.properties.enable.idempotence=true")
                .run(context -> {
                    assertNull(context.getStartupFailure(), "上下文应正常启动");
                    KafkaProperties properties = context.getBean(KafkaProperties.class);
                    assertEquals(ByteArraySerializer.class, properties.getProducer().getValueSerializer(),
                            "Value 为 byte[]，value.serializer 必须是 ByteArraySerializer");
                    assertEquals(ByteArrayDeserializer.class, properties.getConsumer().getValueDeserializer(),
                            "下行消费 byte[]，value.deserializer 必须是 ByteArrayDeserializer");
                    assertTrue(properties.getProducer().getRetries() > 0,
                            "幂等 producer 的 retries 必须被归一化为正数，否则 Producer 创建即失败");
                });
    }
}
