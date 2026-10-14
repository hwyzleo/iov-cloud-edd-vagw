package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * VAGW Kafka 生产/消费属性归一化（EDD-VAGW-DSN-CR-006 §6/§9 + EDD-VAGW-DSN-CR-012 §3.5）。
 * <p>
 * 平台共享 {@code kafka.yaml} 的优先级高于本服务本地 {@code application.yml}，以下两类契约无法只靠
 * 本地配置保证，且都只在运行期首次收发时暴露，本组件在 {@link KafkaProperties} 绑定完成后统一归一化：
 * </p>
 * <ol>
 *   <li><b>序列化契约</b>：VAGW 所有 Producer 使用 {@code KafkaTemplate<String, byte[]>}，
 *       Value 为原始字节（Proto Envelope / DeviceOnlineEvent JSON bytes），Key 为 String；
 *       Spring Boot 的 {@code KafkaProperties} 默认 {@code value.serializer=StringSerializer}，
 *       会导致 {@code Can't convert value of class [B to ... StringSerializer}。Consumer 同理，
 *       下行消费 {@code ConsumerRecord<String, byte[]>}，需 {@code ByteArrayDeserializer}。
 *       （框架的 reactive String 模板自带 String 序列化器，不受影响。）</li>
 *   <li><b>幂等契约</b>：{@code enable.idempotence=true} 时 Kafka 要求 {@code retries>0}，否则创建
 *       Producer 直接失败：{@code Must set retries to non-zero when using the idempotent producer.}</li>
 * </ol>
 */
@Slf4j
@Component
public class VagwKafkaPropertiesNormalizer implements BeanPostProcessor {

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof KafkaProperties kafkaProperties) {
            normalizeProducer(kafkaProperties.getProducer());
            normalizeConsumer(kafkaProperties.getConsumer());
        }
        return bean;
    }

    private static void normalizeProducer(KafkaProperties.Producer producer) {
        if (producer == null) {
            return;
        }
        if (!StringSerializer.class.equals(producer.getKeySerializer())) {
            log.warn("Kafka producer key.serializer 归一化为 StringSerializer（原 {}）：VAGW Key 契约固定为 String",
                    producer.getKeySerializer());
            producer.setKeySerializer(StringSerializer.class);
        }
        if (!ByteArraySerializer.class.equals(producer.getValueSerializer())) {
            log.warn("Kafka producer value.serializer 归一化为 ByteArraySerializer（原 {}）："
                            + "VAGW Value 契约为原始字节（Proto Envelope / DeviceOnlineEvent JSON bytes）",
                    producer.getValueSerializer());
            producer.setValueSerializer(ByteArraySerializer.class);
        }
        if (idempotenceEnabled(producer)) {
            Long retries = effectiveRetries(producer.getRetries(), producer.getProperties());
            if (retries != null && retries == 0L) {
                producer.setRetries(Integer.MAX_VALUE);
                log.warn("Kafka 幂等 Producer 要求 retries>0，检测到 retries=0 且 enable.idempotence=true，"
                        + "已将 retries 归一化为 {}；请校正共享 kafka.yaml", Integer.MAX_VALUE);
            }
        }
    }

    private static void normalizeConsumer(KafkaProperties.Consumer consumer) {
        if (consumer == null) {
            return;
        }
        if (!StringDeserializer.class.equals(consumer.getKeyDeserializer())) {
            log.warn("Kafka consumer key.deserializer 归一化为 StringDeserializer（原 {}）",
                    consumer.getKeyDeserializer());
            consumer.setKeyDeserializer(StringDeserializer.class);
        }
        if (!ByteArrayDeserializer.class.equals(consumer.getValueDeserializer())) {
            log.warn("Kafka consumer value.deserializer 归一化为 ByteArrayDeserializer（原 {}）："
                            + "VAGW 下行 Value 契约为原始字节",
                    consumer.getValueDeserializer());
            consumer.setValueDeserializer(ByteArrayDeserializer.class);
        }
    }

    /**
     * 幂等是否生效：显式 {@code enable.idempotence} 优先；未显式配置时按 kafka-clients 3.x 语义
     * 默认开启，仅当显式 {@code acks} 非 all 时才视为关闭。
     */
    private static boolean idempotenceEnabled(KafkaProperties.Producer producer) {
        String explicit = producer.getProperties().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        if (explicit != null) {
            return Boolean.parseBoolean(explicit.trim());
        }
        String acks = producer.getAcks();
        return acks == null || "all".equalsIgnoreCase(acks.trim()) || "-1".equals(acks.trim());
    }

    /**
     * 有效 retries：typed 属性优先于 properties map；均未配置时返回 null（走 Kafka 默认，非 0）。
     */
    private static Long effectiveRetries(Integer typedRetries, Map<String, String> extra) {
        if (typedRetries != null) {
            return typedRetries.longValue();
        }
        String fromMap = extra.get(ProducerConfig.RETRIES_CONFIG);
        if (fromMap == null) {
            return null;
        }
        try {
            return Long.parseLong(fromMap.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
