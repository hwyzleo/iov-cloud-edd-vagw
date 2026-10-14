package net.hwyz.iov.cloud.edd.vagw.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Kafka 幂等 Producer 配置归一化（EDD-VAGW-DSN-CR-012 §3.5）。
 * <p>
 * Kafka 约束：{@code enable.idempotence=true} 时 {@code retries} 必须 &gt; 0，否则创建 Producer
 * 直接失败：
 * <pre>Must set retries to non-zero when using the idempotent producer.</pre>
 * 本服务按 CR-012 §3.5 固定幂等生产；但平台共享 {@code kafka.yaml} 可能下发
 * {@code spring.kafka.producer.retries=0}（历史为关闭客户端重试）。共享配置优先级高于本服务
 * 本地 {@code application.yml}，因此该冲突无法只靠本地配置消除，且只会在首次 publish 时暴露，
 * 表现为 Outbox 每轮认领重试都失败（静默积压）。
 * </p>
 * <p>
 * 本组件在 {@link KafkaProperties} 完成配置绑定后统一归一化：仅当“幂等生效且有效 retries 解析为 0”
 * 时，将 typed {@code retries} 置为 Kafka 默认 {@link Integer#MAX_VALUE} 并打印 WARN，使幂等承诺与
 * 环境配置解耦；不改变其他任何 producer 参数。
 * </p>
 */
@Slf4j
@Component
public class KafkaIdempotentProducerNormalizer implements BeanPostProcessor {

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof KafkaProperties kafkaProperties) {
            normalize(kafkaProperties.getProducer());
        }
        return bean;
    }

    private static void normalize(KafkaProperties.Producer producer) {
        if (producer == null || !idempotenceEnabled(producer)) {
            return;
        }
        Long effectiveRetries = effectiveRetries(producer.getRetries(), producer.getProperties());
        if (effectiveRetries != null && effectiveRetries == 0L) {
            producer.setRetries(Integer.MAX_VALUE);
            log.warn("Kafka 幂等 Producer 要求 retries>0，检测到 retries=0 且 enable.idempotence=true，"
                            + "已将 retries 归一化为 {}；请校正共享 kafka.yaml，避免运行期发布失败",
                    Integer.MAX_VALUE);
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
