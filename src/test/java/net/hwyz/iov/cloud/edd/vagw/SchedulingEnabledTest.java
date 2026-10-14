package net.hwyz.iov.cloud.edd.vagw;

import net.hwyz.iov.cloud.edd.vagw.application.deviceonline.DeviceOnlineReconciler;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.outbox.DeviceOnlineOutboxPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 调度开启回归防护测试（EDD-VAGW-DSN-CR-012 §3.5/§4）。
 * <p>
 * Spring Boot 默认不处理 {@link Scheduled}：若启动类未声明 {@link EnableScheduling}，
 * 所有 {@code @Scheduled} 任务（设备上线 Outbox 发布器、会话对账）会被<strong>静默忽略</strong>——
 * 典型故障现象是 MQTT Hook 能打印 {@code outbox enqueued}，但 Kafka 永不发布、offset 不增长，
 * 且日志中既无 {@code published} 也无 {@code publish failure} 错误。
 * </p>
 * <p>
 * 本测试锁定“应用确实开启了调度”这一前提，并校验被调度的组件仍带有 {@code @Scheduled} 方法，
 * 防止 {@code @EnableScheduling} 被误删导致再次静默回归。
 * </p>
 */
@DisplayName("调度开启回归防护测试")
class SchedulingEnabledTest {

    @Test
    @DisplayName("启动类（含元注解）声明 @EnableScheduling，否则所有 @Scheduled 任务不会注册")
    void applicationEnablesScheduling() {
        assertTrue(AnnotatedElementUtils.hasAnnotation(Application.class, EnableScheduling.class),
                "Application 缺少 @EnableScheduling：@Scheduled 发布器/对账任务不会被注册，"
                        + "Outbox 事件将永远无法发布到 Kafka");
    }

    @Test
    @DisplayName("设备上线发布器与会话对账确实声明了 @Scheduled 方法（守卫才有意义）")
    void scheduledComponentsDeclareScheduledMethods() {
        assertTrue(hasScheduledMethod(DeviceOnlineOutboxPublisher.class),
                "DeviceOnlineOutboxPublisher 应至少有一个 @Scheduled 方法（publishPending）");
        assertTrue(hasScheduledMethod(DeviceOnlineReconciler.class),
                "DeviceOnlineReconciler 应至少有一个 @Scheduled 方法（reconcile）");
    }

    private static boolean hasScheduledMethod(Class<?> type) {
        for (Method method : type.getDeclaredMethods()) {
            if (AnnotatedElementUtils.hasAnnotation(method, Scheduled.class)) {
                return true;
            }
        }
        return false;
    }
}
