package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Keyprov 有界并发准入（bulkhead）（EDD-VAGW-DSN-CR-016 §2.2）。
 * <p>
 * 以有界并发许可约束单实例最大 in-flight；许可覆盖“提交后 → VMD 完成／失败 → 下行处理完成”
 * 的完整生命周期，无许可时不调用 VMD，立即生成 {@code KEYPROV_OVERLOADED}。默认零等待队列，
 * 不按 hsmUid 建立高基数锁、队列或指标；成功、异常、超时、取消、下行失败等所有终态都必须释放。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KeyprovAdmissionBulkhead {

    private final Semaphore semaphore;
    private final Duration queueWait;
    private final KeyprovMetrics metrics;

    public KeyprovAdmissionBulkhead(KeyprovProperties properties, KeyprovMetrics metrics) {
        int permits = Math.max(0, properties.getMaxInFlight());
        this.semaphore = new Semaphore(permits, true);
        this.queueWait = properties.getQueueWait() == null ? Duration.ZERO : properties.getQueueWait();
        this.metrics = metrics;
    }

    /**
     * 尝试获取一个 Keyprov 并发许可。
     *
     * @return 许可；许可耗尽时返回 {@code null}，调用方必须立即生成 {@code KEYPROV_OVERLOADED}
     */
    public Permit tryAcquire() {
        boolean acquired;
        try {
            long waitMillis = Math.max(0L, queueWait.toMillis());
            acquired = waitMillis <= 0L
                    ? semaphore.tryAcquire()
                    : semaphore.tryAcquire(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            acquired = false;
        }
        if (!acquired) {
            metrics.admission("overloaded");
            return null;
        }
        metrics.admission("admitted");
        metrics.inflightDelta(1);
        return new Permit();
    }

    /** 当前可用许可数（可观测/测试用）。 */
    public int availablePermits() {
        return semaphore.availablePermits();
    }

    /**
     * 一次 Keyprov 处理的并发许可；必须在处理终态（含异常/取消）的 finally 中关闭。
     */
    public final class Permit implements AutoCloseable {

        private final AtomicBoolean released = new AtomicBoolean(false);

        private Permit() {
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                metrics.inflightDelta(-1);
                semaphore.release();
            }
        }
    }
}
