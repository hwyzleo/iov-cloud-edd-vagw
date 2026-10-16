package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keyprov 专属有界执行器（EDD-VAGW-DSN-CR-016 §2.3）。
 * <p>
 * 仅承载 Keyprov VMD 阻塞式调用与其编排，不与 MQTT I/O、FOTA、远控或公共异步执行器共用。
 * 默认零等待队列（{@link SynchronousQueue}）+ {@link ThreadPoolExecutor.AbortPolicy}：容量耗尽
 * 立即拒绝，禁止无界队列、动态无界线程和 {@code CallerRunsPolicy}。若环境显式启用短队列，
 * 容量与最大等待时间必须有界且仅驻留内存；线程异常处理不输出任务对象或 payload。
 * </p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
public class KeyprovExecutor {

    private final ThreadPoolExecutor executor;
    private final KeyprovMetrics metrics;

    public KeyprovExecutor(KeyprovProperties properties, KeyprovMetrics metrics) {
        int workers = Math.max(1, properties.effectiveWorkerCount());
        int queueCapacity = Math.max(0, properties.getQueueCapacity());
        BlockingQueue<Runnable> queue = queueCapacity == 0
                ? new SynchronousQueue<>()
                : new ArrayBlockingQueue<>(queueCapacity);
        this.executor = new ThreadPoolExecutor(
                workers, workers, 0L, TimeUnit.MILLISECONDS, queue,
                new KeyprovThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        this.metrics = metrics;
        log.info("Keyprov executor initialized: workers={}, queueCapacity={}, policy=Abort", workers, queueCapacity);
    }

    /**
     * 提交一个最小不可变任务。
     *
     * @return 提交成功为 {@code true}；容量/线程耗尽被拒绝为 {@code false}（映射 KEYPROV_EXECUTION_REJECTED）
     */
    public boolean submit(Runnable task) {
        try {
            executor.execute(wrap(task));
            return true;
        } catch (RejectedExecutionException e) {
            metrics.executorRejected("queue_full");
            log.warn("Keyprov executor rejected task (bounded capacity exhausted)");
            return false;
        }
    }

    private Runnable wrap(Runnable task) {
        return () -> {
            metrics.executorActiveDelta(1);
            try {
                task.run();
            } catch (Throwable t) {
                // 只输出异常类型与消息，绝不输出任务对象或 payload
                metrics.executorRejected("task_error");
                log.error("Keyprov task failed: type={}", t.getClass().getSimpleName());
            } finally {
                metrics.executorActiveDelta(-1);
            }
        };
    }

    public int activeCount() {
        return executor.getActiveCount();
    }

    public long completedTaskCount() {
        return executor.getCompletedTaskCount();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** 专属线程命名：{@code keyprov-vmd-<n>}；不继承任何任务上下文。 */
    private static final class KeyprovThreadFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "keyprov-vmd-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
