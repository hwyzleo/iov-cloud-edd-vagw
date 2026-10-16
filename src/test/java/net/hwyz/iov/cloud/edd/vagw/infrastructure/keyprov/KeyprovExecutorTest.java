package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Keyprov 专属有界执行器测试（EDD-VAGW-DSN-CR-016 §2.3/§7-3/§7-4）。
 */
class KeyprovExecutorTest {

    private KeyprovExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private KeyprovExecutor executor(int workers, int queueCapacity) {
        KeyprovProperties properties = new KeyprovProperties();
        properties.setMaxInFlight(Math.max(1, workers));
        properties.setWorkerCount(workers);
        properties.setQueueCapacity(queueCapacity);
        executor = new KeyprovExecutor(properties, new KeyprovMetrics());
        return executor;
    }

    @Test
    void zeroQueue_shouldRejectWhenWorkerBusy_notUseCallerRuns() throws Exception {
        KeyprovExecutor executor = executor(1, 0);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        assertTrue(executor.submit(() -> {
            started.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertTrue(started.await(5, TimeUnit.SECONDS));

        // worker 忙且队列容量为 0 → 立即拒绝；绝不能 CallerRuns（否则本线程会执行任务）
        assertFalse(executor.submit(() -> { /* should never run on caller */ }));

        release.countDown();
    }

    @Test
    void boundedQueue_shouldAcceptUpToCapacityThenReject() throws Exception {
        KeyprovExecutor executor = executor(1, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        assertTrue(executor.submit(() -> {
            started.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertTrue(executor.submit(() -> { }));
        assertTrue(executor.submit(() -> { }));
        assertFalse(executor.submit(() -> { }), "超过有界队列容量必须拒绝");

        release.countDown();
    }

    @Test
    void threadName_shouldBeDedicatedKeyprovThread() throws Exception {
        KeyprovExecutor executor = executor(1, 0);
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        assertTrue(executor.submit(() -> {
            threadName.set(Thread.currentThread().getName());
            done.countDown();
        }));
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertNotNull(threadName.get());
        assertTrue(threadName.get().startsWith("keyprov-vmd-"),
                "VMD 调用必须运行在专属线程，实际: " + threadName.get());
    }

    @Test
    void taskException_shouldNotBreakExecutor() throws Exception {
        KeyprovExecutor executor = executor(1, 0);
        CountDownLatch firstFinished = new CountDownLatch(1);
        assertTrue(executor.submit(() -> {
            try {
                throw new IllegalStateException("boom");
            } finally {
                firstFinished.countDown();
            }
        }));
        assertTrue(firstFinished.await(5, TimeUnit.SECONDS));

        long idleDeadline = System.currentTimeMillis() + 2000L;
        while (executor.activeCount() > 0 && System.currentTimeMillis() < idleDeadline) {
            Thread.sleep(5L);
        }

        CountDownLatch done = new CountDownLatch(1);
        assertTrue(executor.submit(done::countDown));
        assertTrue(done.await(5, TimeUnit.SECONDS), "单个任务异常不得拖垮专属执行器");
    }

    @Test
    void queueWait_isBoundedForBulkhead_notExecutor() {
        // 执行器不承担等待语义：仅 bulkhead 的 queue-wait 有界等待。
        KeyprovProperties properties = new KeyprovProperties();
        properties.setQueueWait(Duration.ofMillis(10));
        assertEquals(Duration.ofMillis(10), properties.getQueueWait());
    }
}
