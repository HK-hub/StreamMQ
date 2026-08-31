/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link RuntimeStatsRegistry} 单元测试。
 *
 * <p>覆盖发布前修复 P1-3 的契约：进程内统计登记表为 {@code /actuator/streammq/stats/{group}/{topic}}
 * 提供真实数据源——包括无数据场景、成功/失败/重试/死信计数、平均耗时派生与并发上报下的线程安全。
 */
@DisplayName("运行时统计登记表测试")
class RuntimeStatsRegistryTest {

    @Test
    @DisplayName("无数据维度返回 noData 标记与 uptime，而非空 map")
    void snapshot_unknownDimension_returnsNoData() {
        RuntimeStatsRegistry registry = new RuntimeStatsRegistry();
        Map<String, Object> snapshot = registry.snapshot("g1", "t1");

        assertThat(snapshot.get("noData")).isEqualTo(true);
        assertThat(snapshot.get("uptimeMillis")).isInstanceOf(Long.class);
        assertThat((long) snapshot.get("uptimeMillis")).isGreaterThanOrEqualTo(0L);
        assertThat(snapshot).doesNotContainKey("consumeTotal");
    }

    @Test
    @DisplayName("上报消费结果后快照字段正确（成功/失败/总数）")
    void snapshot_afterConsumeRecords_hasCorrectCounts() {
        RuntimeStatsRegistry registry = new RuntimeStatsRegistry();
        registry.recordConsume("g1", "t1", true, 10_000_000L); // 10ms
        registry.recordConsume("g1", "t1", true, 30_000_000L); // 30ms
        registry.recordConsume("g1", "t1", false, 20_000_000L); // 20ms

        Map<String, Object> snapshot = registry.snapshot("g1", "t1");
        assertThat(snapshot.get("consumeSuccess")).isEqualTo(2L);
        assertThat(snapshot.get("consumeFailure")).isEqualTo(1L);
        assertThat(snapshot.get("consumeTotal")).isEqualTo(3L);
        // 平均耗时 = (10 + 30 + 20) / 3 = 20ms
        assertThat((double) snapshot.get("avgConsumeMillis")).isEqualTo(20.0);
        assertThat(snapshot).doesNotContainKey("noData");
    }

    @Test
    @DisplayName("重试与死信计数独立记录且不影响消费计数")
    void snapshot_recordsRetryAndDlq() {
        RuntimeStatsRegistry registry = new RuntimeStatsRegistry();
        registry.recordConsume("g1", "t1", true, 5_000_000L);
        registry.recordRetry("g1", "t1");
        registry.recordRetry("g1", "t1");
        registry.recordDlq("g1", "t1");

        Map<String, Object> snapshot = registry.snapshot("g1", "t1");
        assertThat(snapshot.get("retried")).isEqualTo(2L);
        assertThat(snapshot.get("dlq")).isEqualTo(1L);
        assertThat(snapshot.get("consumeTotal")).isEqualTo(1L);
    }

    @Test
    @DisplayName("不同 group/topic 维度互不串扰")
    void snapshot_dimensionsAreIsolated() {
        RuntimeStatsRegistry registry = new RuntimeStatsRegistry();
        registry.recordConsume("g1", "t1", true, 1_000_000L);
        registry.recordConsume("g2", "t2", false, 9_000_000L);

        Map<String, Object> a = registry.snapshot("g1", "t1");
        Map<String, Object> b = registry.snapshot("g2", "t2");
        assertThat(a.get("consumeSuccess")).isEqualTo(1L);
        assertThat(a.get("consumeFailure")).isEqualTo(0L);
        assertThat(b.get("consumeSuccess")).isEqualTo(0L);
        assertThat(b.get("consumeFailure")).isEqualTo(1L);
    }

    @Test
    @DisplayName("并发上报线程安全：总计等于各线程上报之和")
    void concurrentRecording_isThreadSafe() throws Exception {
        RuntimeStatsRegistry registry = new RuntimeStatsRegistry();
        int threads = 8;
        int perThread = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(
                        () -> {
                            try {
                                start.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            for (int j = 0; j < perThread; j++) {
                                registry.recordConsume("g1", "t1", true, 1_000_000L);
                                registry.recordRetry("g1", "t1");
                            }
                        });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }

        Map<String, Object> snapshot = registry.snapshot("g1", "t1");
        assertThat(snapshot.get("consumeSuccess")).isEqualTo((long) threads * perThread);
        assertThat(snapshot.get("consumeTotal")).isEqualTo((long) threads * perThread);
        assertThat(snapshot.get("retried")).isEqualTo((long) threads * perThread);
        assertThat(snapshot.get("consumeFailure")).isEqualTo(0L);
        assertThat(snapshot.get("avgConsumeMillis")).isEqualTo(1.0);
    }
}
