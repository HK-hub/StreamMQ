/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** {@link FailureRetryLimiter} 单元测试：冷却期行为、成功清除、并发安全与边界。 */
class FailureRetryLimiterTest {

    @Test
    void initiallyNotBlocked() {
        FailureRetryLimiter limiter = new FailureRetryLimiter(5_000L);
        assertThat(limiter.isBlocked("op:key")).isFalse();
        assertThat(limiter.remainingCooldownMillis("op:key")).isZero();
    }

    @Test
    void failureEntersCooldownAndExpires() throws InterruptedException {
        FailureRetryLimiter limiter = new FailureRetryLimiter(100L);
        limiter.recordFailure("requeueDlq:g:1");

        assertThat(limiter.isBlocked("requeueDlq:g:1")).isTrue();
        assertThat(limiter.remainingCooldownMillis("requeueDlq:g:1")).isBetween(1L, 100L);

        Thread.sleep(150L);
        assertThat(limiter.isBlocked("requeueDlq:g:1")).isFalse();
        assertThat(limiter.remainingCooldownMillis("requeueDlq:g:1")).isZero();
    }

    @Test
    void successClearsCooldown() {
        FailureRetryLimiter limiter = new FailureRetryLimiter(5_000L);
        limiter.recordFailure("createTopic:t");
        assertThat(limiter.isBlocked("createTopic:t")).isTrue();

        limiter.recordSuccess("createTopic:t");
        assertThat(limiter.isBlocked("createTopic:t")).isFalse();
    }

    @Test
    void keysAreIsolatedPerTarget() {
        FailureRetryLimiter limiter = new FailureRetryLimiter(5_000L);
        limiter.recordFailure("ackPending:a");
        assertThat(limiter.isBlocked("ackPending:a")).isTrue();
        assertThat(limiter.isBlocked("ackPending:b")).isFalse();
    }

    @Test
    void zeroCooldownDisablesLimiting() {
        FailureRetryLimiter limiter = new FailureRetryLimiter(0L);
        limiter.recordFailure("deleteTopic:t");
        assertThat(limiter.isBlocked("deleteTopic:t")).isFalse();
        assertThat(limiter.remainingCooldownMillis("deleteTopic:t")).isZero();
    }

    @Test
    void negativeCooldownRejected() {
        assertThatThrownBy(() -> new FailureRetryLimiter(-1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cooldownMillis");
    }

    @Test
    void evictExpiredOnlyRemovesStaleEntries() throws InterruptedException {
        FailureRetryLimiter limiter = new FailureRetryLimiter(50L);
        limiter.recordFailure("a");
        limiter.recordFailure("b");
        Thread.sleep(80L);
        limiter.recordFailure("c");

        int removed = limiter.evictExpired();

        assertThat(removed).isEqualTo(2);
        assertThat(limiter.isBlocked("a")).isFalse();
        assertThat(limiter.isBlocked("b")).isFalse();
        assertThat(limiter.isBlocked("c")).isTrue();
    }

    @Test
    void clearResetsAllState() {
        FailureRetryLimiter limiter = new FailureRetryLimiter(5_000L);
        limiter.recordFailure("a");
        limiter.recordFailure("b");
        limiter.clear();
        assertThat(limiter.isBlocked("a")).isFalse();
        assertThat(limiter.isBlocked("b")).isFalse();
    }

    @Test
    void concurrentAccessIsSafe() throws InterruptedException {
        FailureRetryLimiter limiter = new FailureRetryLimiter(10_000L);
        int threads = 8;
        int ops = 1_000;
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final String key = "key-" + t;
            workers[t] =
                    new Thread(
                            () -> {
                                for (int i = 0; i < ops; i++) {
                                    limiter.recordFailure(key);
                                    limiter.isBlocked(key);
                                    limiter.recordSuccess(key);
                                }
                            });
            workers[t].start();
        }
        for (Thread w : workers) {
            w.join();
        }
        // 所有操作以 recordSuccess 结束，冷却状态应全部清除
        assertThat(limiter.isBlocked("key-0")).isFalse();
        assertThat(limiter.remainingCooldownMillis("key-7")).isZero();
    }
}
