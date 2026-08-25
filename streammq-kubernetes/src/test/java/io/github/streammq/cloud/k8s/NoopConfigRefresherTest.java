/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@link NoopConfigRefresher} 单元测试，验证所有方法为空操作且不抛出异常。 */
@DisplayName("空操作配置刷新器测试")
class NoopConfigRefresherTest {

    private NoopConfigRefresher refresher;

    @BeforeEach
    void setUp() {
        refresher = new NoopConfigRefresher();
    }

    @Test
    @DisplayName("refreshRetryPolicy - 不抛出异常")
    void refreshRetryPolicy_doesNotThrow() {
        assertThatCode(() -> refresher.refreshRetryPolicy(16, new long[] {1000L, 2000L}))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("refreshRetryPolicy - null 参数时不抛出异常")
    void refreshRetryPolicy_withNullDoesNotThrow() {
        assertThatCode(() -> refresher.refreshRetryPolicy(0, null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("refreshConsumerThreads - 不抛出异常")
    void refreshConsumerThreads_doesNotThrow() {
        assertThatCode(() -> refresher.refreshConsumerThreads(2, 16)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("refreshScanInterval - 不抛出异常")
    void refreshScanInterval_doesNotThrow() {
        assertThatCode(() -> refresher.refreshScanInterval(5000L, 10000L))
                .doesNotThrowAnyException();
    }
}
