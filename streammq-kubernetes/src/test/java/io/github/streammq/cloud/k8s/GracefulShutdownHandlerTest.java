/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.InFlightAware;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

/**
 * {@link GracefulShutdownHandler} 单元测试，验证优雅关闭流程与关闭标志管理。
 *
 * <p>使用 Mockito 模拟 {@link StreamMQListenerContainer} 与 {@link ObjectProvider}， 验证 destroy()
 * 方法在不同场景下的行为，以及 isShuttingDown() 标志的转换。
 *
 * <p><b>K2：</b>在途计数契约 {@link InFlightAware} 位于 core（redisson 容器可依赖），本测试用同时实现容器接口与
 * 该契约的假容器验证：计数归零提前退出、计数恒 &gt; 0 等待到 grace 超时、未实现契约直接跳过等待。
 */
@DisplayName("优雅关闭处理器测试")
@ExtendWith(MockitoExtension.class)
class GracefulShutdownHandlerTest {

    /** 同时实现容器接口与 core 在途计数契约的测试替身（等价于 redisson 容器的实现关系）。 */
    interface InFlightContainer extends StreamMQListenerContainer, InFlightAware {}

    @Mock private ObjectProvider<StreamMQListenerContainer> containerProvider;

    @Mock private StreamMQListenerContainer container;

    @Mock private InFlightContainer inFlightContainer;

    private CloudK8sProperties properties;

    private GracefulShutdownHandler handler;

    @BeforeEach
    void setUp() {
        properties = new CloudK8sProperties();
        properties.setGracefulShutdownTimeoutMs(0L);
        handler = new GracefulShutdownHandler(containerProvider, properties);
    }

    @Test
    @DisplayName("初始状态 isShuttingDown 为 false")
    void isShuttingDown_initiallyFalse() {
        assertThat(handler.isShuttingDown()).isFalse();
    }

    @Test
    @DisplayName("destroy - 容器不存在时安全完成并标记关闭")
    void destroy_whenContainerAbsent_completesSafely() {
        when(containerProvider.getIfAvailable()).thenReturn(null);
        handler.destroy();
        assertThat(handler.isShuttingDown()).isTrue();
        verify(container, never()).pause();
        verify(container, never()).stop();
    }

    @Test
    @DisplayName("destroy - 容器存在时调用 pause 与 stop")
    void destroy_whenContainerPresent_callsPauseAndStop() {
        when(containerProvider.getIfAvailable()).thenReturn(container);
        handler.destroy();
        verify(container).pause();
        verify(container).stop();
        assertThat(handler.isShuttingDown()).isTrue();
    }

    @Test
    @DisplayName("destroy - 重复调用只触发一次关闭流程")
    void destroy_calledTwice_onlyTriggersOnce() {
        when(containerProvider.getIfAvailable()).thenReturn(container);
        handler.destroy();
        handler.destroy();
        verify(container).pause();
        verify(container).stop();
        assertThat(handler.isShuttingDown()).isTrue();
    }

    // ===================== K2：在途消息收敛（core InFlightAware 契约） =====================

    @Test
    @DisplayName("K2 - 在途计数归零时提前退出 grace 等待，不睡满时间窗")
    void inFlightAware_countDrains_exitsBeforeGraceDeadline() {
        properties.setGracefulShutdownTimeoutMs(5_000L);
        when(containerProvider.getIfAvailable()).thenReturn(inFlightContainer);
        // 首轮读到 2 → 次轮读到 0：计数归零必须立即收敛
        when(inFlightContainer.getInFlightCount()).thenReturn(2, 0);

        long startNanos = System.nanoTime();
        handler.destroy();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        // 失败即红：若判据失效（core 契约不被识别 → 跳过等待，或计数从未被读取），本断言立即失败
        verify(inFlightContainer, times(2)).getInFlightCount();
        verify(inFlightContainer).pause();
        verify(inFlightContainer).stop();
        assertThat(elapsedMillis).as("计数归零必须提前退出，不得睡满 grace=5000ms").isLessThan(2_000L);
        assertThat(handler.isShuttingDown()).isTrue();
    }

    @Test
    @DisplayName("K2 - 在途计数恒大于 0 时等待到 grace 超时（硬上界生效）")
    void inFlightAware_countStuckAboveZero_waitsUntilGraceTimeout() {
        properties.setGracefulShutdownTimeoutMs(200L);
        when(containerProvider.getIfAvailable()).thenReturn(inFlightContainer);
        when(inFlightContainer.getInFlightCount()).thenReturn(3);

        long startNanos = System.nanoTime();
        handler.destroy();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        // 失败即红：若等待被跳过（判据失效），耗时趋近 0ms，下界断言立即失败
        assertThat(elapsedMillis)
                .as("计数恒 > 0 必须等待到 grace=200ms 超时")
                .isGreaterThanOrEqualTo(180L)
                .isLessThan(3_000L);
        verify(inFlightContainer, atLeast(2)).getInFlightCount();
        verify(inFlightContainer).pause();
        verify(inFlightContainer).stop();
        assertThat(handler.isShuttingDown()).isTrue();
    }

    @Test
    @DisplayName("K2 - 未实现 core InFlightAware 的容器跳过 grace 空转，直接 stop")
    void containerWithoutInFlightContract_skipsGraceWait() {
        properties.setGracefulShutdownTimeoutMs(5_000L);
        when(containerProvider.getIfAvailable()).thenReturn(container);

        long startNanos = System.nanoTime();
        handler.destroy();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        // 失败即红：若无判据容器也被写入 grace 等待，耗时会接近 5000ms
        assertThat(elapsedMillis).as("无判据时必须跳过 grace 空转").isLessThan(1_000L);
        verify(container).pause();
        verify(container).stop();
        assertThat(handler.isShuttingDown()).isTrue();
    }
}
