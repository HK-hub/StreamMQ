/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.StreamMQListenerContainer;
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
 */
@DisplayName("优雅关闭处理器测试")
@ExtendWith(MockitoExtension.class)
class GracefulShutdownHandlerTest {

    @Mock private ObjectProvider<StreamMQListenerContainer> containerProvider;

    @Mock private StreamMQListenerContainer container;

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
}
