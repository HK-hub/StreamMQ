/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.core.converter.MessageConverter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RedissonClient;

/**
 * {@link RedissonStreamProducer#close()} 执行器所有权回归测试。
 *
 * <p>历史缺陷：注入外部执行器后 close() 仍无条件 awaitTermination（最长阻塞
 * DEFAULT_AWAIT_TERMINATION_SECONDS，宿主线程被拖住）并对注入池调用 shutdownNow——误杀 Spring 等提供方共享的线程池。修复后注入路径仅记录
 * debug 日志立即返回。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("RedissonStreamProducer close 执行器所有权测试")
class RedissonStreamProducerCloseTest {

    private final RedissonClient redisson = mock(RedissonClient.class);
    private final MessageConverter converter =
            new DefaultMessageConverter(
                    new io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer<>());

    @Test
    @DisplayName("注入执行器：close 不调用 shutdown/awaitTermination/shutdownNow 且立即返回")
    void closeWithInjectedExecutorDoesNotShutdown() throws Exception {
        RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, "ns", "g", converter, 3000L, 0, 0, 0);
        ExecutorService injected = Mockito.mock(ExecutorService.class);
        producer.setAsyncExecutor(injected);

        long start = System.nanoTime();
        producer.close();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        verify(injected, never()).shutdown();
        verify(injected, never()).shutdownNow();
        verify(injected, never())
                .awaitTermination(org.mockito.ArgumentMatchers.anyLong(), Mockito.any());
        // 注入路径不得有任何等待：远小于内部 awaitTermination 的秒级预算
        assertThat(elapsedMillis).isLessThan(100L);
    }

    @Test
    @DisplayName("内部执行器：close 走 shutdown 关闭序列")
    void closeWithOwnedExecutorShutsDown() throws Exception {
        RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, "ns", "g", converter, 3000L, 0, 0, 0);
        ExecutorService owned = producer.getAsyncExecutor();
        assertThat(owned).isNotNull();
        // 内部执行器为真实对象（非 mock），改用可观测行为断言：
        long start = System.nanoTime();

        producer.close();

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(owned.isShutdown()).isTrue();
        // 空载虚拟线程池：awaitTermination 立即返回，整体关闭远小于秒级预算（证明未走注入路径的空等）
        assertThat(elapsedMillis).isLessThan(1000L);
    }

    @Test
    @DisplayName("重复 close 幂等：第二次不再次触碰执行器")
    void closeIsIdempotent() {
        RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, "ns", "g", converter, 3000L, 0, 0, 0);
        ExecutorService injected = Mockito.mock(ExecutorService.class);
        producer.setAsyncExecutor(injected);

        producer.close();
        producer.close();

        verify(injected, never()).shutdown();
        verify(injected, never()).shutdownNow();
    }
}
