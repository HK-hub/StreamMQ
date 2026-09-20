/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.lock.RedissonOrderlyShardLockManager;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.StaticApplicationContext;

/**
 * Listener 容器装配的"属性接线"回归测试（R6-S8，失败即红）。
 *
 * <p>回归背景：顺序消费分片锁租约（R1-9）此前只有容器 API（{@code setOrderlyShardLockLeaseMillis}），没有配置面——用户无法通过 {@code
 * application.yml} 打开有限租约。 现新增 {@code streammq.consumer.orderly-shard-lock-lease-millis}
 * 并在装配时注入容器（以及容器的 {@link RedissonOrderlyShardLockManager}）。
 */
@DisplayName("Listener 容器属性接线测试（S8）")
class StreamMQListenerContainerWiringTest {

    @Test
    @DisplayName("orderly-shard-lock-lease-millis 属性可绑定")
    void propertyBinds() {
        new ApplicationContextRunner()
                .withUserConfiguration(PropertiesConfig.class)
                .withPropertyValues("streammq.consumer.orderly-shard-lock-lease-millis=8000")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(
                                            context.getBean(StreamMQProperties.class)
                                                    .getConsumer()
                                                    .getOrderlyShardLockLeaseMillis())
                                    .isEqualTo(8000L);
                        });
    }

    @Test
    @DisplayName("producer.compression-codec 属性可绑定（S3 的配置名精确匹配入口）")
    void compressionCodecPropertyBinds() {
        new ApplicationContextRunner()
                .withUserConfiguration(PropertiesConfig.class)
                .withPropertyValues(
                        "streammq.producer.compression-codec=zstd",
                        "streammq.admin.list-page-size=500")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            StreamMQProperties properties =
                                    context.getBean(StreamMQProperties.class);
                            assertThat(properties.getProducer().getCompressionCodec())
                                    .isEqualTo("zstd");
                            assertThat(properties.getAdmin().getListPageSize()).isEqualTo(500);
                        });
    }

    @Test
    @DisplayName("装配时把配置值注入容器与其分片锁管理器（>0 = 有限租约）")
    void configuredLease_isAppliedToContainerAndLockManager() throws Exception {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getConsumer().setOrderlyShardLockLeaseMillis(8000L);

        DefaultStreamMQListenerContainer container = newContainer(properties);

        assertThat(lockManagerLeaseOf(container)).isEqualTo(8000L);
    }

    @Test
    @DisplayName("默认 0 = 看门狗续期 + 严格有序（不改变既有语义）")
    void defaultLease_keepsWatchdogSemantics() throws Exception {
        StreamMQProperties properties = new StreamMQProperties();
        assertThat(properties.getConsumer().getOrderlyShardLockLeaseMillis()).isEqualTo(0L);

        DefaultStreamMQListenerContainer container = newContainer(properties);

        assertThat(lockManagerLeaseOf(container)).isEqualTo(0L);
    }

    /** 用真实容器 + mock 依赖调用装配方法，验证"装配调用"确实落到容器上（不启动 Spring 上下文）。 */
    private static DefaultStreamMQListenerContainer newContainer(StreamMQProperties properties) {
        return new StreamMQListenerContainerAutoConfiguration()
                .streamMQListenerContainer(
                        mock(RedissonClient.class),
                        mock(StreamMQListenerFactory.class),
                        mock(MessageConverter.class),
                        mock(RetryPolicy.class),
                        mock(DlqFailureStrategy.class),
                        DlqConfig.builder().build(),
                        properties,
                        providerOf(),
                        providerOf(),
                        providerOf(),
                        providerOf(),
                        providerOf(Executors.newVirtualThreadPerTaskExecutor()),
                        providerOf(),
                        new StaticApplicationContext());
    }

    /** 读取容器内部的分片锁管理器租约（容器未暴露该读回接口，测试用反射）。 */
    private static long lockManagerLeaseOf(DefaultStreamMQListenerContainer container)
            throws Exception {
        Field field = DefaultStreamMQListenerContainer.class.getDeclaredField("shardLockManager");
        field.setAccessible(true);
        Object manager = field.get(container);
        assertThat(manager).isInstanceOf(RedissonOrderlyShardLockManager.class);
        return ((RedissonOrderlyShardLockManager) manager).getLeaseMillis();
    }

    @SafeVarargs
    private static <T> ObjectProvider<T> providerOf(T... beans) {
        @SuppressWarnings("unchecked")
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        when(provider.orderedStream()).thenAnswer(invocation -> Arrays.stream(beans));
        when(provider.stream()).thenAnswer(invocation -> Arrays.stream(beans));
        return provider;
    }

    /** 仅启用配置属性绑定（不触发任何 StreamMQ 组件装配）。 */
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(StreamMQProperties.class)
    static class PropertiesConfig {}
}
