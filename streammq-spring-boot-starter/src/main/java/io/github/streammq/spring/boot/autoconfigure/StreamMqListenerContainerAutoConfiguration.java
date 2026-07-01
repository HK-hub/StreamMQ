package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMqListenerContainer;
import io.github.streammq.core.consumer.StreamMqConsumerFactory;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.RetryPolicy;
import io.github.streammq.spring.boot.properties.StreamMqProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Listener 容器自动装配：注册 {@link DefaultStreamMqListenerContainer}、
 * {@link StreamMqListenerRegistrar} 与 {@link StreamMqListenerContainerLifecycle}。
 *
 * <p>装配条件：
 * <ul>
 *   <li>{@code streammq.enabled=true}</li>
 *   <li>classpath 存在 {@link DefaultStreamMqListenerContainer} 与 {@link RedissonClient}</li>
 *   <li>存在已注册的 {@link StreamMqConsumerFactory} Bean</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({DefaultStreamMqListenerContainer.class, RedissonClient.class})
@ConditionalOnBean(StreamMqConsumerFactory.class)
public class StreamMqListenerContainerAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqListenerContainerAutoConfiguration.class);

    /**
     * 默认 Listener 容器。
     *
     * @param redisson Redisson 客户端
     * @param consumerFactory 消费者工厂
     * @param messageConverter 消息转换器
     * @param retryPolicy 重试策略
     * @param properties 配置
     * @return Listener 容器
     */
    @Bean
    @ConditionalOnMissingBean(DefaultStreamMqListenerContainer.class)
    public DefaultStreamMqListenerContainer streamMqListenerContainer(RedissonClient redisson,
                                                                       StreamMqConsumerFactory consumerFactory,
                                                                       MessageConverter messageConverter,
                                                                       RetryPolicy retryPolicy,
                                                                       StreamMqProperties properties) {
        String namespace = properties.getNamespace();
        LOG.info("Creating DefaultStreamMqListenerContainer: namespace={}", namespace);
        return new DefaultStreamMqListenerContainer(
            redisson, consumerFactory, messageConverter, retryPolicy, namespace);
    }

    /**
     * Listener 注册器：在所有单例 Bean 初始化后扫描注解并注册 Listener。
     *
     * @param listenerContainer Listener 容器
     * @return 注册器
     */
    @Bean
    @ConditionalOnMissingBean(StreamMqListenerRegistrar.class)
    public StreamMqListenerRegistrar streamMqListenerRegistrar(DefaultStreamMqListenerContainer listenerContainer) {
        return new StreamMqListenerRegistrar(listenerContainer);
    }

    /**
     * 容器生命周期：通过 SmartLifecycle 启动与停止 Listener 容器。
     *
     * @param listenerContainer Listener 容器
     * @return 生命周期包装
     */
    @Bean
    @ConditionalOnMissingBean(name = "streamMqListenerContainerLifecycle")
    public StreamMqListenerContainerLifecycle streamMqListenerContainerLifecycle(
            DefaultStreamMqListenerContainer listenerContainer) {
        return new StreamMqListenerContainerLifecycle(listenerContainer);
    }
}
