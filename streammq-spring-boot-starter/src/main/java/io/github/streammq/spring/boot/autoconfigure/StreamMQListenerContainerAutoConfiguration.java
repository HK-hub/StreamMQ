package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.RetryPolicy;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
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
 * Listener 容器自动装配：注册 {@link DefaultStreamMQListenerContainer}、
 * {@link StreamMQListenerRegistrar} 与 {@link StreamMQListenerContainerLifecycle}。
 *
 * <p>装配条件：
 * <ul>
 *   <li>{@code streammq.enabled=true}</li>
 *   <li>classpath 存在 {@link DefaultStreamMQListenerContainer} 与 {@link RedissonClient}</li>
 *   <li>存在已注册的 {@link StreamMQListenerFactory} Bean</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({DefaultStreamMQListenerContainer.class, RedissonClient.class})
@ConditionalOnBean(StreamMQListenerFactory.class)
public class StreamMQListenerContainerAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQListenerContainerAutoConfiguration.class);

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
    @ConditionalOnMissingBean(DefaultStreamMQListenerContainer.class)
    public DefaultStreamMQListenerContainer streamMqListenerContainer(RedissonClient redisson,
                                                                      StreamMQListenerFactory consumerFactory,
                                                                      MessageConverter messageConverter,
                                                                      RetryPolicy retryPolicy,
                                                                      StreamMQProperties properties) {
        String namespace = properties.getNamespace();
        LOG.info("Creating DefaultStreamMqListenerContainer: namespace={}", namespace);
        return new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, messageConverter, retryPolicy, namespace);
    }

    /**
     * Listener 注册器：在所有单例 Bean 初始化后扫描注解并注册 Listener。
     *
     * @param listenerContainer Listener 容器
     * @return 注册器
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQListenerRegistrar.class)
    public StreamMQListenerRegistrar streamMqListenerRegistrar(DefaultStreamMQListenerContainer listenerContainer) {
        return new StreamMQListenerRegistrar(listenerContainer);
    }

    /**
     * 容器生命周期：通过 SmartLifecycle 启动与停止 Listener 容器。
     *
     * @param listenerContainer Listener 容器
     * @return 生命周期包装
     */
    @Bean
    @ConditionalOnMissingBean(name = "streamMqListenerContainerLifecycle")
    public StreamMQListenerContainerLifecycle streamMqListenerContainerLifecycle(
            DefaultStreamMQListenerContainer listenerContainer) {
        return new StreamMQListenerContainerLifecycle(listenerContainer);
    }
}
