package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.interceptor.ConsumerInterceptor;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.ObjectProvider;

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
    public DefaultStreamMQListenerContainer streamMQListenerContainer(RedissonClient redisson,
                                                                       StreamMQListenerFactory consumerFactory,
                                                                       MessageConverter messageConverter,
                                                                       RetryPolicy retryPolicy,
                                                                       DlqFailureStrategy dlqFailureStrategy,
                                                                       DlqConfig dlqConfig,
                                                                       StreamMQProperties properties,
                                                                       ObjectProvider<ConsumerInterceptor> consumerInterceptorProvider,
                                                                       ObjectProvider<ConsumerFilter> consumerFilterProvider,
                                                                       ApplicationContext applicationContext) {
        String namespace = properties.getNamespace();
        LOG.info("Creating DefaultStreamMQListenerContainer: namespace={}, dlqFailureStrategy={}",
            namespace, dlqFailureStrategy.name());
        DefaultStreamMQListenerContainer container = new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, messageConverter, retryPolicy,
            dlqFailureStrategy, dlqConfig, namespace);

        container.setFilterResolver(filterClass -> {
            try {
                return applicationContext.getBean(filterClass);
            } catch (org.springframework.beans.factory.NoSuchBeanDefinitionException e) {
                return null;
            }
        });

        java.util.List<ConsumerInterceptor> interceptors = consumerInterceptorProvider.stream().toList();
        if (!interceptors.isEmpty()) {
            LOG.info("Registering {} ConsumerInterceptor(s): {}", interceptors.size(),
                interceptors.stream().map(ConsumerInterceptor::name).toList());
            container.addConsumerInterceptors(interceptors);
        }

        java.util.List<ConsumerFilter> filters = consumerFilterProvider.stream().toList();
        if (!filters.isEmpty()) {
            LOG.info("Registering {} ConsumerFilter(s): {}", filters.size(),
                filters.stream().map(ConsumerFilter::name).toList());
            container.addConsumerFilters(filters);
        }

        return container;
    }

    /**
     * Listener 注册器：在所有单例 Bean 初始化后扫描注解并注册 Listener。
     *
     * @param listenerContainer Listener 容器
     * @return 注册器
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQListenerRegistrar.class)
    public StreamMQListenerRegistrar streamMQListenerRegistrar(DefaultStreamMQListenerContainer listenerContainer) {
        return new StreamMQListenerRegistrar(listenerContainer);
    }

    /**
     * 容器生命周期：通过 SmartLifecycle 启动与停止 Listener 容器。
     *
     * @param listenerContainer Listener 容器
     * @return 生命周期包装
     */
    @Bean
    @ConditionalOnMissingBean(name = "streamMQListenerContainerLifecycle")
    public StreamMQListenerContainerLifecycle streamMQListenerContainerLifecycle(
            DefaultStreamMQListenerContainer listenerContainer) {
        return new StreamMQListenerContainerLifecycle(listenerContainer);
    }
}
