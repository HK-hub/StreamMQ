/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.filter.ConsumerFilterResolver;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ConsumerFilterResolver} 默认实现：反射调用无参构造实例化过滤器。
 *
 * <p>容器（{@code DefaultStreamMQListenerContainer}）的出厂默认解析器。 在 Spring 环境中，可通过注册自定义 {@code
 * ConsumerFilterResolver} Bean（从 ApplicationContext 解析单例）替换本实现， 使 per-consumer 过滤器支持依赖注入。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class ReflectiveConsumerFilterResolver implements ConsumerFilterResolver {

    private static final Logger LOG =
            LoggerFactory.getLogger(ReflectiveConsumerFilterResolver.class);

    @Override
    public ConsumerFilter resolve(Class<? extends ConsumerFilter> filterClass) {
        if (Objects.isNull(filterClass) || filterClass == ConsumerFilter.class) {
            return null;
        }
        try {
            return filterClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException ex) {
            LOG.warn(
                    "Failed to instantiate consumer filter {}: {}",
                    filterClass.getName(),
                    ex.getMessage());
            return null;
        }
    }
}
