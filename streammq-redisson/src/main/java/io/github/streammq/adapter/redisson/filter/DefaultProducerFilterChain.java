/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.core.filter.ProducerFilter;
import io.github.streammq.core.filter.ProducerFilterChain;
import io.github.streammq.core.message.Message;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 生产者过滤器链默认实现（策略类）。
 *
 * <p>管理全局 {@link ProducerFilter} 列表，并按 {@link ProducerFilter#order()} 升序执行。 任一过滤器返回 false 则消息被阻止发送。
 *
 * <p>线程安全：使用 {@link CopyOnWriteArrayList}，支持运行时动态添加过滤器。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultProducerFilterChain implements ProducerFilterChain {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultProducerFilterChain.class);

    @Getter private final List<ProducerFilter> filters = new CopyOnWriteArrayList<>();

    @Override
    public void addFilter(ProducerFilter filter) {
        Objects.requireNonNull(filter, "filter");
        int insertIndex = 0;
        for (ProducerFilter existing : filters) {
            if (existing.order() <= filter.order()) {
                insertIndex++;
            } else {
                break;
            }
        }
        filters.add(insertIndex, filter);
        LOG.debug("Added ProducerFilter: {} (order={})", filter.name(), filter.order());
    }

    @Override
    public void addFilters(Collection<ProducerFilter> filters) {
        if (Objects.nonNull(filters)) {
            for (ProducerFilter filter : filters) {
                addFilter(filter);
            }
        }
    }

    @Override
    public boolean accept(Message<?> message) {
        Objects.requireNonNull(message, "message");
        for (ProducerFilter filter : filters) {
            try {
                if (!filter.accept(message)) {
                    LOG.debug(
                            "ProducerFilter {} rejected message: topic={}, tag={}",
                            filter.name(),
                            message.getTopic(),
                            message.getTag());
                    return false;
                }
            } catch (RuntimeException ex) {
                // 与消费侧同理：求值异常必须向上传播为发送失败，
                // 绝不允许"过滤器坏了"被降级为"消息照发"
                throw new IllegalStateException(
                        "ProducerFilter evaluation failed: " + filter.name(), ex);
            }
        }
        return true;
    }
}
