/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.filter;

import io.github.streammq.core.message.Message;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 日志生产者过滤器（默认实现）：在 {@link ProducerFilter} 命中时记录 INFO 级别日志， 所有消息均放行（返回 {@code
 * true}），仅用于审计与问题排查。
 *
 * <p>本类是 {@link ProducerFilter} SPI 的默认空操作+日志记录实现， 业务方可注册自定义 {@link ProducerFilter} Bean
 * 覆盖本默认。 若同时存在多个同类型 Bean，Spring 依赖 {@link org.springframework.core.annotation.Order} / {@link
 * ProducerFilter#order()} 决定执行顺序。
 *
 * <p>日志格式：
 *
 * <ul>
 *   <li>{@code accept} 命中时输出：{@code [ProducerFilter] accept topic={}, tag={}, keys={}}
 * </ul>
 *
 * <p>执行顺序为 0，与 {@link ProducerFilter} 默认顺序一致；自定义过滤器可按需调整。
 *
 * <p>线程安全：本类为无状态单例，可安全在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class LoggingProducerFilter implements ProducerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerFilter.class);

    @Override
    public boolean accept(Message<?> message) {
        Objects.requireNonNull(message, "message");
        LOG.info(
                "[ProducerFilter] accept topic={}, tag={}, keys={}",
                message.getTopic(),
                message.getTag(),
                message.getKeys());
        return true;
    }

    @Override
    public String name() {
        return "logging-producer-filter";
    }
}
