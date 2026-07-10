package io.github.streammq.core.filter;

import io.github.streammq.core.message.Message;

import java.util.Collection;
import java.util.List;

/**
 * 生产者过滤器链策略接口。
 *
 * <p>管理全局 {@link ProducerFilter} 列表，并按 {@link ProducerFilter#order()} 升序执行。
 * 任一过滤器返回 false 则消息被阻止发送。
 *
 * <p>实现可通过 {@code DefaultStreamMessageTemplate} 构造器注入，
 * 以自定义过滤器链行为。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ProducerFilterChain {

    /**
     * 添加单个过滤器（按 {@link ProducerFilter#order()} 升序插入）。
     *
     * @param filter 过滤器实例
     */
    void addFilter(ProducerFilter filter);

    /**
     * 批量添加过滤器。
     *
     * @param filters 过滤器集合
     */
    void addFilters(Collection<ProducerFilter> filters);

    /**
     * 执行过滤器链。
     *
     * @param message 待过滤消息
     * @return true 全部通过（不过滤），false 任一过滤器拒绝（阻止发送）
     */
    boolean accept(Message<?> message);

    /**
     * 返回当前已注册的过滤器列表（按 order 升序）。
     *
     * @return 过滤器列表
     */
    List<ProducerFilter> getFilters();
}