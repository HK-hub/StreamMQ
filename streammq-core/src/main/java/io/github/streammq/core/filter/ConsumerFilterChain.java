package io.github.streammq.core.filter;

import io.github.streammq.core.message.Message;
import java.util.Collection;
import java.util.List;

/**
 * 消费者过滤器链策略接口。
 *
 * <p>管理全局 {@link ConsumerFilter} 列表，并按 {@link ConsumerFilter#order()} 升序执行。 任一过滤器返回 false 则消息被跳过。
 *
 * <p>实现可通过 {@code DefaultStreamMQListenerContainer} 构造器注入， 以自定义过滤器链行为。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerFilterChain {

  /**
   * 添加单个过滤器（按 {@link ConsumerFilter#order()} 升序插入）。
   *
   * @param filter 过滤器实例
   */
  void addFilter(ConsumerFilter filter);

  /**
   * 批量添加过滤器。
   *
   * @param filters 过滤器集合
   */
  void addFilters(Collection<ConsumerFilter> filters);

  /**
   * 执行过滤器链。
   *
   * @param message 待过滤消息
   * @return true 全部通过（不过滤），false 任一过滤器拒绝（跳过）
   */
  boolean accept(Message<?> message);

  /**
   * 返回当前已注册的过滤器列表（按 order 升序）。
   *
   * @return 过滤器列表
   */
  List<ConsumerFilter> getFilters();
}
