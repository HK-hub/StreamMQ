package io.github.streammq.adapter.redisson.filter;

import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.filter.ConsumerFilterChain;
import io.github.streammq.core.message.Message;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费者过滤器链默认实现（策略类）。
 *
 * <p>管理全局 {@link ConsumerFilter} 列表，并按 {@link ConsumerFilter#order()} 升序执行。 任一过滤器返回 false 则消息被跳过。
 *
 * <p>线程安全：使用 {@link CopyOnWriteArrayList}，支持运行时动态添加过滤器。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultConsumerFilterChain implements ConsumerFilterChain {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultConsumerFilterChain.class);

  @Getter private final List<ConsumerFilter> filters = new CopyOnWriteArrayList<>();

  @Override
  public void addFilter(ConsumerFilter filter) {
    Objects.requireNonNull(filter, "filter");
    int order = filter.order();
    if (order < -1000 || order > 1000) {
      LOG.warn(
          "ConsumerFilter {} order={} is outside recommended range [-1000, 1000], "
              + "may cause unexpected ordering behavior",
          filter.name(),
          order);
    }
    int insertIndex = 0;
    for (ConsumerFilter existing : filters) {
      if (existing.order() <= filter.order()) {
        insertIndex++;
      } else {
        break;
      }
    }
    filters.add(insertIndex, filter);
    LOG.debug("Added ConsumerFilter: {} (order={})", filter.name(), filter.order());
  }

  @Override
  public void addFilters(Collection<ConsumerFilter> filters) {
    if (Objects.nonNull(filters)) {
      for (ConsumerFilter filter : filters) {
        addFilter(filter);
      }
    }
  }

  @Override
  public boolean accept(Message<?> message) {
    Objects.requireNonNull(message, "message");
    for (ConsumerFilter filter : filters) {
      try {
        if (!filter.accept(message)) {
          LOG.debug(
              "ConsumerFilter {} rejected message: topic={}, tag={}",
              filter.name(),
              message.getTopic(),
              message.getTag());
          return false;
        }
      } catch (RuntimeException ex) {
        LOG.warn("ConsumerFilter {} threw exception: {}", filter.name(), ex.getMessage(), ex);
      }
    }
    return true;
  }
}
