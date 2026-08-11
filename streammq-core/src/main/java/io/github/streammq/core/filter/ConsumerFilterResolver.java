package io.github.streammq.core.filter;

/**
 * 消费者过滤器解析器接口。
 *
 * <p>用于从容器（如 Spring）中获取 per-consumer 过滤器实例。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerFilterResolver {

  /**
   * 根据过滤器类获取过滤器实例。
   *
   * @param filterClass 过滤器类
   * @return 过滤器实例，可为 null
   */
  ConsumerFilter resolve(Class<? extends ConsumerFilter> filterClass);
}
