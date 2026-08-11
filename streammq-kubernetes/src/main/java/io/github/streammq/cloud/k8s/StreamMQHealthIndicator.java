package io.github.streammq.cloud.k8s;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * StreamMQ 健康指标，集成 Spring Boot Actuator 暴露健康状态。
 *
 * <p>由 {@link CloudK8sAutoConfiguration} 通过 {@code @Import} 装配， 启用条件由 AutoConfiguration 的
 * {@code @ConditionalOnClass(StreamMQListenerContainer.class)} 统一控制。通过 {@code ObjectProvider}
 * 可选注入容器，避免对 {@code streammq-redisson} 的强依赖。
 *
 * <p>暴露的健康信息：
 *
 * <ul>
 *   <li>状态：容器运行中为 UP，否则为 DOWN
 *   <li>activeConsumers：当前已注册的消费者数量
 *   <li>running：容器是否正在运行
 * </ul>
 *
 * <p>当 {@link StreamMQListenerContainer} 不存在时，健康状态为 DOWN 并标注原因。
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
public class StreamMQHealthIndicator implements HealthIndicator {

  private final ObjectProvider<StreamMQListenerContainer> containerProvider;

  /**
   * 构造健康指标。
   *
   * @param containerProvider 监听器容器的可选注入提供者
   */
  public StreamMQHealthIndicator(ObjectProvider<StreamMQListenerContainer> containerProvider) {
    this.containerProvider = Objects.requireNonNull(containerProvider, "containerProvider");
  }

  @Override
  public Health health() {
    StreamMQListenerContainer container = containerProvider.getIfAvailable();
    if (Objects.isNull(container)) {
      return Health.down().withDetail("reason", "StreamMQListenerContainer not available").build();
    }
    boolean running = container.isRunning();
    int activeConsumers = container.getConsumers().size();
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("activeConsumers", activeConsumers);
    details.put("running", running);
    if (running) {
      return Health.up().withDetails(details).build();
    }
    return Health.down().withDetails(details).build();
  }
}
