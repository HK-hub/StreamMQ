package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.template.StreamMessageTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ Binder Spring Boot 自动装配类。
 *
 * <p>装配条件：
 *
 * <ul>
 *   <li>classpath 中存在 {@link StreamMessageTemplate}（表示 StreamMQ 已被启用）
 *   <li>{@code spring.cloud.stream.streammq.binder.enabled=true}（默认 true）
 * </ul>
 *
 * <p>装配内容：
 *
 * <ol>
 *   <li>{@link StreamMQMessageBinder} - 注册为 Spring Bean，由 Spring Cloud Stream 的 {@code
 *       BinderFactory} 自动发现并使用
 *   <li>{@link StreamMQBinderHealthIndicator} - 健康检查（仅当 Actuator 在 classpath 时生效）
 *   <li>{@link StreamMQExtendedBindingProperties} - per-binding 扩展属性
 * </ol>
 *
 * <p>用户在 {@code application.yml} 中通过如下配置指定使用 StreamMQ Binder：
 *
 * <pre>{@code
 * spring:
 *   cloud:
 *     stream:
 *       default-binder: streammq
 *       streammq:
 *         binder:
 *           namespace: streammq
 *           send-timeout: 3000
 *         bindings:
 *           myBinding-in-0:
 *             consumer:
 *               selectorExpression: "tag1 || tag2"
 *               shardCount: 8
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(StreamMessageTemplate.class)
@ConditionalOnProperty(
    prefix = "spring.cloud.stream.streammq.binder",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties({
  StreamMQBinderProperties.class,
  StreamMQExtendedBindingProperties.class
})
public class StreamMQBinderConfiguration {

  /**
   * 创建 StreamMQ Binder Bean。
   *
   * <p>注册为 {@link org.springframework.cloud.stream.binder.Binder} 类型的 Bean， Spring Cloud Stream
   * 会自动发现并使用。 当应用中存在多个 Binder 时，通过 {@code spring.cloud.stream.default-binder=streammq} 指定。
   *
   * @param template StreamMQ 消息模板
   * @param listenerContainer StreamMQ Listener 容器
   * @param binderProperties Binder 全局属性
   * @param extendedBindingProperties per-binding 扩展属性
   * @return StreamMQ Binder 实例
   */
  @Bean
  @ConditionalOnMissingBean(StreamMQMessageBinder.class)
  public StreamMQMessageBinder streamMQMessageBinder(
      StreamMessageTemplate template,
      StreamMQListenerContainer listenerContainer,
      StreamMQBinderProperties binderProperties,
      StreamMQExtendedBindingProperties extendedBindingProperties) {
    log.info(
        "创建 StreamMQMessageBinder: namespace={}, sendTimeout={}, retryTimes={}",
        binderProperties.getNamespace(),
        binderProperties.getSendTimeout(),
        binderProperties.getRetryTimes());
    StreamMQMessageBinder binder =
        new StreamMQMessageBinder(template, listenerContainer, binderProperties);
    binder.setExtendedBindingProperties(extendedBindingProperties);
    return binder;
  }

  /**
   * 创建 StreamMQ Binder 健康检查 Bean（仅当 Actuator 在 classpath 时生效）。
   *
   * @param listenerContainer StreamMQ Listener 容器
   * @return 健康检查指标
   */
  @Bean
  @ConditionalOnMissingBean(name = "streamMQBinderHealthIndicator")
  @ConditionalOnClass(name = "org.springframework.boot.actuate.health.AbstractHealthIndicator")
  public org.springframework.boot.actuate.health.HealthIndicator streamMQBinderHealthIndicator(
      StreamMQListenerContainer listenerContainer) {
    log.info("创建 StreamMQBinderHealthIndicator");
    return new StreamMQBinderHealthIndicator(listenerContainer);
  }
}
