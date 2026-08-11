package io.github.streammq.spring.cloud.stream.binder;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * StreamMQ 扩展绑定属性，绑定前缀 {@code spring.cloud.stream.streammq.bindings}。
 *
 * <p>继承 {@link AbstractExtendedBindingProperties}，由 Spring Cloud Stream 框架调用：
 *
 * <ul>
 *   <li>{@link #getExtendedConsumerProperties(String)} - 获取指定 binding 的消费者扩展属性
 *   <li>{@link #getExtendedProducerProperties(String)} - 获取指定 binding 的生产者扩展属性
 * </ul>
 *
 * <p>全局默认值前缀 {@code spring.cloud.stream.streammq.default.consumer.*} / {@code
 * spring.cloud.stream.streammq.default.producer.*}， 对应配置会自动绑定到新建的 {@link StreamMQBindingProperties}
 * 实例上。
 *
 * <p>该类由 {@link StreamMQBinderConfiguration} 通过 {@code @EnableConfigurationProperties} 注册为 Bean，
 * 并通过 {@link StreamMQMessageBinder#setExtendedBindingProperties} 注入到 Binder 中。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ConfigurationProperties("spring.cloud.stream.streammq.bindings")
public class StreamMQExtendedBindingProperties
    extends AbstractExtendedBindingProperties<
        StreamMQConsumerProperties, StreamMQProducerProperties, StreamMQBindingProperties> {

  /** 全局默认值前缀 */
  private static final String DEFAULTS_PREFIX = "spring.cloud.stream.streammq.default";

  @Override
  public String getDefaultsPrefix() {
    return DEFAULTS_PREFIX;
  }

  @Override
  public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
    return StreamMQBindingProperties.class;
  }
}
