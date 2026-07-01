package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.annotation.EnableStreamMq;
import io.github.streammq.core.template.StreamMqTemplate;
import org.redisson.api.RedissonClient;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * StreamMQ 自动装配主入口。
 *
 * <p>当 {@code streammq.enabled=true}（默认）且 classpath 中存在
 * {@link RedissonClient} 与 {@link StreamMqTemplate} 时触发装配。
 *
 * <p>装配链：
 * <ol>
 *   <li>{@link StreamMqCoreAutoConfiguration} - 序列化器 / 转换器 / 工厂 / 模板 / @StreamMqProducer 注入</li>
 *   <li>{@link StreamMqSchedulerAutoConfiguration} - 重试 / 延时 / 事务回查 调度器</li>
 *   <li>{@link StreamMqListenerContainerAutoConfiguration} - Listener 容器 + 注解扫描 + SmartLifecycle</li>
 *   <li>{@link StreamMqHealthAutoConfiguration} - Actuator HealthIndicator（可选）</li>
 * </ol>
 *
 * <p>触发方式：
 * <ul>
 *   <li>Starter 在 classpath 时自动生效（推荐）</li>
 *   <li>或通过 {@link EnableStreamMq} 注解显式开启（向后兼容）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({RedissonClient.class, StreamMqTemplate.class})
@Import({
    StreamMqCoreAutoConfiguration.class,
    StreamMqSchedulerAutoConfiguration.class,
    StreamMqListenerContainerAutoConfiguration.class,
    StreamMqHealthAutoConfiguration.class
})
public class StreamMqAutoConfiguration {

}
