/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.annotation.EnableStreamMQ;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * StreamMQ 自动装配主入口。
 *
 * <p>当 {@code streammq.enabled=true}（默认）且 classpath 中存在 {@link RedissonClient} 与 {@link
 * StreamMessageTemplate} 时触发装配。
 *
 * <p>装配链：
 *
 * <ol>
 *   <li>{@link StreamMQTraceAutoConfiguration} - 追踪存储 / 查询服务（可选）
 *   <li>{@link StreamMQCoreAutoConfiguration} - 序列化器 / 转换器 / 工厂 / 模板 / 服务
 *   <li>{@link StreamMQSchedulerAutoConfiguration} - 重试 / 延时 / 事务回查 调度器
 *   <li>{@link StreamMQListenerContainerAutoConfiguration} - Listener 容器 + 注解扫描 + SmartLifecycle
 *   <li>{@link StreamMQHealthAutoConfiguration} - Actuator HealthIndicator（可选）
 * </ol>
 *
 * <p>注意：{@link StreamMQMetricsAutoConfiguration} <b>不通过 {@code @Import} 引入</b>。 它依赖 {@code
 * MeterRegistry} Bean，必须登记在 {@code
 * META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports} 中并配合
 * {@code @AutoConfigureAfter(MetricsAutoConfiguration)} 才能在 Boot 注册 {@code MeterRegistry} 之后评估
 * {@code @ConditionalOnBean(MeterRegistry)}； 若经 {@code @Import} 嵌套引入则不参与 .imports 排序， 会因装配过早而静默丢失
 * {@link io.github.streammq.core.metrics.StreamMQMetrics} Bean。
 *
 * <p>触发方式：
 *
 * <ul>
 *   <li>Starter 在 classpath 时经 {@code AutoConfiguration.imports} 自动生效（推荐，无需任何注解）
 *   <li>{@link EnableStreamMQ} 为<b>空标记注解</b>（不含 {@code @Import}），不参与装配决策——仅用于向 StreamMQ
 *       团队声明"本应用有意使用 StreamMQ"；装配由自动配置独立完成
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = StreamMQSpringConstants.PROP_PREFIX,
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
@ConditionalOnClass({RedissonClient.class, StreamMessageTemplate.class})
@Import({
    StreamMQTraceAutoConfiguration.class,
    StreamMQCoreAutoConfiguration.class,
    StreamMQSchedulerAutoConfiguration.class,
    StreamMQListenerContainerAutoConfiguration.class,
    StreamMQHealthAutoConfiguration.class
})
public class StreamMQAutoConfiguration {}
