package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.interceptor.TraceContextConsumerInterceptor;
import io.github.streammq.adapter.redisson.interceptor.TraceContextProducerInterceptor;
import io.github.streammq.adapter.redisson.trace.RedisStreamMQTraceService;
import io.github.streammq.adapter.redisson.trace.RedisTraceCollector;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 追踪存储与查询自动装配（v1.0+）。
 *
 * <p>当 {@code streammq.trace.enabled=true} 且 {@code streammq.trace.storage=redis} 时：
 *
 * <ul>
 *   <li>注册 {@link RedisTraceCollector} 作为 {@link TraceCollector}，将追踪数据写入 Redis Stream
 *   <li>注册 {@link RedisStreamMQTraceService} 作为 {@link StreamMQTraceService}，提供追踪查询能力
 *   <li>注册追踪上下文拦截器，自动收集发送/消费事件
 * </ul>
 *
 * <p>本配置在 {@link StreamMQCoreAutoConfiguration} 之前加载， 确保 {@code RedisTraceCollector} 优先于 Noop/Slf4j
 * 收集器注册。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({RedissonClient.class, StreamMQTraceService.class})
@ConditionalOnProperty(prefix = "streammq.trace", name = "enabled", havingValue = "true")
@AutoConfigureBefore(StreamMQCoreAutoConfiguration.class)
@EnableConfigurationProperties(StreamMQProperties.class)
public class StreamMQTraceAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQTraceAutoConfiguration.class);

    /**
     * Redis 追踪收集器：将追踪记录写入 Redis Stream。
     *
     * <p>当 {@code streammq.trace.storage=redis} 时注册，覆盖默认的 Noop/Slf4j 收集器。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置属性
     * @return RedisTraceCollector 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceCollector.class)
    @ConditionalOnProperty(prefix = "streammq.trace", name = "storage", havingValue = "redis")
    public TraceCollector redisTraceCollector(
            RedissonClient redisson, StreamMQProperties properties) {
        LOG.info(
                "Using RedisTraceCollector (trace storage=redis, namespace={})",
                properties.getNamespace());
        return new RedisTraceCollector(redisson, properties.getNamespace());
    }

    /**
     * Redis 追踪查询服务：从 Redis Stream 查询追踪记录。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置属性
     * @return RedisStreamMQTraceService 实例
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQTraceService.class)
    @ConditionalOnProperty(prefix = "streammq.trace", name = "storage", havingValue = "redis")
    public StreamMQTraceService redisStreamMQTraceService(
            RedissonClient redisson, StreamMQProperties properties) {
        LOG.info(
                "Using RedisStreamMQTraceService (trace storage=redis, namespace={})",
                properties.getNamespace());
        return new RedisStreamMQTraceService(redisson, properties.getNamespace());
    }

    /**
     * 追踪上下文生产者拦截器：在 trace.enabled 时注册。
     *
     * @param traceCollector 追踪收集器
     * @return TraceContextProducerInterceptor 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceContextProducerInterceptor.class)
    public TraceContextProducerInterceptor streamMQTraceProducerInterceptor(
            TraceCollector traceCollector) {
        LOG.info("Using TraceContextProducerInterceptor (trace enabled)");
        return new TraceContextProducerInterceptor(traceCollector);
    }

    /**
     * 追踪上下文消费者拦截器：在 trace.enabled 时注册。
     *
     * @param traceCollector 追踪收集器
     * @return TraceContextConsumerInterceptor 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceContextConsumerInterceptor.class)
    public TraceContextConsumerInterceptor streamMQTraceConsumerInterceptor(
            TraceCollector traceCollector) {
        LOG.info("Using TraceContextConsumerInterceptor (trace enabled)");
        return new TraceContextConsumerInterceptor(traceCollector);
    }
}
