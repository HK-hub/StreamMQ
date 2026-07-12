package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.interceptor.TraceContextConsumerInterceptor;
import io.github.streammq.adapter.redisson.interceptor.TraceContextProducerInterceptor;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.adapter.redisson.trace.NoopTraceCollector;
import io.github.streammq.adapter.redisson.trace.Slf4jTraceCollector;
import io.github.streammq.adapter.redisson.compression.GzipCompressionCodec;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.service.DefaultStreamMessageService;
import io.github.streammq.core.service.StreamMessageService;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ 核心自动装配：注册 Serializer / Converter / RetryPolicy / ProducerFactory /
 * ConsumerFactory / Template 等基础 Bean。
 *
 * <p>装配条件：
 * <ul>
 *   <li>{@code streammq.enabled=true}（默认 true）</li>
 *   <li>classpath 存在 {@link RedissonClient} 与 {@link StreamMessageTemplate}</li>
 *   <li>存在已注册的 {@link RedissonClient} Bean（通常来自 {@code redisson-spring-boot-starter}）；
 *       若用户未注册，本类提供指向 {@code localhost:6379} 的兜底实例（仅用于开发环境）</li>
 * </ul>
 *
 * <p>所有核心 Bean 均标注 {@code @ConditionalOnMissingBean}，用户可在自定义配置类中覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({RedissonClient.class, StreamMessageTemplate.class})
@EnableConfigurationProperties(StreamMQProperties.class)
public class StreamMQCoreAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQCoreAutoConfiguration.class);

    /**
     * 开发环境兜底：当用户未注册 RedissonClient Bean 时，
     * 创建一个指向 localhost:6379 的默认实例。
     *
     * <p><b>生产环境强烈建议</b>引入 {@code redisson-spring-boot-starter}
     * 或手动注册 {@link RedissonClient} Bean 以使用正确的 Redis 集群配置。
     *
     * @return RedissonClient 实例
     */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(RedissonClient.class)
    public RedissonClient redissonClient() {
        LOG.warn("No RedissonClient bean found, creating default localhost:6379 instance. " +
            "For production, please use redisson-spring-boot-starter or register your own RedissonClient bean.");
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        return Redisson.create(config);
    }

    /**
     * 默认序列化器：JacksonJsonSerializer。
     *
     * <p>若配置 {@code streammq.producer.serializer} 指定其他实现类，将通过反射加载。
     *
     * @param properties 配置
     * @return 序列化器
     */
    @Bean
    @ConditionalOnMissingBean(MessageSerializer.class)
    public MessageSerializer<?> streamMQMessageSerializer(StreamMQProperties properties) {
        String className = properties.getProducer().getSerializer();
        if (StringUtils.isEmpty(className)
            || JacksonJsonSerializer.class.getName().equals(className)) {
            LOG.info("Using default JacksonJsonSerializer");
            return new JacksonJsonSerializer<>();
        }
        return instantiate(className, MessageSerializer.class);
    }

    /**
     * 默认消息转换器：DefaultMessageConverter。
     *
     * @param serializer 序列化器
     * @param compressionCodecProvider 压缩编解码器（可选）
     * @return 消息转换器
     */
    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter streamMQMessageConverter(MessageSerializer<?> serializer,
            ObjectProvider<CompressionCodec> compressionCodecProvider) {
        DefaultMessageConverter converter = new DefaultMessageConverter(serializer);
        CompressionCodec codec = compressionCodecProvider.getIfAvailable();
        if (codec != null) {
            converter.setCompressionCodec(codec);
            LOG.info("CompressionCodec injected into DefaultMessageConverter: {}", codec.name());
        }
        LOG.info("Using DefaultMessageConverter with serializer={}", serializer.getClass().getSimpleName());
        return converter;
    }

    /**
     * 默认压缩编解码器：GzipCompressionCodec。
     *
     * <p>当用户未注册自定义 {@link CompressionCodec} Bean 时使用 GZIP 实现。
     * 可通过注册自定义 Bean 覆盖（如 LZ4）。
     *
     * @return GzipCompressionCodec 实例
     */
    @Bean
    @ConditionalOnMissingBean(CompressionCodec.class)
    public CompressionCodec streamMQCompressionCodec() {
        LOG.info("Using GzipCompressionCodec");
        return new GzipCompressionCodec();
    }

    /**
     * 默认重试策略：根据 {@code streammq.retry.policy} 配置加载。
     *
     * @param properties 配置
     * @return 重试策略
     */
    @Bean
    @ConditionalOnMissingBean(RetryPolicy.class)
    public RetryPolicy streamMQRetryPolicy(StreamMQProperties properties) {
        String className = properties.getRetry().getPolicy();
        LOG.info("Using RetryPolicy: {}", className);
        return instantiate(className, RetryPolicy.class);
    }

    /**
     * DLQ 配置 Bean（从 properties 读取，供全局和 per-consumer 策略使用）。
     */
    @Bean
    @ConditionalOnMissingBean(DlqConfig.class)
    public DlqConfig streamMQDlqConfig(StreamMQProperties properties) {
        StreamMQProperties.Dlq dlqProps = properties.getDlq();
        LOG.info("Creating DlqConfig: strategy={}, maxDlqRetryAttempts={}, secondaryDlqEnabled={}",
            dlqProps.getFailureStrategy(), dlqProps.getMaxDlqRetryAttempts(), dlqProps.isSecondaryDlqEnabled());
        return DlqConfig.builder()
            .failureStrategyClass(dlqProps.getFailureStrategy())
            .maxDlqRetryAttempts(dlqProps.getMaxDlqRetryAttempts())
            .dlqRetryDelayMs(dlqProps.getDlqRetryDelayMs())
            .secondaryDlqEnabled(dlqProps.isSecondaryDlqEnabled())
            .secondaryDlqKeyPrefix(dlqProps.getSecondaryDlqKeyPrefix())
            .dlqAlertThreshold(dlqProps.getAlertThreshold())
            .dlqRetryBackoffMultiplier(dlqProps.getRetryBackoffMultiplier())
            .dlqRetryMaxDelayMs(dlqProps.getRetryMaxDelayMs())
            .build();
    }

    /**
     * 全局 DLQ 消费失败处理策略 Bean。
     */
    @Bean
    @ConditionalOnMissingBean(DlqFailureStrategy.class)
    public DlqFailureStrategy streamMQDlqFailureStrategy(DlqConfig dlqConfig) {
        String className = dlqConfig.getFailureStrategyClass();
        LOG.info("Using DlqFailureStrategy: {}", className);
        return instantiate(className, DlqFailureStrategy.class);
    }

    /**
     * 生产者工厂：基于 Redisson 实现。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     * @param compressionCodecProvider 压缩编解码器（可选）
     * @return 生产者工厂
     */
    @Bean
    @ConditionalOnMissingBean(StreamMessageProducerFactory.class)
    public StreamMessageProducerFactory streamMQProducerFactory(RedissonClient redisson, MessageConverter converter,
            ObjectProvider<CompressionCodec> compressionCodecProvider) {
        RedissonStreamProducerFactory factory = new RedissonStreamProducerFactory(redisson, converter);
        CompressionCodec codec = compressionCodecProvider.getIfAvailable();
        if (codec != null) {
            factory.setCompressionCodec(codec);
            LOG.info("CompressionCodec injected into RedissonStreamProducerFactory: {}", codec.name());
        }
        LOG.info("Creating RedissonStreamProducerFactory");
        return factory;
    }

    /**
     * Listener 工厂：基于 Redisson 实现。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     * @return Listener 工厂
     */
    @Bean
    @ConditionalOnMissingBean(StreamMQListenerFactory.class)
    public StreamMQListenerFactory streamMQListenerFactory(RedissonClient redisson, MessageConverter converter) {
        LOG.info("Creating RedissonStreamListenerFactory");
        return new RedissonStreamListenerFactory(redisson, converter);
    }

    /**
     * 默认 StreamMQTemplate：基于 Redisson 实现。
     *
     * @param producerFactory 生产者工厂
     * @param converter 消息转换器
     * @param properties 配置
     * @return 模板
     */
    @Bean
    @ConditionalOnMissingBean(StreamMessageTemplate.class)
    public StreamMessageTemplate streamMQTemplate(StreamMessageProducerFactory producerFactory,
                                                     MessageConverter converter,
                                                     StreamMQProperties properties,
                                                     ObjectProvider<TransactionScanner> transactionScannerProvider,
                                                     ObjectProvider<StreamMQMetrics> metricsProvider) {
        String defaultGroup = properties.getProducer().getGroup();
        String txGroup = properties.getTransaction().getDefaultGroup();
        // 注入 namespace / send-message-timeout / stream.max-len 到 defaultConfig,
        // 保证 Producer 与 ListenerContainer 使用相同的 namespace,避免消息写入与读取 Key 不一致。
        ProducerConfig defaultConfig = ProducerConfig.builder()
            .group(defaultGroup)
            .namespace(properties.getNamespace())
            .sendMessageTimeout(properties.getProducer().getSendMessageTimeout())
            .streamMaxLen(properties.getProducer().getStreamMaxLen())
            .compressThreshold(properties.getProducer().getCompressThreshold())
            .build();
        LOG.info("Creating DefaultStreamMessageTemplate: defaultGroup={}, transactionGroup={}, namespace={}",
            defaultGroup, txGroup, properties.getNamespace());
        DefaultStreamMessageTemplate template = new DefaultStreamMessageTemplate(
            producerFactory, defaultGroup, converter, defaultConfig, txGroup);
        // 注入 TransactionScanner（如果可用），启用完整的半消息 + 回查事务流程
        TransactionScanner scanner = transactionScannerProvider.getIfAvailable();
        if (scanner != null) {
            template.setTransactionScanner(scanner);
            LOG.info("TransactionScanner injected into DefaultStreamMessageTemplate: full half-message flow enabled");
        }
        // 注入指标收集器（如果可用），启用发送指标埋点
        StreamMQMetrics metrics = metricsProvider.getIfAvailable();
        if (metrics != null) {
            template.setMetrics(metrics);
            LOG.info("StreamMQMetrics injected into DefaultStreamMessageTemplate: send metrics enabled");
        }
        return template;
    }

    /**
     * 注册 {@link StreamMessageService}，封装 {@link StreamMessageTemplate} 提供更简洁的发送 API。
     *
     * <p>用户可直接注入 {@link StreamMessageService}，仅传入 topic 与 body 即可发送消息，
     * 无需手动构造 {@code Message} 对象。遵循「依赖接口而非实现」原则，默认注册
     * {@link DefaultStreamMessageService} 实现。
     *
     * @param template StreamMQ 模板
     * @return StreamMessageService 实例
     */
    @Bean
    @ConditionalOnMissingBean(StreamMessageService.class)
    public StreamMessageService streamMQService(StreamMessageTemplate template) {
        LOG.info("Creating StreamMessageService wrapping {}", template.getClass().getSimpleName());
        return new DefaultStreamMessageService(template);
    }

    // ===================== 内置策略 Bean =====================

    /**
     * 默认追踪收集器：{@link NoopTraceCollector}。
     *
     * <p>当 {@code streammq.tracing.enabled=false}（默认）或未配置时注册，
     * 所有追踪方法均为空操作，避免空指针。
     *
     * @return NoopTraceCollector 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceCollector.class)
    @ConditionalOnProperty(prefix = "streammq.tracing", name = "enabled", havingValue = "false", matchIfMissing = true)
    public TraceCollector streamMQNoopTraceCollector() {
        LOG.info("Using NoopTraceCollector (tracing disabled)");
        return new NoopTraceCollector();
    }

    /**
     * SLF4J 追踪收集器：{@link Slf4jTraceCollector}。
     *
     * <p>当 {@code streammq.tracing.enabled=true} 时注册，覆盖 NoopTraceCollector，
     * 通过 SLF4J 输出追踪日志。
     *
     * @return Slf4jTraceCollector 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceCollector.class)
    @ConditionalOnProperty(prefix = "streammq.tracing", name = "enabled", havingValue = "true")
    public TraceCollector streamMQSlf4jTraceCollector() {
        LOG.info("Using Slf4jTraceCollector (tracing enabled)");
        return new Slf4jTraceCollector();
    }

    /**
     * 默认管理鉴权器：{@link DenyAllAuthenticator}。
     *
     * <p>始终拒绝所有访问请求，作为安全兜底，避免误开放运维端点。
     * 用户可注册自定义 {@link ManagementAuthenticator} Bean 覆盖。
     *
     * @return DenyAllAuthenticator 实例
     */
    @Bean
    @ConditionalOnMissingBean(ManagementAuthenticator.class)
    public ManagementAuthenticator streamMQManagementAuthenticator() {
        LOG.info("Using DenyAllAuthenticator (security fallback)");
        return new DenyAllAuthenticator();
    }

    /**
     * 追踪上下文生产者拦截器：当 {@code streammq.tracing.enabled=true} 时注册。
     *
     * @param traceCollector 追踪收集器
     * @return TraceContextProducerInterceptor 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceContextProducerInterceptor.class)
    @ConditionalOnProperty(prefix = "streammq.tracing", name = "enabled", havingValue = "true")
    public TraceContextProducerInterceptor streamMQTraceContextProducerInterceptor(TraceCollector traceCollector) {
        LOG.info("Using TraceContextProducerInterceptor (tracing enabled)");
        return new TraceContextProducerInterceptor(traceCollector);
    }

    /**
     * 追踪上下文消费者拦截器：当 {@code streammq.tracing.enabled=true} 时注册。
     *
     * @param traceCollector 追踪收集器
     * @return TraceContextConsumerInterceptor 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceContextConsumerInterceptor.class)
    @ConditionalOnProperty(prefix = "streammq.tracing", name = "enabled", havingValue = "true")
    public TraceContextConsumerInterceptor streamMQTraceContextConsumerInterceptor(TraceCollector traceCollector) {
        LOG.info("Using TraceContextConsumerInterceptor (tracing enabled)");
        return new TraceContextConsumerInterceptor(traceCollector);
    }

    // ===================== 工具方法 =====================

    /**
     * 通过反射实例化指定类的对象。
     * 要求目标类具有无参构造函数。
     *
     * @param className 类全限定名
     * @param expectedType 期望类型
     * @param <T> 类型
     * @return 实例
     */
    @SuppressWarnings("unchecked")
    private static <T> T instantiate(String className, Class<T> expectedType) {
        if (StringUtils.isEmpty(className)) {
            throw new IllegalArgumentException("Class name must not be null or empty for " + expectedType.getName());
        }
        try {
            Class<?> clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            if (!expectedType.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(
                    "Class " + className + " is not assignable to " + expectedType.getName());
            }
            return (T) clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException ex) {
            throw new IllegalArgumentException(
                "Failed to instantiate " + className + " as " + expectedType.getName()
                    + " (requires no-arg constructor)", ex);
        }
    }
}
