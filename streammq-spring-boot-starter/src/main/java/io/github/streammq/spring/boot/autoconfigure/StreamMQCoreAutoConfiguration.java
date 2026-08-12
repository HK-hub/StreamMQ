package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.compression.DefaultCompressionCodecRegistry;
import io.github.streammq.adapter.redisson.compression.GzipCompressionCodec;
import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.event.AsyncStreamMQEventBus;
import io.github.streammq.adapter.redisson.interceptor.TraceContextConsumerInterceptor;
import io.github.streammq.adapter.redisson.interceptor.TraceContextProducerInterceptor;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.adapter.redisson.trace.NoopTraceCollector;
import io.github.streammq.adapter.redisson.trace.Slf4jTraceCollector;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.compression.CompressionCodecRegistry;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.event.StreamMQEventBus;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.service.DefaultStreamMessageService;
import io.github.streammq.core.service.StreamMessageService;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import jakarta.annotation.PostConstruct;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ 核心自动装配：注册 Serializer / Converter / RetryPolicy / ProducerFactory / ConsumerFactory /
 * Template 等基础 Bean。
 *
 * <p>装配条件：
 *
 * <ul>
 *   <li>{@code streammq.enabled=true}（默认 true）
 *   <li>classpath 存在 {@link RedissonClient} 与 {@link StreamMessageTemplate}
 *   <li><b>用户必须自行注册 {@link RedissonClient} Bean</b>（通常来自 {@code redisson-spring-boot-starter}），
 *       StreamMQ 不提供兜底实例，避免意外连接到错误的 Redis
 * </ul>
 *
 * <p>所有核心 Bean 均标注 {@code @ConditionalOnMissingBean}，用户可在自定义配置类中覆盖。
 *
 * <p><b>架构说明（05-1.1 / 05-1.2）：</b>本类直接引用了 Redisson 适配层的具体类 （如 {@code
 * DefaultStreamMessageTemplate}、{@code RedissonStreamProducerFactory}）， 这意味着 Spring Boot Starter 与
 * Redisson 适配层存在紧耦合。 这是有意为之的设计决策——Starter 的职责是提供开箱即用的自动装配体验， 而非实现完全的 SPI 解耦。如需替换 Redis 客户端，可参考
 * {@code streammq-core} 模块的接口定义自行实现适配层，并通过 {@code @ConditionalOnMissingBean} 覆盖本类中的 Bean 定义。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = "streammq",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
@ConditionalOnClass({RedissonClient.class, StreamMessageTemplate.class})
@EnableConfigurationProperties(StreamMQProperties.class)
public class StreamMQCoreAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQCoreAutoConfiguration.class);

    private final StreamMQProperties properties;

    StreamMQCoreAutoConfiguration(StreamMQProperties properties) {
        this.properties = properties;
    }

    /** 启动时校验配置属性值合法性。 */
    @PostConstruct
    void validateProperties() {
        properties.validate();
    }

    /**
     * 默认序列化器，从配置 {@code streammq.producer.serializer} 读取 Class 并实例化。
     *
     * @param properties 配置
     * @return 序列化器
     */
    @Bean
    @ConditionalOnMissingBean(MessageSerializer.class)
    public MessageSerializer<?> streamMQMessageSerializer(StreamMQProperties properties) {
        Class<? extends MessageSerializer> clazz = properties.getProducer().getSerializer();
        LOG.debug("Using MessageSerializer: {}", clazz.getSimpleName());
        return BeanUtils.instantiateClass(clazz);
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
    public MessageConverter streamMQMessageConverter(
            MessageSerializer<?> serializer,
            ObjectProvider<CompressionCodec> compressionCodecProvider,
            CompressionCodecRegistry codecRegistry) {
        DefaultMessageConverter converter = new DefaultMessageConverter(serializer);
        CompressionCodec codec = compressionCodecProvider.getIfAvailable();
        if (codec != null) {
            converter.setCompressionCodec(codec);
            LOG.debug("CompressionCodec injected into DefaultMessageConverter: {}", codec.name());
        }
        converter.setCompressionCodecRegistry(codecRegistry);
        LOG.debug(
                "Using DefaultMessageConverter with serializer={}",
                serializer.getClass().getSimpleName());
        return converter;
    }

    /**
     * 事件总线，模块间异步解耦通信的核心。
     *
     * <p>核心流程通过事件总线发布事件，扩展模块（Tracing/Metrics/Diagnostics） 订阅事件后异步处理，核心流程不直接依赖扩展模块。
     *
     * @return 异步事件总线
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(StreamMQEventBus.class)
    public StreamMQEventBus streamMQEventBus() {
        LOG.debug("Creating AsyncStreamMQEventBus");
        return new AsyncStreamMQEventBus();
    }

    /**
     * 压缩编解码器注册表，管理所有可用 Codec（按名称索引）。
     *
     * <p>默认注册 {@code gzip}，用户自定义 Codec 通过实现 {@link CompressionCodec} 并注册 Spring Bean 即可自动加入注册表。
     *
     * @param codecs 所有用户注册的 CompressionCodec Bean（可选）
     * @return 注册表
     */
    @Bean
    @ConditionalOnMissingBean(CompressionCodecRegistry.class)
    public CompressionCodecRegistry streamMQCompressionCodecRegistry(
            ObjectProvider<CompressionCodec> codecs) {
        DefaultCompressionCodecRegistry registry = new DefaultCompressionCodecRegistry();
        // 内置 Codec
        registry.register(new GzipCompressionCodec());
        // 用户自定义 Codec
        codecs.forEach(registry::register);
        LOG.debug("CompressionCodecRegistry created with codecs: {}", registry.availableCodecs());
        return registry;
    }

    /**
     * 默认压缩编解码器：GzipCompressionCodec。
     *
     * <p>当用户未注册自定义 {@link CompressionCodec} Bean 时使用 GZIP 实现。 可通过注册自定义 Bean 覆盖（如 LZ4）。
     *
     * @return GzipCompressionCodec 实例
     */
    @Bean
    @ConditionalOnMissingBean(CompressionCodec.class)
    public CompressionCodec streamMQCompressionCodec() {
        LOG.debug("Using GzipCompressionCodec");
        return new GzipCompressionCodec();
    }

    /**
     * 默认重试策略，从配置 {@code streammq.retry.policy} 读取 Class 并实例化。
     *
     * @param properties 配置
     * @return 重试策略
     */
    @Bean
    @ConditionalOnMissingBean(RetryPolicy.class)
    public RetryPolicy streamMQRetryPolicy(StreamMQProperties properties) {
        Class<? extends RetryPolicy> clazz = properties.getRetry().getPolicy();
        LOG.debug("Using RetryPolicy: {}", clazz.getSimpleName());
        return BeanUtils.instantiateClass(clazz);
    }

    /** DLQ 配置 Bean（从 properties 读取，供全局和 per-consumer 策略使用）。 */
    @Bean
    @ConditionalOnMissingBean(DlqConfig.class)
    public DlqConfig streamMQDlqConfig(StreamMQProperties properties) {
        StreamMQProperties.Dlq dlqProps = properties.getDlq();
        LOG.debug(
                "Creating DlqConfig: strategy={}, maxDlqRetryAttempts={}, secondaryDlqEnabled={}",
                dlqProps.getFailureStrategy().getSimpleName(),
                dlqProps.getMaxDlqRetryAttempts(),
                dlqProps.isSecondaryDlqEnabled());
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

    /** 全局 DLQ 消费失败处理策略 Bean。 */
    @Bean
    @ConditionalOnMissingBean(DlqFailureStrategy.class)
    public DlqFailureStrategy streamMQDlqFailureStrategy(DlqConfig dlqConfig) {
        Class<? extends DlqFailureStrategy> clazz = dlqConfig.getFailureStrategyClass();
        LOG.debug("Using DlqFailureStrategy: {}", clazz.getSimpleName());
        return BeanUtils.instantiateClass(clazz);
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
    public StreamMessageProducerFactory streamMQProducerFactory(
            RedissonClient redisson,
            MessageConverter converter,
            ObjectProvider<CompressionCodec> compressionCodecProvider) {
        RedissonStreamProducerFactory factory =
                new RedissonStreamProducerFactory(redisson, converter);
        CompressionCodec codec = compressionCodecProvider.getIfAvailable();
        if (codec != null) {
            factory.setCompressionCodec(codec);
            LOG.debug(
                    "CompressionCodec injected into RedissonStreamProducerFactory: {}",
                    codec.name());
        }
        LOG.debug("Creating RedissonStreamProducerFactory");
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
    public StreamMQListenerFactory streamMQListenerFactory(
            RedissonClient redisson, MessageConverter converter) {
        LOG.debug("Creating RedissonStreamListenerFactory");
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
    public StreamMessageTemplate streamMQTemplate(
            StreamMessageProducerFactory producerFactory,
            MessageConverter converter,
            StreamMQProperties properties,
            StreamMQEventBus eventBus,
            ObjectProvider<TransactionScanner> transactionScannerProvider,
            ObjectProvider<StreamMQMetrics> metricsProvider,
            ObjectProvider<ProducerInterceptor> producerInterceptorProvider) {
        String defaultGroup = properties.getProducer().getGroup();
        String txGroup = properties.getTransaction().getDefaultGroup();
        // 注入 namespace / send-message-timeout / stream.max-len 到 defaultConfig,
        // 保证 Producer 与 ListenerContainer 使用相同的 namespace,避免消息写入与读取 Key 不一致。
        ProducerConfig defaultConfig =
                ProducerConfig.builder()
                        .group(defaultGroup)
                        .namespace(properties.getNamespace())
                        .sendMessageTimeout(properties.getProducer().getSendMessageTimeout())
                        .streamMaxLen(properties.getProducer().getStreamMaxLen())
                        .compressThreshold(properties.getProducer().getCompressThreshold())
                        .build();
        LOG.debug(
                "Creating DefaultStreamMessageTemplate: defaultGroup={}, transactionGroup={},"
                        + " namespace={}",
                defaultGroup,
                txGroup,
                properties.getNamespace());
        DefaultStreamMessageTemplate template =
                new DefaultStreamMessageTemplate(
                        producerFactory, defaultGroup, converter, defaultConfig, txGroup);
        template.setEventBus(eventBus);
        // 注入 TransactionScanner（如果可用），启用完整的半消息 + 回查事务流程
        TransactionScanner scanner = transactionScannerProvider.getIfAvailable();
        if (scanner != null) {
            template.setTransactionScanner(scanner);
            LOG.debug(
                    "TransactionScanner injected into DefaultStreamMessageTemplate: full"
                            + " half-message flow enabled");
        }
        // 注入指标收集器（如果可用），启用发送指标埋点
        StreamMQMetrics metrics = metricsProvider.getIfAvailable();
        if (metrics != null) {
            template.setMetrics(metrics);
            LOG.debug(
                    "StreamMQMetrics injected into DefaultStreamMessageTemplate: send metrics"
                            + " enabled");
        }
        // 注入生产者拦截器（如果可用），启用追踪、日志等切面能力
        java.util.List<ProducerInterceptor> producerInterceptors =
                producerInterceptorProvider.stream().toList();
        if (!producerInterceptors.isEmpty()) {
            LOG.debug(
                    "Registering {} ProducerInterceptor(s): {}",
                    producerInterceptors.size(),
                    producerInterceptors.stream().map(ProducerInterceptor::name).toList());
            template.setProducerInterceptors(producerInterceptors);
        }
        return template;
    }

    /**
     * 注册 {@link StreamMessageService}，封装 {@link StreamMessageTemplate} 提供更简洁的发送 API。
     *
     * <p>用户可直接注入 {@link StreamMessageService}，仅传入 topic 与 body 即可发送消息， 无需手动构造 {@code Message}
     * 对象。遵循「依赖接口而非实现」原则，默认注册 {@link DefaultStreamMessageService} 实现。
     *
     * @param template StreamMQ 模板
     * @return StreamMessageService 实例
     */
    @Bean
    @ConditionalOnMissingBean(StreamMessageService.class)
    public StreamMessageService streamMQService(StreamMessageTemplate template) {
        LOG.debug("Creating StreamMessageService wrapping {}", template.getClass().getSimpleName());
        return new DefaultStreamMessageService(template);
    }

    // ===================== 内置策略 Bean =====================

    /**
     * 默认追踪收集器：{@link NoopTraceCollector}。
     *
     * <p>当 {@code streammq.tracing.enabled=false}（默认）或未配置时注册， 所有追踪方法均为空操作，避免空指针。
     *
     * @return NoopTraceCollector 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceCollector.class)
    @ConditionalOnProperty(
            prefix = "streammq.tracing",
            name = "enabled",
            havingValue = "false",
            matchIfMissing = true)
    public TraceCollector streamMQNoopTraceCollector() {
        LOG.debug("Using NoopTraceCollector (tracing disabled)");
        return new NoopTraceCollector();
    }

    /**
     * SLF4J 追踪收集器：{@link Slf4jTraceCollector}。
     *
     * <p>当 {@code streammq.tracing.enabled=true} 时注册，覆盖 NoopTraceCollector， 通过 SLF4J 输出追踪日志。
     *
     * @return Slf4jTraceCollector 实例
     */
    @Bean
    @ConditionalOnMissingBean(TraceCollector.class)
    @ConditionalOnProperty(prefix = "streammq.tracing", name = "enabled", havingValue = "true")
    public TraceCollector streamMQSlf4jTraceCollector() {
        LOG.debug("Using Slf4jTraceCollector (tracing enabled)");
        return new Slf4jTraceCollector();
    }

    /**
     * 默认管理鉴权器：{@link DenyAllAuthenticator}。
     *
     * <p>始终拒绝所有访问请求，作为安全兜底，避免误开放运维端点。 用户可注册自定义 {@link ManagementAuthenticator} Bean 覆盖。
     *
     * @return DenyAllAuthenticator 实例
     */
    @Bean
    @ConditionalOnMissingBean(ManagementAuthenticator.class)
    public ManagementAuthenticator streamMQManagementAuthenticator() {
        LOG.debug("Using DenyAllAuthenticator (security fallback)");
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
    public TraceContextProducerInterceptor streamMQTraceContextProducerInterceptor(
            TraceCollector traceCollector) {
        LOG.debug("Using TraceContextProducerInterceptor (tracing enabled)");
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
    public TraceContextConsumerInterceptor streamMQTraceContextConsumerInterceptor(
            TraceCollector traceCollector) {
        LOG.debug("Using TraceContextConsumerInterceptor (tracing enabled)");
        return new TraceContextConsumerInterceptor(traceCollector);
    }

    // SPI 类名已改为 Class<?> 属性，由 Spring Boot 启动时通过 ConfigurationProperties
    // 的 Class binding 自动校验，不再需要反射 instantiate/validateClass 方法。
}
