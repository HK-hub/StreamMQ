/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.broadcast.RedisBroadcastInstanceRegistry;
import io.github.streammq.adapter.redisson.compression.DefaultCompressionCodecRegistry;
import io.github.streammq.adapter.redisson.compression.GzipCompressionCodec;
import io.github.streammq.adapter.redisson.compression.Lz4CompressionCodec;
import io.github.streammq.adapter.redisson.compression.Lz4CompressionCodecFactory;
import io.github.streammq.adapter.redisson.container.ConsumerTuning;
import io.github.streammq.adapter.redisson.container.DefaultConsumerTuning;
import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.interceptor.TraceContextConsumerInterceptor;
import io.github.streammq.adapter.redisson.interceptor.TraceContextProducerInterceptor;
import io.github.streammq.adapter.redisson.listener.RedissonBroadcastGroupRegistry;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.adapter.redisson.trace.NoopTraceCollector;
import io.github.streammq.adapter.redisson.trace.Slf4jTraceCollector;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.broadcast.BroadcastInstanceIdResolver;
import io.github.streammq.core.broadcast.BroadcastInstanceRegistry;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.compression.CompressionCodecRegistry;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.interceptor.TraceCollector;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.service.DefaultStreamMessageService;
import io.github.streammq.core.service.StreamMessageService;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import jakarta.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
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
 * <p><b>架构说明：</b>本类直接引用了 Redisson 适配层的具体类（如 {@code DefaultStreamMessageTemplate}、{@code
 * RedissonStreamProducer}），Spring Boot Starter 与 Redisson 适配层存在紧耦合。这是有意为之——Starter
 * 的职责是提供开箱即用的自动装配体验。 如需替换 Redis 客户端，可参考 {@code streammq-core} 模块的接口定义自行实现适配层， 并通过
 * {@code @ConditionalOnMissingBean} 覆盖本类中的 Bean 定义。
 *
 * <p>从 0.1.1 起，{@code StreamMessageProducerFactory} 已移除：Producer 直接作为 Bean 注册， Template 注入具体
 * Producer 实例，彻底遵循 DIP（依赖抽象，不依赖具体工厂）。
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
@EnableConfigurationProperties(StreamMQProperties.class)
public class StreamMQCoreAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQCoreAutoConfiguration.class);

    private final StreamMQProperties properties;

    StreamMQCoreAutoConfiguration(StreamMQProperties properties) {
        this.properties = properties;
    }

    /** 启动时校验配置属性值合法性，并输出一次追踪能力姿态摘要。 */
    @PostConstruct
    void validateProperties() {
        properties.validate();
        logTracingPosture();
    }

    /**
     * 客户端地址可信策略（限流按此聚合来源）。
     *
     * <p>安全默认值：不信任 {@code X-Forwarded-For}，仅按不可伪造的 {@code remoteAddr} 聚合——防止客户端伪造 XFF 绕过失败限流。作为
     * Bean 提供（而不是写入静态全局），使策略具备容器生命周期归属，可被管理端点与 {@code streammq-diagnostics} 端点共同注入复用。
     */
    @Bean
    @ConditionalOnMissingBean(
            io.github.streammq.core.util.WebRequestAuthSupport.ClientAddressPolicy.class)
    public io.github.streammq.core.util.WebRequestAuthSupport.ClientAddressPolicy
            streamMQClientAddressPolicy(StreamMQProperties properties) {
        io.github.streammq.core.util.WebRequestAuthSupport.ClientAddressPolicy policy =
                new io.github.streammq.core.util.WebRequestAuthSupport.ClientAddressPolicy(
                        properties.getAdmin().isTrustForwardedHeaders(),
                        new HashSet<>(properties.getAdmin().getTrustedProxies()));
        LOG.info(
                "StreamMQ web auth client-address policy: trustForwardedHeaders={},"
                        + " trustedProxies={}",
                policy.trustForwardedHeaders(),
                policy.trustedProxyCidrs());
        return policy;
    }

    /**
     * 输出追踪相关开关的启动期摘要（单条 INFO），便于运维确认实际生效的追踪姿态。
     *
     * <p>内容：日志级追踪（{@code streammq.tracing.enabled}）、存储级追踪（{@code streammq.trace.enabled} 与 {@code
     * streammq.trace.storage}）、OpenTelemetry 是否在 classpath（供 otel 模块判定）。
     */
    private void logTracingPosture() {
        boolean otelDetected =
                org.springframework.util.ClassUtils.isPresent(
                        "io.opentelemetry.api.OpenTelemetry",
                        org.springframework.util.ClassUtils.getDefaultClassLoader());
        LOG.info(
                "StreamMQ tracing posture: tracing.enabled={}, trace.enabled={},"
                        + " trace.storage={}, otel.detected(classpath)={}",
                properties.getTracing().isEnabled(),
                properties.getTrace().isEnabled(),
                properties.getTrace().getStorage(),
                otelDetected);
    }

    /**
     * 默认序列化器，从配置 {@code streammq.producer.serializer} 读取 Class 并实例化； 未配置（或配置为空）时使用默认值 {@link
     * StreamMQConstants#DEFAULT_SERIALIZER}（0.1.2 起为 {@code JacksonJsonSerializer}）。
     *
     * <p><b>安全默认：</b>0.1.1 曾默认 Fury 宽松模式（吞吐约为 Jackson 的 7~13 倍），但其允许把 Redis 中字节流反序列化为 classpath
     * 上任意类，在共享/多租户 Redis 上是反序列化 RCE 攻击面，且会通过本 SDK 传播给下游应用。 0.1.2 起默认回退为 Jackson（严格类型、无 gadget
     * 面、Redis 中人类可读）。需要高吞吐的用户显式 opt-in 到 Fury。
     *
     * <p>当序列化器为 Fury 时，按 {@code streammq.producer.fury-require-class-registration} 决定类注册白名单： {@code
     * false}=宽松模式（仅限受信单租户 Redis），{@code true}=强制白名单（共享/多租户 Redis 建议）。 其它序列化器沿用无参构造实例化。
     *
     * <p><b>optional 依赖的优雅失败：</b>Fury / Protostuff 在 {@code streammq-redisson} 中是 optional 依赖（避免把
     * Guava、Protostuff 强制塞进所有下游应用）。用户显式配置但 classpath 上没有时， 这里给出可直接照做的修复指引，而不是抛一个含义不明的 {@code
     * NoClassDefFoundError}。
     *
     * @param properties 配置
     * @return 序列化器
     */
    @Bean
    @ConditionalOnMissingBean(MessageSerializer.class)
    public MessageSerializer<?> streamMQMessageSerializer(StreamMQProperties properties) {
        Class<? extends MessageSerializer> clazz = properties.getProducer().getSerializer();
        if (Objects.isNull(clazz)) {
            // 配置为显式空值（如 serializer: 留空）时回退到默认序列化器，避免 NPE
            clazz = StreamMQSpringConstants.DEFAULT_SERIALIZER_CLASS;
        }
        if (StreamMQSpringConstants.FURY_SERIALIZER_CLASS_NAME.equals(clazz.getName())) {
            boolean requireClassRegistration =
                    properties.getProducer().isFuryRequireClassRegistration();
            java.util.List<Class<?>> registered =
                    properties.getProducer().getFuryRegisteredClasses();
            if (requireClassRegistration && (registered == null || registered.isEmpty())) {
                LOG.warn(
                        "FurySerializer is running in class-registration whitelist mode (default)"
                            + " but no payload types are registered: every unregistered body type"
                            + " will fail to deserialize. Declare them via"
                            + " streammq.producer.fury-registered-classes, or set"
                            + " streammq.producer.fury-require-class-registration=false (only when"
                            + " Redis is fully trusted).");
            }
            LOG.info(
                    "Using FurySerializer with requireClassRegistration={}, registeredTypes={}"
                            + " (change via streammq.producer.fury-require-class-registration /"
                            + " streammq.producer.fury-registered-classes)",
                    requireClassRegistration,
                    Objects.isNull(registered) ? 0 : registered.size());
            return instantiateFury(clazz, requireClassRegistration, registered);
        }
        LOG.debug(
                "Using MessageSerializer: {} (default: {})",
                clazz.getName(),
                StreamMQConstants.DEFAULT_SERIALIZER);
        return instantiateSerializer(clazz);
    }

    /**
     * 按 {@code requireClassRegistration} 实例化 Fury 序列化器。
     *
     * <p>通过反射调用而非直接 {@code new FurySerializer<>(...)}：starter 不持有 Fury 的编译期引用， 未引入 Fury 的应用不会被
     * {@code NoClassDefFoundError} 拖垮。
     */
    private MessageSerializer<?> instantiateFury(
            Class<? extends MessageSerializer> clazz,
            boolean requireClassRegistration,
            java.util.List<Class<?>> registeredTypes) {
        try {
            if (Objects.nonNull(registeredTypes) && !registeredTypes.isEmpty()) {
                return clazz.getDeclaredConstructor(boolean.class, Class[].class)
                        .newInstance(
                                requireClassRegistration, registeredTypes.toArray(new Class<?>[0]));
            }
            return clazz.getDeclaredConstructor(boolean.class)
                    .newInstance(requireClassRegistration);
        } catch (ReflectiveOperationException | LinkageError e) {
            // 反射调用把构造器内部抛出的异常包成 InvocationTargetException：必须先解包，
            // 否则宽松模式的安全门禁（SecurityException）会被误报成"缺 fory-core 依赖"，
            // 用户照着提示加依赖后仍失败，且看不到真正原因（发布前修复 R6-S4）。
            Throwable cause =
                    (e instanceof java.lang.reflect.InvocationTargetException ite
                                    && ite.getCause() != null)
                            ? ite.getCause()
                            : e;
            if (cause instanceof SecurityException) {
                throw new IllegalStateException(
                        "streammq.producer.serializer="
                                + StreamMQSpringConstants.FURY_SERIALIZER_CLASS_NAME
                                + " with streammq.producer.fury-require-class-registration=false"
                                + " requests Fury UNRESTRICTED (loose) mode, which is gated for"
                                + " security reasons: Redis bytes may be deserialized into any"
                                + " class on the classpath (deserialization RCE on shared /"
                                + " multi-tenant Redis). To enable it explicitly, start the JVM"
                                + " with -Dstreammq.security.allowUnrestrictedSerializer=true"
                                + " (only for fully trusted single-tenant Redis), or keep"
                                + " streammq.producer.fury-require-class-registration=true and"
                                + " register payload types via"
                                + " streammq.producer.fury-registered-classes. Cause: "
                                + cause.getMessage(),
                        cause);
            }
            throw new IllegalStateException(
                    "streammq.producer.serializer is set to "
                            + StreamMQSpringConstants.FURY_SERIALIZER_CLASS_NAME
                            + " but Apache Fory (formerly Apache Fury) is not on the classpath."
                            + " Add the dependency: org.apache.fory:fory-core (it is an optional"
                            + " dependency of streammq-redisson). Alternatively use the default"
                            + " JacksonJsonSerializer, or ProtostuffSerializer with"
                            + " io.protostuff:protostuff-core + protostuff-runtime.",
                    e);
        }
    }

    /** 实例化序列化器，并对 optional 依赖缺失给出可操作的错误信息。 */
    private MessageSerializer<?> instantiateSerializer(Class<? extends MessageSerializer> clazz) {
        try {
            return BeanUtils.instantiateClass(clazz);
        } catch (LinkageError e) {
            throw new IllegalStateException(
                    "Failed to instantiate MessageSerializer "
                            + clazz.getName()
                            + ": a required optional dependency is missing from the classpath."
                            + " Apache Fory needs org.apache.fory:fory-core; Protostuff needs"
                            + " io.protostuff:protostuff-core + protostuff-runtime. Or fall back to"
                            + " the default JacksonJsonSerializer.",
                    e);
        }
    }

    /**
     * 默认消息转换器：DefaultMessageConverter。
     *
     * <p><b>多 Codec 语义（R6-S3）：</b>允许注册多个 {@link CompressionCodec} Bean（按名称写入/解压、由注册表索引）。
     * 此处只解析"默认"Codec：{@code streammq.producer.compression-codec} 精确匹配（显式配置优先）→ {@code @Primary} →
     * 唯一候选；多候选且无法消歧时不会静默取任意一个。
     *
     * @param serializer 序列化器
     * @param compressionCodecProvider 压缩编解码器 Bean（可选，可多个）
     * @param properties 配置
     * @param codecRegistry 压缩编解码器注册表
     * @return 消息转换器
     */
    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter streamMQMessageConverter(
            MessageSerializer<?> serializer,
            ObjectProvider<CompressionCodec> compressionCodecProvider,
            StreamMQProperties properties,
            CompressionCodecRegistry codecRegistry) {
        DefaultMessageConverter converter = new DefaultMessageConverter(serializer);
        CompressionCodec codec =
                resolveDefaultCompressionCodec(
                        compressionCodecProvider,
                        codecRegistry,
                        properties,
                        "DefaultMessageConverter");
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
     * StreamMQ 统一虚拟线程池：容器消费循环、异步发送、事件分发等全部复用此池。
     *
     * <p><b>自定义方式：</b>注册名为 {@code streammqExecutor} 的 {@link ExecutorService} Bean 即可覆盖本默认实现，
     * 全部内部组件（容器 / 模板 / 事件总线）自动切换到用户提供的执行器。 注意：仅按 <b>Bean 名称</b> {@code streammqExecutor}
     * 判定是否回退，避免误吞用户自定义的其他任意 {@link ExecutorService} Bean。
     *
     * @return 虚拟线程池
     */
    @Bean(name = "streammqExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "streammqExecutor")
    public ExecutorService streammqExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * 广播消费实例注册中心：为广播消费者提供<b>跨重启稳定</b>的持久化实例身份。
     *
     * <p><b>为什么默认装配：</b>广播消费的每个实例使用一个独立 Redis 消费者组， 组名由实例身份派生。身份不稳定会导致每次重启新建组——旧组成为僵尸组持续占用 Redis
     * 内存，且重启期间产生的消息<b>不会被补投</b>。注册中心把身份持久化到 Redis（辅以本地文件快路径）， 从根本上消除该问题。
     *
     * <p><b>可覆盖：</b>注册自定义 {@link BroadcastInstanceRegistry} Bean 即可替换（例如接入外部服务发现系统）。
     *
     * @param redisson Redisson 客户端
     * @return 广播实例注册中心
     */
    @Bean
    @ConditionalOnMissingBean(BroadcastInstanceRegistry.class)
    public BroadcastInstanceRegistry streamMQBroadcastInstanceRegistry(RedissonClient redisson) {
        LOG.debug("Creating RedisBroadcastInstanceRegistry (persistent broadcast identity)");
        return new RedisBroadcastInstanceRegistry(redisson);
    }

    /**
     * 广播实例身份解析器：把"配置 / 本地持久文件 / 注册中心回收 / 注册中心分配"四级来源收敛为单一稳定身份。
     *
     * @param registry 广播实例注册中心
     * @param properties 配置属性
     * @return 身份解析器
     */
    @Bean
    @ConditionalOnMissingBean(BroadcastInstanceIdResolver.class)
    public BroadcastInstanceIdResolver streamMQBroadcastInstanceIdResolver(
            BroadcastInstanceRegistry registry, StreamMQProperties properties) {
        StreamMQProperties.Consumer consumer = properties.getConsumer();
        String file = consumer.getBroadcastInstanceIdFile();
        java.nio.file.Path path =
                io.github.streammq.core.util.StringUtils.isEmpty(file)
                        ? null
                        : java.nio.file.Paths.get(file);
        return new BroadcastInstanceIdResolver(
                registry,
                path,
                consumer.getBroadcastLeaseTimeout().toMillis(),
                consumer.getBroadcastReclaimGrace().toMillis());
    }

    /**
     * 压缩编解码器注册表，管理所有可用 Codec（按名称索引）。
     *
     * <p>默认注册 {@code gzip}；若 classpath 存在 {@code org.lz4:lz4-java}，自动追加 {@code lz4} Codec（通过 {@link
     * Lz4CompressionCodecFactory#tryCreate()} 反射探测，不引入编译期依赖）。 用户自定义 Codec 通过实现 {@link
     * CompressionCodec} 并注册 Spring Bean 即可自动加入注册表。
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
        // 可选 LZ4 Codec：仅当 classpath 存在 lz4-java 时注册，避免硬依赖
        registerOptionalLz4(registry);
        // 用户自定义 Codec
        // 全量注册（允许多个 Codec：按 name() 索引，供 compressed 字段按名解压）
        codecs.forEach(registry::register);
        LOG.debug("CompressionCodecRegistry created with codecs: {}", registry.availableCodecs());
        return registry;
    }

    /**
     * 解析"默认"压缩 Codec（选择顺序见 {@link StreamMQBeanResolution#defaultCompressionCodec}）。
     *
     * <p>多候选且无需压缩时返回 null 并记录 WARN——不静默取任意一个，避免"线上压缩格式取决于 Bean 注册顺序"。
     */
    private CompressionCodec resolveDefaultCompressionCodec(
            ObjectProvider<CompressionCodec> provider,
            CompressionCodecRegistry registry,
            StreamMQProperties properties,
            String injectionPoint) {
        java.util.List<CompressionCodec> candidates = provider.orderedStream().toList();
        boolean compressionRequired = properties.getProducer().getCompressThreshold() > 0;
        CompressionCodec codec =
                StreamMQBeanResolution.defaultCompressionCodec(
                        candidates,
                        candidates.size() > 1 ? provider.getIfUnique() : null,
                        registry,
                        properties.getProducer().getCompressionCodec(),
                        compressionRequired,
                        injectionPoint);
        if (codec == null && candidates.size() > 1) {
            LOG.warn(
                    "{}: {} CompressionCodec beans are registered but none is @Primary and"
                        + " streammq.producer.compression-codec is not set; no default codec was"
                        + " selected (nothing is picked arbitrarily). Decompression still resolves"
                        + " codecs by name from the registry. Set"
                        + " streammq.producer.compression-codec=<name> or mark one @Primary when"
                        + " compression-on-send is needed. Candidates: {}",
                    injectionPoint,
                    candidates.size(),
                    StreamMQBeanResolution.describe(candidates));
        }
        return codec;
    }

    /**
     * 条件性注册 LZ4 Codec：探测 lz4-java 是否在 classpath。
     *
     * <p>设计要点：
     *
     * <ul>
     *   <li>使用 {@link Lz4CompressionCodecFactory#isAvailable()} 仅探测不初始化，避免冷启动开销
     *   <li>使用 {@link Lz4CompressionCodecFactory#tryCreate()} 创建实例，LZ4 不可用时返回 null 而非抛异常
     *   <li>不可用时输出单条 INFO 日志，便于运维确认 lz4-java 未生效的原因
     * </ul>
     */
    private void registerOptionalLz4(DefaultCompressionCodecRegistry registry) {
        if (!Lz4CompressionCodecFactory.isAvailable()) {
            LOG.info("LZ4 compression codec not available (add org.lz4:lz4-java to enable)");
            return;
        }
        Lz4CompressionCodec lz4 = Lz4CompressionCodecFactory.tryCreate();
        if (lz4 == null) {
            // 已记录 WARN 日志
            return;
        }
        registry.register(lz4);
        LOG.info(
                "LZ4 compression codec registered (org.lz4:lz4-java detected on classpath,"
                        + " name={})",
                lz4.name());
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
        // 默认 FixedArrayRetryPolicy 支持通过 streammq.retry.delay-array 自定义延时数组
        if (clazz == FixedArrayRetryPolicy.class) {
            long[] delayMillis = parseDelayArray(properties.getRetry().getDelayArray());
            if (delayMillis != null) {
                LOG.info(
                        "Using FixedArrayRetryPolicy with custom delay-array: {}ms",
                        properties.getRetry().getDelayArray());
                return new FixedArrayRetryPolicy(delayMillis);
            }
        }
        LOG.debug("Using RetryPolicy: {}", clazz.getSimpleName());
        return BeanUtils.instantiateClass(clazz);
    }

    /**
     * 解析 {@code streammq.retry.delay-array}（逗号分隔毫秒值）为延时数组；空/非法时返回 null（使用策略默认）。
     *
     * @param delayArray 配置值，可为空
     * @return 延时数组，或 null
     */
    private static long[] parseDelayArray(String delayArray) {
        if (delayArray == null || delayArray.trim().isEmpty()) {
            return null;
        }
        try {
            String[] parts = delayArray.split(",");
            long[] result = new long[parts.length];
            for (int i = 0; i < parts.length; i++) {
                result[i] = Long.parseLong(parts[i].trim());
                if (result[i] <= 0) {
                    return null;
                }
            }
            return result;
        } catch (NumberFormatException ex) {
            LOG.warn(
                    "Invalid streammq.retry.delay-array '{}', using FixedArrayRetryPolicy default",
                    delayArray);
            return null;
        }
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
                .minRetryDelayMs(dlqProps.getMinRetryDelayMs())
                .streamMaxLen(dlqProps.getStreamMaxLen())
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
     * 默认生产者：基于 Redisson 实现，作为独立 Bean 注册。
     *
     * <p>用户可直接注入 {@link StreamMessageProducer} 发送消息，或通过 {@link StreamMessageTemplate} / {@link
     * io.github.streammq.core.service.StreamMessageService} 使用。 生命周期由 Spring 容器管理，与 Template 解耦。
     *
     * <p><b>多 Codec 语义（R6-S3）：</b>注册多个 {@link CompressionCodec} Bean 时，默认压缩 Codec 按 {@code
     * streammq.producer.compression-codec} 精确匹配（显式配置优先）→ {@code @Primary} → 唯一候选 解析；多候选且
     * 无法消歧时不会静默取任意一个（压缩已启用则启动失败并列出候选）。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     * @param properties 配置
     * @param compressionCodecProvider 压缩编解码器 Bean（可选，可多个）
     * @param codecRegistry 压缩编解码器注册表（用于按名称解析内置 Codec）
     * @return 生产者实例
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(StreamMessageProducer.class)
    public StreamMessageProducer streamMQProducer(
            RedissonClient redisson,
            MessageConverter converter,
            StreamMQProperties properties,
            ObjectProvider<CompressionCodec> compressionCodecProvider,
            CompressionCodecRegistry codecRegistry) {
        RedissonStreamProducer producer =
                RedissonStreamProducer.builder()
                        .redisson(redisson)
                        .namespace(properties.getNamespace())
                        .group(properties.getProducer().getGroup())
                        .converter(converter)
                        .defaultTimeoutMillis(properties.getProducer().getSendMessageTimeout())
                        .maxLen(properties.getProducer().getStreamMaxLen())
                        .compressThreshold(properties.getProducer().getCompressThreshold())
                        .maxMessageSize(properties.getProducer().getMaxMessageSize())
                        .build();
        CompressionCodec codec =
                resolveDefaultCompressionCodec(
                        compressionCodecProvider,
                        codecRegistry,
                        properties,
                        "RedissonStreamProducer");
        if (codec != null) {
            producer.setCompressionCodec(codec);
            LOG.debug("CompressionCodec injected into RedissonStreamProducer: {}", codec.name());
        }
        LOG.debug(
                "Creating RedissonStreamProducer: group={}, namespace={}",
                properties.getProducer().getGroup(),
                properties.getNamespace());
        return producer;
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
            RedissonClient redisson, MessageConverter converter, ConsumerTuning tuning) {
        LOG.debug("Creating RedissonStreamListenerFactory");
        return new RedissonStreamListenerFactory(redisson, converter, tuning);
    }

    /**
     * 消费调优参数对象（Parameter Object），被 {@link #streamMQListenerFactory} 与 Listener 容器消费。
     *
     * <p>默认实现 {@link DefaultConsumerTuning} 提供带下界保护的写入与哨兵解析；用户可通过 {@code @Bean} 覆盖以注入自定义调优策略。
     *
     * @return 消费调优配置
     */
    @Bean
    @ConditionalOnMissingBean(ConsumerTuning.class)
    public ConsumerTuning consumerTuning() {
        return new DefaultConsumerTuning();
    }

    /**
     * 默认 StreamMQTemplate：基于 Redisson 实现。
     *
     * <p>直接注入 {@link StreamMessageProducer} Bean，避免通过 Factory 间接创建带来的歧义。
     *
     * @param producer 生产者实例
     * @param converter 消息转换器
     * @param properties 配置
     * @return 模板
     */
    @Bean
    @ConditionalOnMissingBean(StreamMessageTemplate.class)
    public StreamMessageTemplate streamMQTemplate(
            StreamMessageProducer producer,
            MessageConverter converter,
            StreamMQProperties properties,
            ObjectProvider<TransactionScanner> transactionScannerProvider,
            ObjectProvider<StreamMQMetrics> metricsProvider,
            ObjectProvider<ProducerInterceptor> producerInterceptorProvider,
            @Qualifier("streammqExecutor") ExecutorService streammqExecutor) {
        String defaultGroup = properties.getProducer().getGroup();
        String txGroup = properties.getTransaction().getDefaultGroup();
        // ProducerConfig 仍需要：用于 Template 层读取 retryTimes / sendMessageTimeout 等发送参数
        ProducerConfig defaultConfig =
                ProducerConfig.builder()
                        .group(defaultGroup)
                        .namespace(properties.getNamespace())
                        .sendMessageTimeout(properties.getProducer().getSendMessageTimeout())
                        .streamMaxLen(properties.getProducer().getStreamMaxLen())
                        .compressThreshold(properties.getProducer().getCompressThreshold())
                        .maxMessageSize(properties.getProducer().getMaxMessageSize())
                        .retryTimes(properties.getProducer().getRetryTimes())
                        .build();
        LOG.debug(
                "Creating DefaultStreamMessageTemplate: defaultGroup={}, transactionGroup={},"
                        + " namespace={}",
                defaultGroup,
                txGroup,
                properties.getNamespace());
        DefaultStreamMessageTemplate template =
                new DefaultStreamMessageTemplate(
                        producer, defaultGroup, converter, defaultConfig, txGroup);
        template.setAsyncSendExecutor(streammqExecutor);
        TransactionScanner scanner =
                StreamMQBeanResolution.uniqueOrNull(
                        transactionScannerProvider, "TransactionScanner");
        if (scanner != null) {
            template.setTransactionScanner(scanner);
            LOG.debug(
                    "TransactionScanner injected into DefaultStreamMessageTemplate: full"
                            + " half-message flow enabled");
        }
        StreamMQMetrics metrics =
                StreamMQBeanResolution.uniqueOrNull(metricsProvider, "StreamMQMetrics");
        if (metrics != null) {
            template.setMetrics(metrics);
            LOG.debug(
                    "StreamMQMetrics injected into DefaultStreamMessageTemplate: send metrics"
                            + " enabled");
        }
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
            prefix = StreamMQSpringConstants.PROP_PREFIX_TRACING,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
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
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_TRACING,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE)
    public TraceCollector streamMQSlf4jTraceCollector() {
        LOG.debug("Using Slf4jTraceCollector (tracing enabled)");
        return new Slf4jTraceCollector();
    }

    /**
     * 默认广播消费组注册表：{@link RedissonBroadcastGroupRegistry}。
     *
     * <p>负责僵尸广播消费者组的回收（{@code XGROUP DESTROY} 释放已死实例占用的 PEL）与注册表计数。 遵循依赖倒置：调用方只依赖 {@link
     * BroadcastGroupRegistry} 接口，用户可注册自定义 Bean 覆盖—— 例如接入外部监控系统、改用不同的存储布局，或在不希望自动销毁消费者组的环境中提供空实现
     * （此时应自行承担 PEL 泄漏风险）。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @return BroadcastGroupRegistry 实例
     */
    @Bean
    @ConditionalOnMissingBean(BroadcastGroupRegistry.class)
    public BroadcastGroupRegistry streamMQBroadcastGroupRegistry(
            RedissonClient redisson,
            StreamMQProperties properties,
            ObjectProvider<BroadcastInstanceRegistry> instanceRegistryProvider) {
        LOG.debug("Using RedissonBroadcastGroupRegistry");
        BroadcastInstanceRegistry instanceRegistry =
                StreamMQBeanResolution.uniqueOrNull(
                        instanceRegistryProvider, "BroadcastInstanceRegistry");
        StreamMQProperties.Consumer consumer = properties.getConsumer();
        return new RedissonBroadcastGroupRegistry(
                redisson,
                properties.getNamespace(),
                RedissonBroadcastGroupRegistry.BROADCAST_GROUP_STALE_TTL_MS,
                RedissonBroadcastGroupRegistry.DEFAULT_MAX_SWEEP,
                instanceRegistry,
                consumer.getBroadcastLeaseTimeout().toMillis(),
                consumer.getBroadcastReclaimGrace().toMillis());
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
        LOG.info(
                "StreamMQ admin endpoints are secured by default (DenyAllAuthenticator). All"
                    + " management requests return 401. To enable access, register an"
                    + " AllowAllAuthenticator / BasicAuthAuthenticator / TokenAuthenticator bean."
                    + " See: https://github.com/HK-hub/StreamMQ#management-rest-api");
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
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_TRACING,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE)
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
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_TRACING,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE)
    public TraceContextConsumerInterceptor streamMQTraceContextConsumerInterceptor(
            TraceCollector traceCollector) {
        LOG.debug("Using TraceContextConsumerInterceptor (tracing enabled)");
        return new TraceContextConsumerInterceptor(traceCollector);
    }

    // SPI 类名已改为 Class<?> 属性，由 Spring Boot 启动时通过 ConfigurationProperties
    // 的 Class binding 自动校验，不再需要反射 instantiate/validateClass 方法。
}
