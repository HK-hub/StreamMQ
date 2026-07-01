package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.consumer.RedissonStreamConsumerFactory;
import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.template.DefaultStreamMqTemplate;
import io.github.streammq.core.producer.StreamMqProducerFactory;
import io.github.streammq.core.consumer.StreamMqConsumerFactory;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.MessageSerializer;
import io.github.streammq.core.spi.RetryPolicy;
import io.github.streammq.core.template.StreamMqTemplate;
import io.github.streammq.spring.boot.properties.StreamMqProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * StreamMQ 核心自动装配：注册 Serializer / Converter / RetryPolicy / ProducerFactory /
 * ConsumerFactory / Template 等基础 Bean。
 *
 * <p>装配条件：
 * <ul>
 *   <li>{@code streammq.enabled=true}（默认 true）</li>
 *   <li>classpath 存在 {@link RedissonClient} 与 {@link StreamMqTemplate}</li>
 *   <li>存在已注册的 {@link RedissonClient} Bean（通常来自 {@code redisson-spring-boot-starter}）</li>
 * </ul>
 *
 * <p>所有核心 Bean 均标注 {@code @ConditionalOnMissingBean}，用户可在自定义配置类中覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({RedissonClient.class, StreamMqTemplate.class})
@EnableConfigurationProperties(StreamMqProperties.class)
public class StreamMqCoreAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqCoreAutoConfiguration.class);

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
    public MessageSerializer<?> streamMqMessageSerializer(StreamMqProperties properties) {
        String className = properties.getProducer().getSerializer();
        if (className == null || className.isEmpty()
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
     * @return 消息转换器
     */
    @Bean
    @ConditionalOnMissingBean(MessageConverter.class)
    public MessageConverter streamMqMessageConverter(MessageSerializer<?> serializer) {
        LOG.info("Using DefaultMessageConverter with serializer={}", serializer.getClass().getSimpleName());
        return new DefaultMessageConverter(serializer);
    }

    /**
     * 默认重试策略：根据 {@code streammq.retry.policy} 配置加载。
     *
     * @param properties 配置
     * @return 重试策略
     */
    @Bean
    @ConditionalOnMissingBean(RetryPolicy.class)
    public RetryPolicy streamMqRetryPolicy(StreamMqProperties properties) {
        String className = properties.getRetry().getPolicy();
        LOG.info("Using RetryPolicy: {}", className);
        return instantiate(className, RetryPolicy.class);
    }

    /**
     * 生产者工厂：基于 Redisson 实现。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     * @return 生产者工厂
     */
    @Bean
    @ConditionalOnMissingBean(StreamMqProducerFactory.class)
    public StreamMqProducerFactory streamMqProducerFactory(RedissonClient redisson, MessageConverter converter) {
        LOG.info("Creating RedissonStreamProducerFactory");
        return new RedissonStreamProducerFactory(redisson, converter);
    }

    /**
     * 消费者工厂：基于 Redisson 实现。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     * @return 消费者工厂
     */
    @Bean
    @ConditionalOnMissingBean(StreamMqConsumerFactory.class)
    public StreamMqConsumerFactory streamMqConsumerFactory(RedissonClient redisson, MessageConverter converter) {
        LOG.info("Creating RedissonStreamConsumerFactory");
        return new RedissonStreamConsumerFactory(redisson, converter);
    }

    /**
     * 默认 StreamMqTemplate：基于 Redisson 实现。
     *
     * @param producerFactory 生产者工厂
     * @param converter 消息转换器
     * @param properties 配置
     * @return 模板
     */
    @Bean
    @ConditionalOnMissingBean(StreamMqTemplate.class)
    public StreamMqTemplate<?> streamMqTemplate(StreamMqProducerFactory producerFactory,
                                                 MessageConverter converter,
                                                 StreamMqProperties properties) {
        String defaultGroup = properties.getProducer().getGroup();
        String txGroup = properties.getTransaction().getDefaultGroup();
        // 注入 namespace / send-message-timeout / stream.max-len 到 defaultProperties,
        // 保证 Producer 与 ListenerContainer 使用相同的 namespace,避免消息写入与读取 Key 不一致。
        java.util.Map<String, Object> defaultProps = new java.util.HashMap<>(4);
        defaultProps.put("namespace", properties.getNamespace());
        defaultProps.put("send-message-timeout", properties.getProducer().getSendMessageTimeout());
        defaultProps.put("stream.max-len", properties.getProducer().getStreamMaxLen());
        LOG.info("Creating DefaultStreamMqTemplate: defaultGroup={}, transactionGroup={}, namespace={}",
            defaultGroup, txGroup, properties.getNamespace());
        return new DefaultStreamMqTemplate<>(
            producerFactory, defaultGroup, converter, defaultProps, txGroup);
    }

    /**
     * 注册 {@link StreamMqProducerBeanPostProcessor}，处理 {@code @StreamMqProducer} 字段注入。
     *
     * @param producerFactory 生产者工厂
     * @param converter 消息转换器
     * @param properties 配置
     * @return BeanPostProcessor
     */
    @Bean
    public StreamMqProducerBeanPostProcessor streamMqProducerBeanPostProcessor(
            StreamMqProducerFactory producerFactory,
            MessageConverter converter,
            StreamMqProperties properties) {
        return new StreamMqProducerBeanPostProcessor(producerFactory, converter, properties);
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
        if (className == null || className.isEmpty()) {
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
