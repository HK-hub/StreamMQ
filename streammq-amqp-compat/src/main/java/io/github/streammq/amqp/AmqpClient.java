package io.github.streammq.amqp;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.serializer.ByteArraySerializer;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * AMQP 兼容层客户端入口。
 *
 * <p>对齐 RabbitMQ {@code ConnectionFactory} 风格，提供静态工厂方法
 * {@link #create(AmqpConfig)} 创建客户端实例，然后通过 {@link #newChannel()}
 * 获取 {@link AmqpChannel} 进行消息收发。
 *
 * <p>使用示例：
 * <pre>{@code
 * // 1. 配置
 * AmqpConfig config = AmqpConfig.builder()
 *     .redissonClient(redisson)
 *     .namespace("my-ns")
 *     .defaultExchange("my-exchange")
 *     .build();
 *
 * // 2. 创建客户端
 * AmqpClient client = AmqpClient.create(config);
 *
 * // 3. 获取 Channel
 * AmqpChannel ch = client.newChannel();
 * ch.exchangeDeclare("orders", "direct", true);
 * ch.queueDeclare("order-queue", true, false, false, null);
 * ch.queueBind("order-queue", "orders", "created");
 * ch.basicPublish("orders", "created", null, "hello".getBytes());
 * ch.close();
 *
 * // 4. 关闭客户端
 * client.close();
 * }</pre>
 *
 * <p>生命周期管理：
 * <ul>
 *   <li>{@link AmqpClient} 持有底层 {@link StreamMessageTemplate} 和
 *       {@link StreamMQListenerFactory}，所有 Channel 共享这些资源</li>
 *   <li>Channel 关闭不释放底层资源，仅停止自身的消费线程</li>
 *   <li>{@link #close()} 释放所有底层资源，之后不可再创建新 Channel</li>
 * </ul>
 *
 * <p>默认配置：
 * <ul>
 *   <li>消息序列化器：{@link ByteArraySerializer}（body 类型为 byte[]）</li>
 *   <li>消息转换器：{@link DefaultMessageConverter} + ByteArraySerializer</li>
 *   <li>生产者组名：{@code streammq-amqp-producer}</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AmqpClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpClient.class);

    /** 默认生产者组名 */
    private static final String DEFAULT_PRODUCER_GROUP = "streammq-amqp-producer";

    /** 底层 StreamMQ 消息模板（所有 Channel 共享） */
    private final StreamMessageTemplate template;

    /** 底层 StreamMQ 监听器工厂（所有 Channel 共享） */
    private final StreamMQListenerFactory listenerFactory;

    /** 命名空间 */
    private final String namespace;

    /** 默认 Exchange */
    private final String defaultExchange;

    /** 是否已关闭 */
    private volatile boolean closed = false;

    /**
     * 私有构造，通过 {@link #create(AmqpConfig)} 创建实例。
     *
     * @param template        StreamMQ 消息模板
     * @param listenerFactory StreamMQ 监听器工厂
     * @param namespace       命名空间
     * @param defaultExchange 默认 Exchange
     */
    private AmqpClient(StreamMessageTemplate template, StreamMQListenerFactory listenerFactory,
                       String namespace, String defaultExchange) {
        this.template = template;
        this.listenerFactory = listenerFactory;
        this.namespace = namespace;
        this.defaultExchange = defaultExchange;
    }

    /**
     * 静态工厂方法：根据配置创建 {@link AmqpClient} 实例。
     *
     * <p>内部完成以下初始化：
     * <ol>
     *   <li>确定 {@link MessageConverter}（优先使用配置中的，否则创建默认 byte[] 转换器）</li>
     *   <li>创建 {@link RedissonStreamProducerFactory} + {@link DefaultStreamMessageTemplate}</li>
     *   <li>创建 {@link RedissonStreamListenerFactory}</li>
     * </ol>
     *
     * @param config AMQP 配置（必填）
     * @return AmqpClient 实例
     * @throws NullPointerException 如果 config 为 null 或 config.redissonClient 为 null
     */
    public static AmqpClient create(AmqpConfig config) {
        Objects.requireNonNull(config, "config");

        // 1. 确定 MessageConverter
        MessageConverter converter = config.getMessageConverter();
        if (converter == null) {
            // 默认使用 ByteArraySerializer 构造 Converter（匹配 AMQP byte[] body 语义）
            converter = new DefaultMessageConverter(new ByteArraySerializer());
            LOG.info("No MessageConverter configured, using default DefaultMessageConverter + ByteArraySerializer");
        }

        // 2. 创建 ProducerFactory → StreamMessageTemplate
        RedissonStreamProducerFactory producerFactory =
                new RedissonStreamProducerFactory(config.getRedissonClient(), converter);

        DefaultStreamMessageTemplate template =
                new DefaultStreamMessageTemplate(producerFactory, DEFAULT_PRODUCER_GROUP, converter);

        // 3. 创建 ListenerFactory
        RedissonStreamListenerFactory listenerFactory =
                new RedissonStreamListenerFactory(config.getRedissonClient(), converter);

        LOG.info("AmqpClient created: namespace={}, defaultExchange={}",
                config.getNamespace(), config.getDefaultExchange());

        return new AmqpClient(template, listenerFactory,
                config.getNamespace(), config.getDefaultExchange());
    }

    /**
     * 创建新的 {@link AmqpChannel}。
     *
     * <p>所有 Channel 共享相同的底层 {@link StreamMessageTemplate} 和
     * {@link StreamMQListenerFactory}，避免重复创建连接资源。
     *
     * @return 新的 AmqpChannel 实例
     * @throws IllegalStateException 如果 Client 已关闭
     */
    public AmqpChannel newChannel() {
        if (closed) {
            throw new IllegalStateException("AmqpClient is closed, cannot create new channel");
        }
        return new AmqpChannel(template, listenerFactory, namespace, defaultExchange);
    }

    /**
     * 关闭客户端，释放底层资源。
     *
     * <p>依次执行：
     * <ol>
     *   <li>关闭 {@link StreamMQListenerFactory}（释放所有底层 Listener）</li>
     *   <li>关闭 {@link RedissonStreamProducerFactory}（释放 Producer）</li>
     * </ol>
     *
     * <p>注意：此方法不会关闭 {@code RedissonClient}（由外部管理其生命周期）。
     * 重复调用安全（幂等）。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        LOG.info("Closing AmqpClient...");

        if (listenerFactory != null) {
            try {
                listenerFactory.close();
            } catch (Exception ex) {
                LOG.warn("Error closing listenerFactory: {}", ex.getMessage(), ex);
            }
        }

        LOG.info("AmqpClient closed");
    }

    /**
     * 返回底层 {@link StreamMessageTemplate}。
     *
     * @return 消息模板
     */
    public StreamMessageTemplate getTemplate() {
        return template;
    }

    /**
     * 返回底层 {@link StreamMQListenerFactory}。
     *
     * @return 监听器工厂
     */
    public StreamMQListenerFactory getListenerFactory() {
        return listenerFactory;
    }

    /**
     * 返回命名空间。
     *
     * @return namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * 返回默认 Exchange。
     *
     * @return defaultExchange
     */
    public String getDefaultExchange() {
        return defaultExchange;
    }
}
