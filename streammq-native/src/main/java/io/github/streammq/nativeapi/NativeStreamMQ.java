package io.github.streammq.nativeapi;

import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import org.redisson.api.RedissonClient;

import java.util.Objects;

/**
 * StreamMQ 原生 API 入口类，不依赖 Spring 容器。
 *
 * <p>使用建造者模式构造，内部持有 {@link RedissonClient}、命名空间和
 * {@link MessageConverter}，并管理 Producer 和 Listener 工厂的生命周期。
 * 提供创建 {@link NativeProducer} 的工厂方法，消费者可通过
 * {@link NativeConsumer} 构造函数直接创建。
 *
 * <p>使用示例（非 Spring 环境）：
 * <pre>{@code
 * NativeStreamMQ streamMQ = NativeStreamMQ.builder()
 *     .redisson(redissonClient)
 *     .namespace("my-app")
 *     .converter(new DefaultMessageConverter(new JacksonJsonSerializer()))
 *     .build();
 *
 * NativeProducer producer = streamMQ.createProducer("my-group");
 * SendResult result = producer.send("my-topic", payload);
 *
 * NativeConsumer<String> consumer = new NativeConsumer<>(
 *     streamMQ, "my-topic", "my-group", "consumer-1");
 * List<Message<String>> messages = consumer.poll(10, Duration.ofSeconds(5));
 * messages.forEach(m -> consumer.ack(m.getMessageId()));
 * }</pre>
 *
 * <p>线程安全：所有字段均为 final，工厂实现自身保证线程安全。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NativeStreamMQ {

    private final RedissonClient redisson;
    private final String namespace;
    private final MessageConverter converter;
    private final StreamMessageProducerFactory producerFactory;
    private final StreamMQListenerFactory listenerFactory;

    /**
     * 包级私有构造，由 {@link NativeStreamMQBuilder} 调用。
     *
     * @param redisson  Redisson 客户端，不能为 null
     * @param namespace 命名空间，可为 null（默认空字符串）
     * @param converter 消息转换器，不能为 null
     * @throws NullPointerException 如果 redisson 或 converter 为 null
     */
    NativeStreamMQ(RedissonClient redisson, String namespace, MessageConverter converter) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.namespace = (namespace == null) ? "" : namespace;
        this.converter = Objects.requireNonNull(converter, "converter");
        this.producerFactory = new RedissonStreamProducerFactory(redisson, converter);
        this.listenerFactory = new RedissonStreamListenerFactory(redisson, converter);
    }

    /**
     * 创建 {@link NativeStreamMQBuilder} 建造者实例。
     *
     * @return 新的建造者实例
     */
    public static NativeStreamMQBuilder builder() {
        return new NativeStreamMQBuilder();
    }

    /**
     * 创建一个指定生产者组的 {@link NativeProducer}。
     *
     * <p>Producer 实例由底层 {@link RedissonStreamProducerFactory} 按 group 缓存，
     * 同一 group 多次调用返回包装不同 {@link StreamMessageProducer} 实例的
     * {@link NativeProducer}（底层 Producer 实例共享）。
     *
     * @param group 生产者组名，不能为 null 或空字符串
     * @return 原生生产者实例
     * @throws NullPointerException     如果 group 为 null
     * @throws IllegalArgumentException 如果 group 为空字符串
     */
    public NativeProducer createProducer(String group) {
        Objects.requireNonNull(group, "group");
        if (group.isEmpty()) {
            throw new IllegalArgumentException("group must not be empty");
        }
        ProducerConfig config = ProducerConfig.builder()
                .group(group)
                .namespace(namespace)
                .build();
        StreamMessageProducer producer = producerFactory.createProducer(config);
        return new NativeProducer(producer);
    }

    /**
     * 返回 Redisson 客户端。
     *
     * @return Redisson 客户端实例，不可为 null
     */
    public RedissonClient getRedisson() {
        return redisson;
    }

    /**
     * 返回命名空间。
     *
     * @return 命名空间，不可为 null（未设置时返回空字符串）
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * 返回消息转换器。
     *
     * @return 消息转换器实例，不可为 null
     */
    public MessageConverter getConverter() {
        return converter;
    }

    /**
     * 返回监听器工厂（包级私有，供同包的 {@link NativeConsumer} 使用）。
     *
     * @return 监听器工厂实例
     */
    StreamMQListenerFactory getListenerFactory() {
        return listenerFactory;
    }
}
