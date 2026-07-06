package io.github.streammq.core.listener;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;

import java.util.Collection;

/**
 * StreamMQ 监听器容器接口。
 *
 * <p>管理所有 Listener 的生命周期、消费线程、Rebalance。
 * 实现类位于 {@code streammq-redisson-adapter} 模块，建议继承 Spring {@code SmartLifecycle}。
 *
 * <p>容器内部为每个注册项创建一个 {@link StreamMQListener}（监听器，负责 PULL 消息），
 * 拉取到的消息分发给业务层实现的 {@link StreamMessageConcurrentlyConsumer}（消费者，onMessage 业务处理）。
 *
 * <p>注册 Consumer 时需提供注解元数据，框架据此创建对应的 Listener 与消费线程。
 * 通过 {@link StreamMQConsumer#messageModel()} 区分并发 / 顺序消费，
 * 通过 {@link StreamMQConsumer#dlqConsumerGroup()} 区分 DLQ 消费者。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQListenerContainer {

    /**
     * 注册一个并发消费 Consumer（包含普通、DLQ 场景；通过 annotation 的 dlqConsumerGroup 区分）。
     *
     * <p>{@link StreamMQConsumer#acknowledgeMode()} 为 {@code MANUAL} 时表示手动 ACK 模式，
     * 框架将忽略 {@code onMessage} 返回值，由 Consumer 通过 {@code context.acknowledge()} 控制 ACK。
     *
     * @param consumer Consumer 实例（{@link StreamMessageConcurrentlyConsumer}）
     * @param annotation 注解元数据（{@link StreamMQConsumer}）
     * @param <T> body 类型
     */
    <T> void registerConsumer(StreamMessageConcurrentlyConsumer<T> consumer,
                              StreamMQConsumer annotation);

    /**
     * 注册一个顺序消费 Consumer。
     *
     * <p>{@link StreamMQConsumer#acknowledgeMode()} 为 {@code MANUAL} 时表示手动 ACK 模式，
     * 框架将忽略 {@code onMessage} 返回值，由 Consumer 通过 {@code context.acknowledge()} 控制 ACK。
     *
     * @param consumer Consumer 实例（{@link StreamMessageOrderlyConsumer}）
     * @param annotation 注解元数据（{@link StreamMQConsumer}，需 {@code messageModel = ORDERLY}）
     * @param <T> body 类型
     */
    <T> void registerOrderlyConsumer(StreamMessageOrderlyConsumer<T> consumer,
                                     StreamMQConsumer annotation);

    /**
     * 返回所有已注册的 Consumer 元信息。
     *
     * @return 不可修改的元信息集合
     */
    Collection<ConsumerMetadata> getConsumers();

    /**
     * 启动所有 Listener。
     *
     * @throws io.github.streammq.core.exception.StreamMQException 如果启动失败
     */
    void start();

    /**
     * 停止所有 Listener，释放线程与连接。
     */
    void stop();

    /**
     * 暂停所有 Listener（不释放资源，可恢复）。
     */
    void pause();

    /**
     * 恢复所有暂停的 Listener。
     */
    void resume();

    /**
     * 返回容器是否正在运行。
     *
     * @return true 如果运行中
     */
    boolean isRunning();

    /**
     * Consumer 元信息。
     *
     * @param topic 主题
     * @param consumerGroup 消费者组
     * @param consumerType consumer 类型
     * @param bodyType body 类型
     */
    record ConsumerMetadata(String topic, String consumerGroup, Class<?> consumerType, Class<?> bodyType) {
    }
}
