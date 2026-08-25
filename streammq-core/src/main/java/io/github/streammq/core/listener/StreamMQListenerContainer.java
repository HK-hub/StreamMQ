/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import java.util.Collection;

/**
 * StreamMQ 监听器容器接口。
 *
 * <p>管理所有 Listener 的生命周期、消费线程、Rebalance。 实现类位于 {@code streammq-redisson-adapter} 模块，建议继承 Spring
 * {@code SmartLifecycle}。
 *
 * <p>容器内部为每个注册项创建一个 {@link StreamMQListener}（监听器，负责 PULL 消息）， 拉取到的消息分发给业务层实现的 {@link
 * StreamMessageConcurrentlyConsumer}（消费者，onMessage 业务处理）。
 *
 * <p>注册 Consumer 时需提供注解元数据，框架据此创建对应的 Listener 与消费线程。 通过 {@link StreamMQConsumer#messageModel()}
 * 区分并发 / 顺序消费， 通过 {@link StreamMQConsumer#dlqMode()} 区分 DLQ 消费者。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQListenerContainer {

    /**
     * 注册一个并发消费 Consumer（包含普通、DLQ 场景；通过 annotation 的 dlqMode 区分）。
     *
     * <p>消费结果由 {@code onMessage} 返回值（{@link io.github.streammq.core.enums.ConsumeAction}）唯一表达，
     * 框架据此执行 ACK / 重试 / DLQ 路由。
     *
     * @param consumer Consumer 实例（{@link StreamMessageConcurrentlyConsumer}）
     * @param annotation 注解元数据（{@link StreamMQConsumer}）
     * @param <T> body 类型
     */
    <T> void registerConsumer(
            StreamMessageConcurrentlyConsumer<T> consumer, StreamMQConsumer annotation);

    /**
     * 注册一个顺序消费 Consumer。
     *
     * <p>消费结果由 {@code onMessage} 返回值（{@link ConsumeAction}）唯一表达。
     *
     * @param consumer Consumer 实例（{@link StreamMessageOrderlyConsumer}）
     * @param annotation 注解元数据（{@link StreamMQConsumer}，需 {@code messageModel = ORDERLY}）
     * @param <T> body 类型
     */
    <T> void registerOrderlyConsumer(
            StreamMessageOrderlyConsumer<T> consumer, StreamMQConsumer annotation);

    /**
     * 注册一个死信队列（DLQ）Consumer。
     *
     * <p>DLQ Consumer 返回 {@code void}，消费失败由 {@code DlqFailureStrategy} 决策。 与普通 Consumer 完全独立——不注册
     * ConsumerGroupManager / RetryScheduler / PelClaimScheduler。
     *
     * @param consumer Consumer 实例（必须同时实现 {@link DlqMessageConsumer}）
     * @param annotation DLQ 注解元数据
     * @param <T> body 类型
     */
    <T> void registerDlqConsumer(DlqMessageConsumer<T> consumer, StreamMQDlqConsumer annotation);

    /**
     * 注销指定 topic + 消费者组的监听器。
     *
     * <p>移除对应的注册项、取消其消费任务并释放组管理资源；主要用于动态绑定场景（如 Spring Cloud Stream binder 的
     * stop/rebind）。任何容器状态下调用均安全：运行中会先停止该监听器再注销，未注册时为幂等空操作。
     *
     * @param topic 主题
     * @param consumerGroup 消费者组
     */
    void unregister(String topic, String consumerGroup);

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

    /** 停止所有 Listener，释放线程与连接。 */
    void stop();

    /** 暂停所有 Listener（不释放资源，可恢复）。 */
    void pause();

    /** 恢复所有暂停的 Listener。 */
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
    record ConsumerMetadata(
            String topic, String consumerGroup, Class<?> consumerType, Class<?> bodyType) {}
}
