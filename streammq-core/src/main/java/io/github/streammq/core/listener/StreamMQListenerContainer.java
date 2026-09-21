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
 * <p>管理所有 Listener 的生命周期、消费线程、Rebalance。 实现类位于 {@code streammq-redisson} 模块，建议继承 Spring {@code
 * SmartLifecycle}。
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
     * 是否存在消费循环启动失败（{@code loopKey → 原因} 的非空集合）。
     *
     * <p><b>为什么必须在接口上：</b>"容器在跑但某些消费循环启动失败"是典型静默故障——这些消费者在注册表里 可见、容器 {@link #isRunning()} 为
     * true，却永不消费。只看 {@code isRunning()} 的健康检查 / K8s 就绪探针 会给出<b>假健康 / 假就绪</b>，把流量导入一个不消费的
     * Pod。把该能力纳入容器契约后， starter、Binder、Kubernetes 三个健康面可用同一判据，而无需向下强转具体实现类。
     *
     * <p>默认实现返回空 map（第三方容器实现不受影响），具体实现覆盖为真实登记表快照。
     *
     * @return 不可修改的失败快照（{@code loopKey → 原因}），空表示全部正常
     * @since 0.1.2
     */
    default java.util.Map<String, String> getConsumeLoopFailures() {
        return java.util.Map.of();
    }

    /**
     * 是否全部消费循环健康（等价于 {@link #getConsumeLoopFailures()} 为空）。
     *
     * @return true 表示不存在启动失败的消费循环
     * @since 0.1.2
     */
    default boolean isConsumeLoopsHealthy() {
        return getConsumeLoopFailures().isEmpty();
    }

    // ===================== 运行时管理（可选，默认空实现） =====================

    /**
     * 触发指定消费者组的重平衡。
     *
     * @param group 消费者组名
     * @return true 如果重平衡已执行
     */
    default boolean rebalanceGroup(String group) {
        return false;
    }

    /** 设置背压队列容量（运行时调参）。 */
    default void setInflightCapacity(int capacity) {}

    /** 设置暂停休眠间隔（运行时调参）。 */
    default void setPausedSleepMillis(long millis) {}

    /** 设置 Broker 异常退避间隔（运行时调参）。 */
    default void setBrokerErrorBackoffMillis(long millis) {}

    /** 设置消费超时取消后的宽限期（运行时调参）。 */
    default void setTimeoutCancelGraceMillis(long millis) {}

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
