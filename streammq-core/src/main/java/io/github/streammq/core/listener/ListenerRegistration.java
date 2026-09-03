/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

import io.github.streammq.core.consumer.StreamMessageConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * Listener 注册信息接口（值对象）。
 *
 * <p>封装容器在注册 Listener 时所需的全量配置，包括监听类型、消费参数、重试策略、 顺序消费分片锁、DLQ 模式标志、跨平台 body 类型等。
 *
 * <p>使用 Builder 模式构造（参见 {@link ListenerRegistration.Builder}），避免多参数构造器的可读性问题。
 *
 * @apiNote <b>线程封闭约定</b>：本对象名义上是值对象，但保留少量 setter （如 {@link #setNamespace(String)}、{@link
 *     #setConverterInstance(MessageConverter)}） 供容器在启动前回填运行时字段。调用方必须在将注册信息交给容器之前完成全部变更；
 *     容器注册完成后将其视为不可变对象，此后再修改属于未定义行为。{@code getShardLocks()} 等集合访问器返回不可修改视图/防御性拷贝。
 * @param <T> Listener 处理的 body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ListenerRegistration<T> {

    ListenerType getType();

    StreamMessageConsumer<T> getConsumer();

    String getTopic();

    String getGroup();

    ConsumeMode getConsumeMode();

    int getMaxReconsumeTimes();

    int getShardCount();

    long getConsumeTimeoutMillis();

    /**
     * 返回顺序消费单条消息消费超时（毫秒），0 表示不启用超时。
     *
     * <p>仅对顺序消费（{@link ListenerType#ORDERLY}）生效；超时后按 {@code RECONSUME_LATER} 重试， 耗尽 {@link
     * #getMaxReconsumeTimes()} 次后进入 DLQ。业务 handler 不响应中断时原线程可能继续运行， 业务层必须保证幂等。
     *
     * @return 超时毫秒数，0 表示不启用（默认）
     */
    long getOrderlyConsumeTimeoutMillis();

    /**
     * 返回顺序消费分片锁（不可修改视图；null 表示未设置）。
     *
     * @return 分片锁列表
     */
    List<Lock> getShardLocks();

    int getPullBatchSize();

    long getPullBlockTimeoutMillis();

    long getPullIntervalMillis();

    String getSelectorExpression();

    Class<? extends MessageSerializer> getSerializer();

    Class<? extends RetryPolicy> getRetryPolicy();

    Class<? extends MessageConverter> getMessageConverter();

    Class<? extends RebalanceStrategy> getRebalanceStrategy();

    long getSuspendCurrentQueueTimeMillis();

    int getStreamMaxLen();

    boolean isEnableMsgTrace();

    boolean isDlqMode();

    Class<?> getTargetBodyType();

    Class<? extends DlqFailureStrategy> getDlqFailureStrategy();

    Class<? extends ConsumerFilter>[] getConsumerFilter();

    SelectorType getSelectorType();

    String getNamespace();

    void setNamespace(String namespace);

    /** 命名空间解析：为空时回填默认命名空间。 */
    void resolveNamespace(String defaultNs);

    /** 注册唯一键（DLQ 模式带 {@code dlq:} 前缀）。 */
    String key();

    /**
     * 并发消费循环数下限（原线程池语义，现为读循环数）：仅 CONCURRENT 集群消费生效。
     *
     * @return 并发数（&gt;= 1，构造时夹取下界 1）
     */
    int getConsumeThreadMin();

    /**
     * 并发消费循环数上限（构造时夹取至 &gt;= {@link #getConsumeThreadMin()}）。
     *
     * <p>注意：与 {@link ListenerConfig} 的校验策略不同——注册模型对非法值「夹取」以保证运行期弹性， ListenerConfig 构造器则直接抛出
     * IllegalArgumentException。
     *
     * @return 上限（&gt;= 下限）
     */
    int getConsumeThreadMax();

    /**
     * 底层 Redis 消费者名；null 表示由适配层自动生成（group + 实例后缀）。
     *
     * @return 消费者名，可为 null
     */
    default String getConsumerName() {
        return null;
    }

    /**
     * 是否为 retry Stream 监听（对齐 RocketMQ %RETRY%{group}%）。
     *
     * @return true 表示 retry 监听
     */
    default boolean isRetryMode() {
        return false;
    }

    /**
     * 新消费者组的起始消费位点（默认 {@link io.github.streammq.core.enums.ConsumeFromWhere#DEFAULT}）。
     *
     * <p>仅在该 Redis 消费者组<b>首次创建</b>时生效，已存在的组不受影响。缺省实现返回全局默认， 保证未声明该字段的自定义实现 / 测试桩不会因接口新增方法而编译失败。
     *
     * @return 起始消费位点策略，永不为 null
     */
    default io.github.streammq.core.enums.ConsumeFromWhere getConsumeFromWhere() {
        return io.github.streammq.core.enums.ConsumeFromWhere.DEFAULT;
    }

    /**
     * per-consumer 已解析转换器实例；null 表示使用全局转换器。
     *
     * @return 转换器实例，可为 null
     */
    MessageConverter getConverterInstance();

    /** 由容器在 per-consumer SPI 解析后回填转换器实例。 */
    void setConverterInstance(MessageConverter converter);

    /** 唯一构造入口：委托 {@link DefaultListenerRegistration.Builder}（字段校验集中于此）。 */
    static <T> DefaultListenerRegistration.Builder<T> builder() {
        return new DefaultListenerRegistration.Builder<>();
    }
}
