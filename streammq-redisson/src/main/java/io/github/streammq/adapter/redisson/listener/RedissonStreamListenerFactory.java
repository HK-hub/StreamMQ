/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.adapter.redisson.container.ConsumerTuning;
import io.github.streammq.adapter.redisson.container.DefaultConsumerTuning;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redisson 的 {@link StreamMQListenerFactory} 默认实现。
 *
 * <p>持有共享的 {@link RedissonClient} 与 {@link MessageConverter}，按 {@link ListenerConfig} 创建 {@link
 * RedissonStreamListener} 实例。
 *
 * <p>配置项（参见 {@link ListenerConfig}）：
 *
 * <ul>
 *   <li>{@code topic} - 主题（必填）
 *   <li>{@code consumerGroup} - 消费者组名（必填）
 *   <li>{@code consumerName} - 消费者实例名（可选，默认自动生成 UUID 后缀）
 *   <li>{@code namespace} - 命名空间（可选，默认空字符串）
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RequiredArgsConstructor
public class RedissonStreamListenerFactory implements StreamMQListenerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamListenerFactory.class);

    @NonNull private final RedissonClient redisson;
    @NonNull private final MessageConverter converter;
    @NonNull private final ConsumerTuning tuning;

    /**
     * 便捷构造：使用默认 {@link DefaultConsumerTuning}。供测试与不需要自定义调优策略的调用方使用。
     *
     * @param redisson Redisson 客户端
     * @param converter 消息转换器
     */
    public RedissonStreamListenerFactory(RedissonClient redisson, MessageConverter converter) {
        this(redisson, converter, new DefaultConsumerTuning());
    }

    /**
     * 已创建 listener 的保活集合（用于 close 时统一关闭）。
     *
     * <p><b>不使用以 listener 为键的 Map：</b>{@link RedissonStreamListener} 重写了 {@code equals/hashCode} （按
     * topic/group/consumerName/mode 计算），若两个 listener 仅 dlqMode 等差异相同，则会被判为相等导致 Map 覆盖、 close
     * 时漏关并泄漏资源。改用 FIFO 队列，createListener 每次恰好入队一个，close 时全部关闭， 无去重依赖、无碰撞。 由于 {@code equals} 仅用于
     * {@code @EqualsAndHashCode} 兼容场景，此处以入队顺序为唯一标识。
     */
    private final ConcurrentLinkedQueue<RedissonStreamListener> listeners =
            new ConcurrentLinkedQueue<>();

    private volatile boolean closed = false;

    /**
     * 创建/关闭的互斥锁（生命周期状态机）。
     *
     * <p><b>为什么必须加锁（发布前红队审查 R5）：</b>{@link #createListener} 是"检查 closed → 构建监听器 → 入队"三步复合操作。仅靠
     * volatile 检查时存在窗口：线程 A 通过检查后、入队前，线程 B 执行完 {@link #close()} 的排空，A
     * 新构建的监听器便逃逸出队列永不关闭——其租约心跳线程持续续租 Redis 实例槽位，让已停机实例的槽位被视为 active、阻碍回收。加锁后"检查"与"入队"原子，{@link
     * #close()} 的"置位 + 排空"也原子，二者必有一方先看到对方。
     */
    private final Object lifecycleLock = new Object();

    /**
     * 广播实例注册中心（可选）：注入后，由本工厂创建的广播监听器会在心跳时续租其持久化实例身份槽位。
     *
     * <p>不注入时广播消费者组仍可工作，但槽位在注册表中的 {@code lastHeartbeat} 会一直停留在启动时， 运行中的实例会落进"可回收"窗口，同主机上另一个同 group
     * 的广播消费者可能抢走该槽位（广播退化为集群语义）。
     */
    private volatile io.github.streammq.core.broadcast.BroadcastInstanceRegistry
            broadcastInstanceRegistry;

    /**
     * 设置广播实例注册中心。
     *
     * @param registry 注册中心，null 表示不续租实例身份
     */
    public void setBroadcastInstanceRegistry(
            io.github.streammq.core.broadcast.BroadcastInstanceRegistry registry) {
        this.broadcastInstanceRegistry = registry;
    }

    /**
     * 返回当前广播实例注册中心（可能为 null）。
     *
     * @return 注册中心
     */
    public io.github.streammq.core.broadcast.BroadcastInstanceRegistry
            getBroadcastInstanceRegistry() {
        return broadcastInstanceRegistry;
    }

    @Override
    public StreamMQListener createListener(ListenerConfig config) {
        if (closed) {
            throw new IllegalStateException("ListenerFactory is closed");
        }
        Objects.requireNonNull(config, "config");
        String topic = config.getTopic();
        if (StringUtils.isEmpty(topic)) {
            throw new IllegalArgumentException("Missing required property: topic");
        }
        String group = config.getConsumerGroup();
        if (StringUtils.isEmpty(group)) {
            throw new IllegalArgumentException("Missing required property: consumerGroup");
        }
        String consumerName = config.getConsumerName();
        if (StringUtils.isEmpty(consumerName)) {
            consumerName = group + "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        String namespace = config.getNamespace();
        if (Objects.isNull(namespace)) {
            namespace = "";
        }

        RedissonStreamListener listener =
                RedissonStreamListener.builder()
                        .redisson(redisson)
                        .namespace(namespace)
                        .topic(topic)
                        .group(group)
                        .consumerName(consumerName)
                        .converter(
                                Objects.nonNull(config.getConverter())
                                        ? config.getConverter()
                                        : converter)
                        .dlqMode(config.isDlqMode())
                        .retryMode(config.isRetryMode())
                        .broadcast(config.isBroadcast())
                        .targetBodyType(config.getTargetBodyType())
                        .consumeFromWhere(config.getConsumeFromWhere())
                        .maxBatchSizeLimit(tuning.maxBatchSizeLimit())
                        .broadcastInstanceRegistry(broadcastInstanceRegistry)
                        .build();
        synchronized (lifecycleLock) {
            if (closed) {
                // 构建期间工厂被关闭：立即自关闭并报错，绝不逃逸出 close() 的排空范围
                closeQuietly(listener);
                throw new IllegalStateException("ListenerFactory is closed");
            }
            listeners.add(listener);
        }
        LOG.debug(
                "Listener created: topic={}, group={}, consumer={}, dlqMode={}, retryMode={},"
                        + " consumeFromWhere={}",
                topic,
                group,
                consumerName,
                config.isDlqMode(),
                config.isRetryMode(),
                config.getConsumeFromWhere());
        return listener;
    }

    @Override
    public void close() {
        int total;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            total = listeners.size();
            RedissonStreamListener listener;
            while ((listener = listeners.poll()) != null) {
                closeQuietly(listener);
            }
        }
        LOG.info("RedissonStreamListenerFactory closed, total listeners: {}", total);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /** 重新打开工厂（容器 restart 场景）：close 之后所有 listener 已关闭，重置内部状态即可继续创建新 listener。 */
    public void reopen() {
        synchronized (lifecycleLock) {
            closed = false;
        }
        LOG.info("RedissonStreamListenerFactory reopened");
    }

    /** 关闭单个监听器，异常只记录不外抛（close 语义为尽力释放）。 */
    private static void closeQuietly(RedissonStreamListener listener) {
        try {
            listener.close();
        } catch (RuntimeException ex) {
            LOG.warn("Failed to close listener: {}", ex.getMessage(), ex);
        }
    }
}
