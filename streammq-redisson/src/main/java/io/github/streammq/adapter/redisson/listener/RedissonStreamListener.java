/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.listener;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Redisson 的 {@link StreamMQListener} 默认实现。
 *
 * <p>底层调用 {@link RStream#readGroup} 拉取消息，{@link RStream#ack} 确认消息。 每个实例绑定一个 Topic + ConsumerGroup +
 * ConsumerName。
 *
 * <p>支持：
 *
 * <ul>
 *   <li>非阻塞拉取 {@link #pull}（基于 {@code XREADGROUP > COUNT n}）
 *   <li>阻塞拉取 {@link #pullBlock}（基于 {@code XREADGROUP > COUNT n BLOCK ms}）
 *   <li>单条/批量 ACK {@link #ack} / {@link #ackBatch}
 *   <li>消费者组自动创建（首次拉取时 lazy init，{@code MKSTREAM}）
 * </ul>
 *
 * <p>线程安全：所有字段均为 final 或线程安全类型，可在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@EqualsAndHashCode(
        of = {"namespace", "topic", "group", "consumerName", "dlqMode", "retryMode", "broadcast"})
public class RedissonStreamListener implements StreamMQListener {

    private static final Logger LOG = LoggerFactory.getLogger(RedissonStreamListener.class);

    /**
     * Class.forName 缓存，避免每条消息重复类加载查找（正结果缓存，负结果不缓存）。
     *
     * <p><b>实例级而非静态：</b>若以 simpleName 为键做成 JVM 级静态缓存，两个不同监听器 （不同 topic /
     * 不同目标类型）会发生跨实例缓存污染——先解析到的类被错误地提供给 另一个监听器，造成反序列化类型混淆。实例级缓存将作用域限制在单个监听器内。
     */
    private final ConcurrentMap<String, Class<?>> classCache = new ConcurrentHashMap<>();

    private final @NonNull RedissonClient redisson;
    private final String namespace;
    private final @NonNull String topic;
    private final @NonNull String group;
    private final @NonNull String consumerName;
    private final @NonNull MessageConverter converter;

    /** DLQ 模式标志：true=从 DLQ Stream 消费死信消息 */
    private final boolean dlqMode;

    /** Retry 模式标志：true=从 retry Stream 消费重试消息（对齐 RocketMQ %RETRY%{group}%） */
    private final boolean retryMode;

    /** 广播消费模式标志：true=每个消费者实例使用独立的消费者组 */
    private final boolean broadcast;

    /**
     * 目标 body 类型（跨平台反序列化回退类型）。
     *
     * <p>当 Stream Entry 缺失 {@code bodyType} 字段（发送方非 StreamMQ SDK）， 或 {@code bodyType}
     * 类不可加载时，回退到此类型。若仍为 null，则回退到 {@link String}。
     */
    private final Class<?> targetBodyType;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean groupCreated = new AtomicBoolean(false);

    /** batchSize 校验上界，对应 Redis Stream 单次 XREADGROUP 的合理上限 */
    private static final int MAX_BATCH_SIZE = StreamMQConstants.MAX_BATCH_SIZE_LIMIT;

    /** BUSYGROUP 错误标识，用于判断消费者组已存在 */
    private static final String BUSYGROUP_MARKER = "BUSYGROUP";

    /** DLQ 转存字段：进入死信的原因编码（对齐 DlqReason 线上协议） */
    private static final String FIELD_DLQ_REASON = "dlqReason";

    /** NOGROUP 错误标识，用于判断 Stream 或消费者组被删除的情况 */
    private static final String NOGROUP_MARKER = "NOGROUP";

    /**
     * 兼容构造器：不启用 DLQ/retry/broadcast 模式（等价于全部 false）。
     *
     * @param redisson Redisson 客户端（必填）
     * @param namespace 命名空间（可为 null，默认空字符串）
     * @param topic 主题（必填）
     * @param group 消费者组名（必填）
     * @param consumerName 消费者实例名（必填）
     * @param converter 消息转换器（必填）
     */
    public RedissonStreamListener(
            @NonNull RedissonClient redisson,
            String namespace,
            @NonNull String topic,
            @NonNull String group,
            @NonNull String consumerName,
            @NonNull MessageConverter converter) {
        this(redisson, namespace, topic, group, consumerName, converter, false, false, false, null);
    }

    /**
     * 构造 Listener，支持 Builder 模式。
     *
     * <p>使用示例：
     *
     * <pre>{@code
     * RedissonStreamListener listener = RedissonStreamListener.builder()
     *     .redisson(redissonClient)
     *     .topic("my-topic")
     *     .group("my-group")
     *     .consumerName("consumer-1")
     *     .converter(converter)
     *     .build();
     * }</pre>
     *
     * <p>DLQ 模式示例：
     *
     * <pre>{@code
     * RedissonStreamListener dlqListener = RedissonStreamListener.builder()
     *     .redisson(redissonClient)
     *     .topic("my-topic")              // 原始 topic
     *     .group("my-group")              // 原始消费者组（用于构造 DLQ Stream Key）
     *     .consumerName("dlq-consumer-1")
     *     .converter(converter)
     *     .dlqMode(true)
     *     .build();
     * }</pre>
     *
     * <p>跨平台 body 类型示例：
     *
     * <pre>{@code
     * RedissonStreamListener listener = RedissonStreamListener.builder()
     *     .redisson(redissonClient)
     *     .topic("cross-lang-topic")
     *     .group("my-group")
     *     .consumerName("consumer-1")
     *     .converter(converter)
     *     .targetBodyType(String.class)   // Go 发送 JSON string，接收为 String 自行解析
     *     .build();
     * }</pre>
     *
     * @param redisson Redisson 客户端（必填）
     * @param namespace 命名空间（可为 null，默认空字符串）
     * @param topic 主题（必填；DLQ 模式下为原始 topic）
     * @param group 消费者组名（必填；DLQ 模式下为 DLQ 消费者组名）
     * @param consumerName 消费者实例名（必填）
     * @param converter 消息转换器（必填）
     * @param dlqMode DLQ 模式标志（true=从 DLQ Stream 消费）
     * @param targetBodyType 目标 body 类型（跨平台回退类型，null=最终回退到 String）
     */
    @Builder
    public RedissonStreamListener(
            @NonNull RedissonClient redisson,
            String namespace,
            @NonNull String topic,
            @NonNull String group,
            @NonNull String consumerName,
            @NonNull MessageConverter converter,
            boolean dlqMode,
            boolean retryMode,
            boolean broadcast,
            Class<?> targetBodyType) {
        this.redisson = redisson;
        this.namespace = Objects.isNull(namespace) ? "" : namespace;
        this.topic = topic;
        this.group = group;
        this.consumerName = consumerName;
        this.converter = converter;
        this.dlqMode = dlqMode;
        this.retryMode = retryMode;
        this.broadcast = broadcast;
        this.targetBodyType = targetBodyType;
    }

    @Override
    public List<Message<?>> pull(int batchSize) {
        ensureOpen();
        validateBatchSize(batchSize);
        ensureGroup();
        return doRead(batchSize, null);
    }

    @Override
    public List<Message<?>> pullBlock(int batchSize, Duration timeout) {
        ensureOpen();
        validateBatchSize(batchSize);
        Objects.requireNonNull(timeout, "timeout");
        ensureGroup();
        return doRead(batchSize, timeout);
    }

    /**
     * 排空本消费者 PEL 中已投递未确认的消息（XREADGROUP id=0 语义）。
     *
     * <p>实例崩溃/停止时，已投递到该消费者 PEL 但未 ACK 的消息会永久滞留； 本方法在消费循环启动前 将这些消息重新交付处理，补齐 at-least-once 恢复路径。
     */
    @Override
    public List<Message<?>> drainPendingOnce(int maxMessages) {
        ensureOpen();
        validateBatchSize(maxMessages);
        ensureGroup();
        RStream<String, String> stream = getStream();
        String effectiveGroup = getEffectiveGroup();
        try {
            // 显式构造 (0,0) 实例而非 MIN 常量：Redisson 会把 greaterThan(MIN) 序列化为 "-"，
            // 该写法仅 XRANGE 合法，XREADGROUP 直接报 ERR Invalid stream ID；
            // XREADGROUP 的历史读取需要字面量 "0-0"（仅本消费者 PEL，不含 '>' 新消息）
            Map<StreamMessageId, Map<String, String>> result =
                    stream.readGroup(
                            effectiveGroup,
                            consumerName,
                            StreamReadGroupArgs.greaterThan(new StreamMessageId(0L, 0L))
                                    .count(maxMessages));
            if (CollectionUtils.isEmpty(result)) {
                return List.of();
            }
            LOG.info(
                    "Draining {} pending entries from own PEL: topic={}, group={}, consumer={}",
                    result.size(),
                    topic,
                    effectiveGroup,
                    consumerName);
            List<Message<?>> messages = new ArrayList<>(result.size());
            List<MessageId> poisonIds = new ArrayList<>();
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : result.entrySet()) {
                try {
                    messages.add(toMessage(entry.getKey(), entry.getValue()));
                } catch (RuntimeException conversionEx) {
                    if (handlePoisonEntry(entry.getKey(), entry.getValue(), conversionEx)) {
                        poisonIds.add(MessageId.fromStreamEntry(entry.getKey().toString()));
                    }
                }
            }
            if (!poisonIds.isEmpty()) {
                ackBatch(poisonIds);
            }
            return messages;
        } catch (RuntimeException ex) {
            LOG.warn(
                    "drainPendingOnce failed (will retry on next pull): topic={}, group={}: {}",
                    topic,
                    effectiveGroup,
                    ex.getMessage());
            return List.of();
        }
    }

    @Override
    public void ack(MessageId messageId) {
        ensureOpen();
        Objects.requireNonNull(messageId, "messageId");
        RStream<String, String> stream = getStream();
        try {
            stream.ack(getEffectiveGroup(), toStreamId(messageId));
        } catch (RuntimeException ex) {
            throw new StreamMQBrokerException(
                    "ack failed for topic " + topic + ", messageId=" + messageId, null, ex);
        }
    }

    @Override
    public void ackBatch(List<MessageId> messageIds) {
        ensureOpen();
        Objects.requireNonNull(messageIds, "messageIds");
        if (messageIds.isEmpty()) {
            return;
        }
        RStream<String, String> stream = getStream();
        StreamMessageId[] streamIds = new StreamMessageId[messageIds.size()];
        for (int i = 0; i < messageIds.size(); i++) {
            streamIds[i] = toStreamId(messageIds.get(i));
        }
        try {
            stream.ack(getEffectiveGroup(), streamIds);
        } catch (RuntimeException ex) {
            throw new StreamMQBrokerException(
                    "ackBatch failed for topic " + topic + ", size=" + messageIds.size(), null, ex);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // 广播模式：不再销毁消费者组。此前 close() 直接 removeGroup 会带来两个严重问题：
            //   1. 组的 PEL 一并丢弃 —— 优雅停机时在途消息永久丢失；
            //   2. 重启后 ensureGroup 以 id(0-0) 重建组 —— 全量历史重放。
            // 现在改为"心跳 + 僵尸组回收"模型：
            //   - 运行中的广播监听器每次拉取成功都会刷新注册表心跳（见 doRead）；
            //   - close()/崩溃后心跳停止，{@link #sweepStaleBroadcastGroups} 在心跳超过
            //     BROADCAST_GROUP_STALE_TTL_MS 后才 XGROUP DESTROY 该僵尸组，
            //     正常重启的实例因组仍存在而继续从上次消费位点恢复，既不重放也不丢 PEL。
            LOG.info(
                    "RedissonStreamListener closed: topic={}, group={}, consumer={}",
                    topic,
                    group,
                    consumerName);
        }
    }

    /**
     * 返回 Listener 是否正在运行。
     *
     * @return true 如果未关闭
     */
    public boolean isRunning() {
        return !closed.get();
    }

    // ===================== 内部方法 =====================

    private List<Message<?>> doRead(int batchSize, Duration timeout) {
        RStream<String, String> stream = getStream();
        StreamReadGroupArgs args;
        if (Objects.nonNull(timeout) && !timeout.isZero() && !timeout.isNegative()) {
            args = StreamReadGroupArgs.neverDelivered().count(batchSize).timeout(timeout);
        } else {
            args = StreamReadGroupArgs.neverDelivered().count(batchSize);
        }
        String effectiveGroup = getEffectiveGroup();
        LOG.debug(
                "doRead: streamKey={}, effectiveGroup={}, consumerName={}, batchSize={},"
                        + " timeout={}",
                stream.getName(),
                effectiveGroup,
                consumerName,
                batchSize,
                timeout);
        try {
            Map<StreamMessageId, Map<String, String>> result =
                    stream.readGroup(effectiveGroup, consumerName, args);
            LOG.debug(
                    "doRead result: streamKey={}, resultSize={}",
                    stream.getName(),
                    result != null ? result.size() : 0);
            if (CollectionUtils.isEmpty(result)) {
                heartbeatBroadcastRegistry();
                return List.of();
            }
            List<Message<?>> messages = new ArrayList<>(result.size());
            List<MessageId> poisonIds = new ArrayList<>();
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : result.entrySet()) {
                LOG.debug(
                        "doRead processing entry: streamKey={}, msgId={}",
                        stream.getName(),
                        entry.getKey());
                try {
                    Message<?> message = toMessage(entry.getKey(), entry.getValue());
                    messages.add(message);
                } catch (RuntimeException conversionEx) {
                    // 毒丸消息逐条隔离：仅该条进入 DLQ 后 ACK；转存失败则保留 PEL 等待重投
                    if (handlePoisonEntry(entry.getKey(), entry.getValue(), conversionEx)) {
                        poisonIds.add(MessageId.fromStreamEntry(entry.getKey().toString()));
                    }
                }
            }
            if (!poisonIds.isEmpty()) {
                ackBatch(poisonIds);
            }
            heartbeatBroadcastRegistry();
            return messages;
        } catch (RuntimeException ex) {
            String msg = ex.getMessage();
            boolean noGroup = Objects.nonNull(msg) && msg.contains(NOGROUP_MARKER);
            // Stream Key 在 group 创建后被删除（外部清理/运维误删/裁剪）时，RESP2 对不存在的
            // key 返回空数组，Redisson 解析为 EmptyList 并在内部强转 Map 时抛 ClassCastException，
            // 其消息不含 NOGROUP 标记——若不识别会退化为每轮拉取都失败的死循环。
            boolean missingStream = ex instanceof ClassCastException;
            if (noGroup || missingStream) {
                LOG.warn(
                        "Group/stream missing ({}), resetting groupCreated flag to trigger"
                                + " re-creation: streamKey={}, effectiveGroup={}, error={}",
                        noGroup ? "NOGROUP" : "missing-stream",
                        stream.getName(),
                        effectiveGroup,
                        msg);
                groupCreated.set(false);
            }
            if (interruptedFailure(ex)) {
                // 容器停止引发的中断（Redisson 包装为 RuntimeException）：恢复中断位并降级为
                // debug——ERROR 级堆栈在停机日志中是噪音，非真实 IO 故障
                Thread.currentThread().interrupt();
                LOG.debug(
                        "doRead interrupted, shutting down: streamKey={}, effectiveGroup={}",
                        stream.getName(),
                        effectiveGroup);
            } else {
                LOG.error(
                        "doRead failed: streamKey={}, effectiveGroup={}, error={}",
                        stream.getName(),
                        effectiveGroup,
                        ex.getMessage());
            }
            throw new StreamMQBrokerException("readGroup failed for topic " + topic, null, ex);
        }
    }

    /**
     * 判断异常是否由线程中断引起。
     *
     * <p>Redisson 将底层 {@link InterruptedException} 包装为 RuntimeException 抛出 （消息含 {@code
     * java.lang.InterruptedException}），需沿 cause 链与消息双重识别。
     */
    private static boolean interruptedFailure(Throwable ex) {
        Throwable t = ex;
        while (Objects.nonNull(t)) {
            if (t instanceof InterruptedException) {
                return true;
            }
            String msg = t.getMessage();
            if (Objects.nonNull(msg) && msg.contains("java.lang.InterruptedException")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Message<?> toMessage(StreamMessageId streamId, Map<String, String> fields) {
        // 反序列化目标类型回退链（对齐 RocketMQ，优先使用消费者声明的泛型类型）：
        //   1. targetBodyType（容器解析自 Listener 泛型 T，消费者声明的类型优先级最高）
        //   2. bodyTypeName 匹配（仅类名匹配，支持跨包/跨模块场景：发送端 com.foo.UserInfo -> 消费端 com.bar.UserInfo）
        //   3. bodyType（Stream Entry 中的完整类名字段）
        //   4. String.class（最终回退，由消费者自行反序列化）
        Class<?> bodyType = targetBodyType;
        if (Objects.isNull(bodyType)) {
            String simpleTypeName = fields.get(DefaultMessageConverter.FIELD_BODY_TYPE_NAME);
            if (StringUtils.isNotEmpty(simpleTypeName)) {
                bodyType = loadClassBySimpleName(simpleTypeName);
            }
        }
        if (Objects.isNull(bodyType)) {
            String fullTypeName = fields.get(DefaultMessageConverter.FIELD_BODY_TYPE);
            if (StringUtils.isNotEmpty(fullTypeName)) {
                bodyType = loadClassBySimpleName(fullTypeName);
                if (Objects.isNull(bodyType)) {
                    LOG.warn(
                            "Body type class not found by full name, fallback to String: {}",
                            fullTypeName);
                }
            }
        }
        if (Objects.isNull(bodyType)) {
            bodyType = String.class;
        }
        Message<?> message =
                converter
                        .fromStreamFields(fields, (Class) bodyType, topic)
                        .withMessageId(MessageId.fromStreamEntry(streamId.toString()));
        return message;
    }

    /**
     * 毒丸消息处理：反序列化失败的单条消息转存 DLQ Stream（携带原始字段与原因标识）。
     *
     * <p>失败语义：DLQ 转存成功 → 返回 true，调用方将其 ACK 移出 PEL（消息不丢，运维可排查/重放）； 转存失败 → 返回 false 且不 ACK，消息保留在 PEL
     * 等待下次投递（宁可重复隔离也不静默丢失）。
     *
     * @return true 表示已转存 DLQ（应 ACK）；false 表示转存失败（保留 PEL）
     */
    private boolean handlePoisonEntry(
            StreamMessageId streamId, Map<String, String> fields, RuntimeException cause) {
        String entryId = streamId.toString();
        LOG.error(
                "Poison message detected, routing to DLQ: topic={}, group={}, entryId={},"
                        + " cause={}",
                topic,
                group,
                entryId,
                cause.getMessage());
        try {
            Map<String, String> dlqFields = new java.util.LinkedHashMap<>(fields);
            dlqFields.put(FIELD_DLQ_REASON, DlqReason.DESERIALIZE.getCode());
            dlqFields.put("dlqEntryId", entryId);
            dlqFields.put(
                    "dlqError",
                    Objects.nonNull(cause.getMessage())
                            ? cause.getMessage()
                            : cause.getClass().getName());
            String dlqKey = StreamMQKeys.dlqStream(namespace, group);
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            dlqStream.add(StreamAddArgs.entries(dlqFields));
            return true;
        } catch (RuntimeException dlqEx) {
            LOG.error(
                    "Failed to route poison message to DLQ, keeping in PEL for redelivery:"
                            + " topic={}, group={}, entryId={}",
                    topic,
                    group,
                    entryId,
                    dlqEx);
            return false;
        }
    }

    /**
     * 以 simpleName 加载 body 目标类型（仅类名匹配，支持跨包/跨模块解析）。
     *
     * <p><b>{@code init=false} 安全性：</b>调用 {@code Class.forName(name, false, loader)} 不触发
     * 静态初始化——反序列化目标类型的解析只做加载，绝不在消费线程上执行用户静态块 （副作用/慢初始化会阻塞消费循环）。
     *
     * <p>命中结果缓存于实例级 {@code classCache}（作用域限制见字段说明）。
     *
     * @param simpleName 类名（simple name 或 full name）
     * @return 加载到的类；找不到返回 null（由调用方走回退链）
     */
    private Class<?> loadClassBySimpleName(String simpleName) {
        Class<?> cached = classCache.get(simpleName);
        if (Objects.nonNull(cached)) {
            return cached;
        }
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (Objects.isNull(classLoader)) {
                classLoader = getClass().getClassLoader();
            }
            Class<?> clazz = Class.forName(simpleName, false, classLoader);
            classCache.put(simpleName, clazz);
            return clazz;
        } catch (ClassNotFoundException e) {
            LOG.debug(
                    "Body type class not found by simple name '{}', will try full name or fallback",
                    simpleName);
            return null;
        }
    }

    // ===================== 广播组心跳与僵尸组回收 =====================

    /**
     * 广播组心跳过期 TTL（毫秒）。
     *
     * <p>实现已迁至 {@link BroadcastGroupRegistry}，本字段保留为兼容别名。
     */
    public static final long BROADCAST_GROUP_STALE_TTL_MS =
            BroadcastGroupRegistry.BROADCAST_GROUP_STALE_TTL_MS;

    /**
     * 刷新本广播监听器在注册表中的心跳（ZSet score = 当前时间）。
     *
     * <p>仅 broadcast 且非 dlq/retry 模式时写入；失败静默（最坏情况是组被回收后由 ensureGroup 重建，语义等同旧版 close-destroy
     * 路径，不会更糟）。
     *
     * <p><b>公开为暂停期保活钩子：</b>消费循环在容器暂停期间不执行拉取，若不补发心跳， 停留超过 {@link #BROADCAST_GROUP_STALE_TTL_MS}
     * 后组会被僵尸回收任务销毁，resume 时全量重放历史。 {@code ConsumeLoopTask} 在暂停休眠周期内调用本方法维持注册表活性；非广播实例调用为 no-op。
     */
    public void heartbeatBroadcastRegistry() {
        if (!broadcast || dlqMode || retryMode) {
            return;
        }
        try {
            redisson.getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace))
                    .add(System.currentTimeMillis(), registryMember());
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast heartbeat failed: {}", ex.getMessage());
        }
    }

    private String registryMember() {
        return topic + "|" + getEffectiveGroup();
    }

    /**
     * 回收僵尸广播消费者组。
     *
     * <p>实现已迁至 {@link BroadcastGroupRegistry#sweepStaleBroadcastGroups(RedissonClient, String)}，
     * 本方法保留为兼容委托。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @return 本次回收的组数量
     */
    public static int sweepStaleBroadcastGroups(RedissonClient redisson, String namespace) {
        return BroadcastGroupRegistry.sweepStaleBroadcastGroups(redisson, namespace);
    }

    /**
     * 返回当前注册表中的广播消费组数量（含活跃与尚未被回收的僵尸组）。
     *
     * <p>实现已迁至 {@link BroadcastGroupRegistry#countBroadcastGroups(RedissonClient, String)}，
     * 本方法保留为兼容委托。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     * @return 注册表中的广播组条目数
     */
    public static long countBroadcastGroups(RedissonClient redisson, String namespace) {
        return BroadcastGroupRegistry.countBroadcastGroups(redisson, namespace);
    }

    private void ensureGroup() {
        if (groupCreated.get()) {
            return;
        }
        if (groupCreated.compareAndSet(false, true)) {
            RStream<String, String> stream = getStream();
            // 广播模式下使用独立消费者组名（每个实例一个组，均接收全量消息）
            String effectiveGroup = getEffectiveGroup();
            LOG.info(
                    "Ensuring consumer group: namespace={}, topic={}, group={}, effectiveGroup={},"
                            + " dlqMode={}, retryMode={}, streamKey={}",
                    namespace,
                    topic,
                    group,
                    effectiveGroup,
                    dlqMode,
                    retryMode,
                    stream.getName());
            try {
                // makeStream：如果 Stream 不存在则创建
                // id(0-0)：从头开始消费
                stream.createGroup(
                        StreamCreateGroupArgs.name(effectiveGroup)
                                .makeStream()
                                .id(new StreamMessageId(0, 0)));
                LOG.info(
                        "Consumer group created: topic={}, group={}{}",
                        topic,
                        effectiveGroup,
                        broadcast ? " (broadcast, unique per instance)" : "");
            } catch (RuntimeException ex) {
                // BUSYGROUP 表示 group 已存在，属于正常情况
                String msg = ex.getMessage();
                if (Objects.nonNull(msg) && msg.contains(BUSYGROUP_MARKER)) {
                    LOG.debug(
                            "Consumer group already exists: topic={}, group={}",
                            topic,
                            effectiveGroup);
                } else {
                    // 其他错误重置标志位，允许下次重试；中断类失败（容器停止）仅降级 debug
                    groupCreated.set(false);
                    if (interruptedFailure(ex)) {
                        Thread.currentThread().interrupt();
                        LOG.debug(
                                "createGroup interrupted, shutting down: streamKey={},"
                                        + " effectiveGroup={}",
                                stream.getName(),
                                effectiveGroup);
                    } else {
                        LOG.error(
                                "createGroup failed: streamKey={}, effectiveGroup={}, error={}",
                                stream.getName(),
                                effectiveGroup,
                                ex.getMessage());
                    }
                    throw new StreamMQBrokerException(
                            "createGroup failed for topic " + topic + ", group " + effectiveGroup,
                            null,
                            ex);
                }
            }
        }
    }

    /**
     * 获取实际使用的消费者组名。 广播模式下，每个消费者实例使用独立组名（{@code {group}:{consumerName}}）， 确保每个实例都能接收到全量消息。
     *
     * <p>重试循环（retryMode）除外：重试消息写入共享的 {@code retry:msg:{topic}:{group}} 流，若广播实例各自建组，
     * 每条重试消息会被所有实例重复消费；因此重试循环统一使用基组名，由任一实例处理一次即可。
     *
     * @return 实际的消费者组名
     */
    private String getEffectiveGroup() {
        if (retryMode) {
            return group;
        }
        if (broadcast) {
            return group + StreamMQConstants.BROADCAST_GROUP_SEPARATOR + consumerName;
        }
        return group;
    }

    private RStream<String, String> getStream() {
        String streamKey;
        if (dlqMode) {
            streamKey = StreamMQKeys.dlqStream(namespace, group);
        } else if (retryMode) {
            streamKey = StreamMQKeys.retryStream(namespace, topic, group);
        } else {
            streamKey = StreamMQKeys.topicStream(namespace, topic);
        }
        LOG.debug(
                "getStream: streamKey={}, namespace={}, topic={}, group={}, dlqMode={},"
                        + " retryMode={}",
                streamKey,
                namespace,
                topic,
                group,
                dlqMode,
                retryMode);
        return redisson.getStream(streamKey);
    }

    private static StreamMessageId toStreamId(MessageId messageId) {
        return new StreamMessageId(messageId.getTimestamp(), messageId.getSequence());
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Listener is closed: topic=" + topic + ", group=" + group);
        }
    }

    private static void validateBatchSize(int batchSize) {
        if (batchSize <= 0 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "batchSize must be between 1 and " + MAX_BATCH_SIZE + ", got " + batchSize);
        }
    }
}
