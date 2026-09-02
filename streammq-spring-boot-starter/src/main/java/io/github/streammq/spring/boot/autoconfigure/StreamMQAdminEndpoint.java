/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.converter.MessageFields;
import io.github.streammq.adapter.redisson.listener.RedissonBroadcastGroupRegistry;
import io.github.streammq.adapter.redisson.metrics.RuntimeStatsRegistry;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.util.*;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamMQ 运维管理 REST 端点，通过 Spring Boot Actuator 暴露。
 *
 * <p>提供以下端点（对齐 PRD §6.6）：
 *
 * <ul>
 *   <li>{@code GET /actuator/streammq/groups} - 列出所有 ConsumerGroup 及状态
 *   <li>{@code GET /actuator/streammq/groups/{group}/pending} - 列出 pending 消息
 *   <li>{@code GET /actuator/streammq/dlq/{group}} - 列出 DLQ 消息
 *   <li>{@code POST /actuator/streammq/dlq/{group}/requeue} - 重投 DLQ 消息
 *   <li>{@code DELETE /actuator/streammq/dlq/{group}/{msgId}} - 删除指定 DLQ 消息
 *   <li>{@code GET /actuator/streammq/topics} - 列出已创建的 Topic
 *   <li>{@code GET /actuator/streammq/stats/{group}/{topic}} - 查询运行时统计
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQAdminEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQAdminEndpoint.class);

    private final RedissonClient redisson;
    private final StreamMQListenerContainer container;
    private final String namespace;
    private final FailureRetryLimiter failureRetryLimiter;

    /**
     * 广播组注册表（SPI 接口注入，依赖倒置）。
     *
     * <p>用户可注册自定义 {@link BroadcastGroupRegistry} Bean 覆盖广播组统计/回收策略。
     */
    private final BroadcastGroupRegistry broadcastGroupRegistry;

    // 注意：P1-1 之前这里曾有 FIELD_PLACEHOLDER / FIELD_PLACEHOLDER_BORN_TS 两个常量，
    // 用于向业务 Stream 写入占位消息以"创建" Topic。该做法会向消费者投递 body==null 的
    // 真实消息，已废弃；Topic 元数据现登记在 StreamMQKeys.topicRegistry(namespace) Set 中。

    public StreamMQAdminEndpoint(
            RedissonClient redisson, StreamMQListenerContainer container, String namespace) {
        this(
                redisson,
                container,
                namespace,
                FailureRetryLimiter.DEFAULT_COOLDOWN_MILLIS,
                new RedissonBroadcastGroupRegistry(redisson, namespace));
    }

    /**
     * 构造管理端点。
     *
     * @param redisson Redis 客户端
     * @param container 监听容器（可为 null，表示未装配）
     * @param namespace 命名空间
     * @param failureRetryCooldownMillis 写操作失败后的重试冷却期（毫秒）；0 表示禁用限流
     * @param broadcastGroupRegistry 广播组注册表（依赖倒置：传 null 回落默认 Redisson 实现）
     */
    public StreamMQAdminEndpoint(
            RedissonClient redisson,
            StreamMQListenerContainer container,
            String namespace,
            long failureRetryCooldownMillis,
            BroadcastGroupRegistry broadcastGroupRegistry) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.container = container;
        this.namespace = namespace == null ? "" : namespace;
        this.failureRetryLimiter = new FailureRetryLimiter(failureRetryCooldownMillis);
        this.broadcastGroupRegistry =
                Objects.nonNull(broadcastGroupRegistry)
                        ? broadcastGroupRegistry
                        : new RedissonBroadcastGroupRegistry(redisson, this.namespace);
    }

    /** pending 列表单次最大拉取条数，可通过 {@link #setMaxPendingQuerySize(int)} 覆盖 */
    private volatile int maxPendingQuerySize = StreamMQSpringConstants.MAX_PENDING_QUERY_SIZE;

    /**
     * 设置 pending 列表单次最大拉取条数。
     *
     * @param size 最大拉取条数，必须 &gt; 0
     */
    public void setMaxPendingQuerySize(int size) {
        if (size > 0) {
            this.maxPendingQuerySize = size;
        }
    }

    /**
     * 返回当前广播消费组条目数（含活跃组与尚未被回收的僵尸组）。
     *
     * <p>广播模式下每个容器实例占用一个独立的 Redis 消费者组，组名随容器实例标识变化，因此该数字 约等于心跳超时窗口内「实例数 ×
     * 重启次数」的累积量。持续增长通常意味着实例处于崩溃循环， 或心跳超时配置过长——两者都会持续占用 Redis 内存。
     *
     * @return 广播消费组条目数；查询失败时返回 -1（不阻塞总览）
     */
    public long countBroadcastGroups() {
        try {
            return broadcastGroupRegistry.countBroadcastGroups();
        } catch (RuntimeException ex) {
            LOG.debug("Failed to count broadcast groups: {}", ex.getMessage());
            return -1L;
        }
    }

    /** 列出所有已注册的 ConsumerGroup 及其实例数、pending 消息数。 */
    public List<Map<String, Object>> listGroups() {
        List<Map<String, Object>> result = new ArrayList<>();
        if (container == null) {
            LOG.debug("Listener container not available, returning empty consumer groups");
            return result;
        }
        var consumers = container.getConsumers();
        for (var meta : consumers) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("topic", meta.topic());
            info.put("group", meta.consumerGroup());
            info.put("consumerClass", meta.consumerType().getName());
            info.put("bodyType", meta.bodyType().getName());
            info.put("containerRunning", container.isRunning());
            // 查询实例数（instances Hash 使用 StringCodec 存储字符串时间戳，跨 codec 兼容）
            String instancesKey =
                    StreamMQKeys.consumerGroupInstances(namespace, meta.consumerGroup());
            try {
                RMap<String, String> instances =
                        redisson.getMap(instancesKey, StringCodec.INSTANCE);
                Map<String, String> all = instances.readAllMap();
                info.put("activeInstances", all != null ? all.size() : 0);
                if (CollectionUtils.isNotEmpty(all)) {
                    List<Map<String, Object>> instList = new ArrayList<>();
                    for (var e : all.entrySet()) {
                        Map<String, Object> inst = new LinkedHashMap<>();
                        inst.put("instanceId", e.getKey());
                        long lastHeartbeat = parseTimestamp(e.getValue());
                        inst.put("lastHeartbeat", lastHeartbeat);
                        inst.put("ageMs", System.currentTimeMillis() - lastHeartbeat);
                        instList.add(inst);
                    }
                    info.put("instances", instList);
                }
            } catch (RuntimeException ex) {
                info.put("instancesError", ex.getMessage());
            }
            // 查询 PEL 大小
            try {
                String streamKey = StreamMQKeys.topicStream(namespace, meta.topic());
                RStream<String, String> stream = redisson.getStream(streamKey);
                var pendingInfo =
                        stream.listPending(
                                meta.consumerGroup(),
                                StreamMessageId.MIN,
                                StreamMessageId.MAX,
                                maxPendingQuerySize);
                info.put("pendingCount", pendingInfo.size());
            } catch (RuntimeException ex) {
                info.put("pendingCount", "N/A: " + ex.getMessage());
            }
            result.add(info);
        }
        return result;
    }

    /** 列出指定 ConsumerGroup 的 pending 消息。 */
    public List<Map<String, Object>> listPending(String group, String topic, int count) {
        List<Map<String, Object>> result = new ArrayList<>();
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        RStream<String, String> stream = redisson.getStream(streamKey);
        try {
            var pendingInfo =
                    stream.listPending(
                            group,
                            StreamMessageId.MIN,
                            StreamMessageId.MAX,
                            Math.min(count, maxPendingQuerySize));
            for (var entry : pendingInfo) {
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("messageId", entry.getId().toString());
                info.put("consumerName", entry.getConsumerName());
                info.put("idleTimeMs", entry.getIdleTime());
                info.put("deliveryCount", entry.getLastTimeDelivered());
                result.add(info);
            }
        } catch (RuntimeException ex) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", ex.getMessage());
            result.add(error);
        }
        return result;
    }

    /** 列出 DLQ 消息。 */
    public List<Map<String, Object>> listDlq(String group, int count) {
        List<Map<String, Object>> result = new ArrayList<>();
        String dlqKey = StreamMQKeys.dlqStream(namespace, group);
        RStream<String, String> dlqStream = redisson.getStream(dlqKey);
        try {
            var entries = dlqStream.range(count, StreamMessageId.MIN, StreamMessageId.MAX);
            if (entries != null) {
                for (var entry : entries.entrySet()) {
                    Map<String, Object> info = new LinkedHashMap<>();
                    info.put("messageId", entry.getKey().toString());
                    info.put("fields", entry.getValue());
                    result.add(info);
                }
            }
        } catch (RuntimeException ex) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", ex.getMessage());
            result.add(error);
        }
        return result;
    }

    /**
     * 将 DLQ 消息原子重投到原 Topic。
     *
     * <p>通过单个 Lua 脚本（{@code XADD} 目标 Stream + {@code XDEL} 源 DLQ）保证重投的原子性： 此前的「先 XADD 再
     * XDEL」两步操作在中间失败时会导致消息重复（DLQ 未删除但目标已写入）。 脚本约定：KEYS = [dlqKey, targetKey]，ARGV = [field1,
     * value1, ..., fieldN, valueN, msgId] （字段 K/V 对在前，待删除的 DLQ 消息 ID 固定在末尾）。
     */
    public Map<String, Object> requeueDlq(String group, String msgId, String targetTopic) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "requeueDlq:" + group + ":" + msgId;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        String dlqKey = StreamMQKeys.dlqStream(namespace, group);
        try {
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            StreamMessageId streamMsgId = parseId(msgId);
            // 读取 DLQ 消息
            var entries = dlqStream.range(1, streamMsgId, streamMsgId);
            if (CollectionUtils.isEmpty(entries)) {
                result.put("success", false);
                result.put("error", "DLQ message not found: " + msgId);
                return result;
            }
            // 复制为可变 Map：不假设 Redisson range() 返回的 Map 实现可变（不同 codec/版本可能返回
            // 不可变 Map，直接 remove 会抛 UnsupportedOperationException）
            Map<String, String> fields = new LinkedHashMap<>(entries.values().iterator().next());
            // 移除 DLQ 元数据
            fields.remove(RetryScheduler.FIELD_DLQ_REASON);
            fields.remove(MessageFields.ORIGINAL_MESSAGE_ID);
            // Lua unpack() 受 LUAI_MAXCSTACK（默认 8000）限制：XADD 的 K/V 参数 = 2*字段数 + 1。
            // 常规消息字段数远低于 Redis stream_max_entry_fields（默认 100），但若运维调大该上限
            // 且消息携带大量用户属性，unpack 会抛 "too many results to unpack"——此处显式设防，
            // 给出可读错误而非被降级为失败冷却。
            if (fields.size() > MAX_REQUEUE_FIELD_COUNT) {
                result.put("success", false);
                result.put(
                        "error",
                        "DLQ message has "
                                + fields.size()
                                + " fields, exceeding the requeue limit of "
                                + MAX_REQUEUE_FIELD_COUNT);
                LOG.warn(
                        "DLQ requeue rejected: message too large (fields={} > {}): group={},"
                                + " msgId={}",
                        fields.size(),
                        MAX_REQUEUE_FIELD_COUNT,
                        group,
                        msgId);
                return result;
            }
            // 组装 ARGV：K/V 对在前，msgId 固定在末尾
            String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
            List<Object> argv = new ArrayList<>(fields.size() * 2 + 1);
            for (Map.Entry<String, String> entry : fields.entrySet()) {
                argv.add(entry.getKey());
                argv.add(entry.getValue());
            }
            argv.add(msgId.trim());
            // 原子脚本：XADD 成功才 XDEL，避免两步操作中间失败导致消息重复或丢失
            Long requeued =
                    redisson.getScript(StringCodec.INSTANCE)
                            .eval(
                                    RScript.Mode.READ_WRITE,
                                    REQUEUE_LUA,
                                    RScript.ReturnType.INTEGER,
                                    List.of(dlqKey, targetStreamKey),
                                    argv.toArray());
            if (requeued == null || requeued != 1L) {
                result.put("success", false);
                result.put("error", "requeue script did not execute: return " + requeued);
                LOG.warn(
                        "DLQ requeue script returned {}: group={}, msgId={}, targetTopic={}",
                        requeued,
                        group,
                        msgId,
                        targetTopic);
                return result;
            }
            result.put("success", true);
            result.put("targetTopic", targetTopic);
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info(
                    "DLQ message requeued atomically: group={}, oldId={}, targetTopic={}",
                    group,
                    msgId,
                    targetTopic);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn("DLQ requeue failed: group={}, msgId={}: {}", group, msgId, ex.getMessage());
        }
        return result;
    }

    /**
     * DLQ 重投单条消息允许的最大字段数。
     *
     * <p>Lua {@code unpack} 受 {@code LUAI_MAXCSTACK}（默认 8000）限制，XADD 参数为 {@code 2*字段数 + 1}； 预留安全余量取
     * 1000（远超 Redis 默认 {@code stream_max_entry_fields=100}）。仅当运维调大该 Redis 上限
     * 且消息携带大量用户属性时才会触达，此设防用于给出可读错误而非 Lua 内部异常。
     */
    static final int MAX_REQUEUE_FIELD_COUNT = 1000;

    /**
     * DLQ 重投 Lua 脚本：XADD 目标流成功后 XDEL 源 DLQ 条目，返回 1；参数形态异常时返回 0。
     *
     * <p>ARGV 约定：偶数位为 field，紧随其后为 value，最后一个元素为待删除的消息 ID（{@code ts-seq}）。
     */
    private static final String REQUEUE_LUA =
            "local n = #ARGV\n"
                    + "if n < 3 or n % 2 == 0 then return 0 end\n"
                    + "local msgId = table.remove(ARGV)\n"
                    + "redis.call('XADD', KEYS[2], '*', unpack(ARGV))\n"
                    + "redis.call('XDEL', KEYS[1], msgId)\n"
                    + "return 1\n";

    /** 删除指定 DLQ 消息。 */
    public Map<String, Object> deleteDlq(String group, String msgId) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "deleteDlq:" + group + ":" + msgId;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        String dlqKey = StreamMQKeys.dlqStream(namespace, group);
        try {
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            StreamMessageId streamMsgId = parseId(msgId);
            long deleted = dlqStream.remove(streamMsgId);
            result.put("success", deleted > 0);
            result.put("deleted", deleted);
            failureRetryLimiter.recordSuccess(limitKey);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
        }
        return result;
    }

    /**
     * 列出所有已知 Topic。
     *
     * <p>合并两个来源：
     *
     * <ul>
     *   <li>本实例已注册消费者所声明的 Topic
     *   <li>由管理端点显式创建的 Topic（{@code streammq:{ns}:meta:topics} Set）
     * </ul>
     *
     * <p>发布前修复 P1-1 的配套项：Topic 创建不再写入业务 Stream，因此必须能从注册表读回， 否则会出现"创建成功但 listTopics 查不到"的不一致。
     */
    public List<String> listTopics() {
        List<String> topics = new ArrayList<>();
        if (container != null) {
            for (var meta : container.getConsumers()) {
                if (!topics.contains(meta.topic())) {
                    topics.add(meta.topic());
                }
            }
        }
        try {
            RSet<String> registry =
                    redisson.getSet(StreamMQKeys.topicRegistry(namespace), StringCodec.INSTANCE);
            for (String registered : registry.readAll()) {
                if (!topics.contains(registered)) {
                    topics.add(registered);
                }
            }
        } catch (RuntimeException ex) {
            LOG.debug("Failed to read topic registry: {}", ex.getMessage());
        }
        Collections.sort(topics);
        return topics;
    }

    /**
     * 获取运行时统计。
     *
     * <p><b>发布前修复 P1-3：</b>此前只读 Redis 上的 {@code meta:stats} Hash，而该 key 全项目无写入方， 端点永远返回 {@code
     * {}}。现数据来源为容器内的 {@link RuntimeStatsRegistry}（进程内累积， 由消费管线与重试/DLQ 处理器在上报），并补充 Redis
     * 侧的持久化聚合值与实时积压指标。
     *
     * @param group 消费者组
     * @param topic 主题
     * @return 统计快照
     */
    public Map<String, Object> getStats(String group, String topic) {
        Map<String, Object> stats = new LinkedHashMap<>();
        // 1) 进程内真实计数（消费成功/失败、重试、死信、平均耗时）
        if (container
                instanceof
                io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer
                                dlc) {
            stats.putAll(dlc.runtimeStats().snapshot(group, topic));
        } else {
            stats.put("noData", true);
            stats.put("hint", "Listener container not available on this instance.");
        }
        // 2) Redis 侧持久化聚合（保留向后兼容：用户可自行写入该 Hash 作为跨实例聚合值）
        try {
            RMap<String, String> persisted =
                    redisson.getMap(StreamMQKeys.metaStats(namespace, group, topic));
            Map<String, String> persistedValues = persisted.readAllMap();
            if (!persistedValues.isEmpty()) {
                stats.put("persisted", persistedValues);
            }
        } catch (RuntimeException ex) {
            stats.put("persistedError", ex.getMessage());
        }
        // 3) 实时积压：pending 条数（运维最关心的滞后指标）
        try {
            RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            stats.put(
                    "pendingCount",
                    stream.listPending(
                                    group,
                                    StreamMessageId.MIN,
                                    StreamMessageId.MAX,
                                    maxPendingQuerySize)
                            .size());
        } catch (RuntimeException ex) {
            stats.put("pendingCount", "N/A: " + ex.getMessage());
        }
        return stats;
    }

    /**
     * 手动 ACK 一条 pending 消息。
     *
     * <p>通过 {@code RStream.ack(group, streamMessageId)} 确认消息已处理完成， 将其从 PEL（Pending Entry List）中移除。
     *
     * @param group 消费者组名
     * @param topic 主题
     * @param msgId 消息 ID（格式：{@code ts-seq}）
     * @return 操作结果
     */
    public Map<String, Object> ackPending(String group, String topic, String msgId) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "ackPending:" + group + ":" + topic + ":" + msgId;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            StreamMessageId streamMsgId = parseId(msgId);
            long acked = stream.ack(group, streamMsgId);
            result.put("success", acked > 0);
            result.put("acked", acked);
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info("Pending message acked: group={}, topic={}, msgId={}", group, topic, msgId);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn(
                    "Ack pending failed: group={}, topic={}, msgId={}: {}",
                    group,
                    topic,
                    msgId,
                    ex.getMessage());
        }
        return result;
    }

    /**
     * 触发消费组重平衡。
     *
     * <p>通过清除 {@code streammq:{ns}:cg:{group}:instances} 中的实例注册信息， 使所有实例在下次心跳时重新注册并触发分片重新分配； 同时调用容器
     * {@code rebalanceGroup} 对 ORDERLY 消费者执行一次分片分配并广播通知。
     *
     * @param group 消费者组名
     * @return 操作结果
     */
    public Map<String, Object> triggerRebalance(String group) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "triggerRebalance:" + group;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, group);
        try {
            RMap<String, Long> instances = redisson.getMap(instancesKey);
            int cleared = instances.size();
            instances.delete();
            result.put("clearedInstances", cleared);
            boolean rebalanced = false;
            if (container != null) {
                rebalanced = container.rebalanceGroup(group);
            }
            result.put("success", true);
            result.put("rebalanceExecuted", rebalanced);
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info(
                    "Rebalance triggered: group={}, clearedInstances={}, rebalanceExecuted={}",
                    group,
                    cleared,
                    rebalanced);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn("Trigger rebalance failed: group={}: {}", group, ex.getMessage());
        }
        return result;
    }

    /**
     * 创建 Topic（登记到 Topic 注册表）。
     *
     * <p><b>发布前修复 P1-1：</b>旧实现向业务 Stream 写入一条 {@code __placeholder} 占位消息， 依赖"Stream 首次 XADD
     * 时自动创建"这一副作用。该占位消息会被所有消费者当作真实消息投递 （{@code body == null}），在业务 handler 中直接 NPE ——
     * 一次运维操作污染整条消费链路。
     *
     * <p>新实现把 Topic 元数据登记在独立的 {@code streammq:{ns}:meta:topics} Set 中：
     *
     * <ul>
     *   <li>创建 Topic 不再产生任何会被消费的条目；
     *   <li>业务 Stream 仍由首次真实发送自然创建（Redis 语义不变）；
     *   <li>{@link #listTopics()} 因此能同时返回"显式创建"与"已有消费者"两类 Topic。
     * </ul>
     *
     * @param topic 主题名
     * @return 操作结果
     */
    public Map<String, Object> createTopic(String topic) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "createTopic:" + topic;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        String registryKey = StreamMQKeys.topicRegistry(namespace);
        try {
            RSet<String> registry = redisson.getSet(registryKey, StringCodec.INSTANCE);
            boolean added = registry.add(topic);
            result.put("success", true);
            result.put("topic", topic);
            result.put("registryKey", registryKey);
            result.put("created", added);
            result.put("streamKey", StreamMQKeys.topicStream(namespace, topic));
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info("Topic registered: topic={}, created={}", topic, added);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn("Create topic failed: topic={}: {}", topic, ex.getMessage());
        }
        return result;
    }

    /**
     * 删除 Topic（不可逆，需显式确认）。
     *
     * <p>删除目标 Stream 及其所有数据。同时从 Topic 注册表中移除登记项。
     *
     * <p><b>发布前修复 P2：</b>这是一个不可逆的破坏性操作，暴露在 HTTP DELETE 上。要求调用方 通过 {@code confirm} 参数回传 topic
     * 名以确认意图，避免路径参数被误构造/误触发时直接 销毁整条业务流。
     *
     * @param topic 主题名
     * @param confirm 确认串，必须等于 topic
     * @return 操作结果
     */
    public Map<String, Object> deleteTopic(String topic, String confirm) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "deleteTopic:" + topic;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        if (!Objects.equals(topic, confirm)) {
            result.put("success", false);
            result.put(
                    "error",
                    "Refusing to delete topic '"
                            + topic
                            + "': this operation is irreversible. Pass confirm="
                            + topic
                            + " to acknowledge.");
            LOG.warn("Delete topic rejected, missing/incorrect confirm: topic={}", topic);
            return result;
        }
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            boolean deleted = stream.delete();
            redisson.getSet(StreamMQKeys.topicRegistry(namespace), StringCodec.INSTANCE)
                    .remove(topic);
            result.put("success", true);
            result.put("topic", topic);
            result.put("deleted", deleted);
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info("Topic deleted: topic={}, deleted={}", topic, deleted);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn("Delete topic failed: topic={}: {}", topic, ex.getMessage());
        }
        return result;
    }

    /**
     * 更新消费组配置。
     *
     * <p><b>发布前修复 P1-4：</b>旧实现把配置写入 {@code streammq:{ns}:meta:config:{group}} Hash 后就 结束了 ——
     * 全项目<b>没有任何代码读取该 Hash</b>。用户以为参数已生效，实际什么都没发生 （静默失败比没有这个功能更危险）。
     *
     * <p>新实现逐 key 执行真实的运行期变更，并把结果（含被拒绝的 key 及原因）完整回传：
     *
     * <table>
     *   <tr><th>key</th><th>取值</th><th>效果</th></tr>
     *   <tr><td>{@code paused}</td><td>true/false</td><td>暂停 / 恢复消费循环</td></tr>
     *   <tr><td>{@code inflightCapacity}</td><td>整数 &ge; 0</td><td>背压队列容量（0=禁用）</td></tr>
     *   <tr><td>{@code pausedSleepMillis}</td><td>正整数</td><td>暂停状态下的休眠间隔</td></tr>
     *   <tr><td>{@code brokerErrorBackoffMillis}</td><td>正整数</td><td>Broker 异常后的退避间隔</td></tr>
     *   <tr><td>{@code timeoutCancelGraceMillis}</td><td>正整数</td><td>消费超时取消后的宽限期</td></tr>
     * </table>
     *
     * <p><b>不支持的 key 会被显式拒绝并在响应中列出</b>，而不是静默写入一个无人读取的 Hash。 无法在运行期安全变更的参数（如 {@code
     * consumeThreadMin}、{@code maxReconsumeTimes}） 需要重启生效 —— 响应以 {@code rejected} 明确告知，不假装生效。
     *
     * @param group 消费者组名
     * @param config 配置键值对
     * @return 操作结果（含 {@code applied} / {@code rejected} 明细）
     */
    public Map<String, Object> updateGroupConfig(String group, Map<String, String> config) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(config)) {
            result.put("success", false);
            result.put("error", "config must not be null or empty");
            return result;
        }
        String limitKey = "updateGroupConfig:" + group;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }

        Map<String, Object> applied = new LinkedHashMap<>();
        Map<String, String> rejected = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String raw = entry.getValue() == null ? "" : entry.getValue().trim();
            if (container == null) {
                rejected.put(key, "listener container not available on this instance");
                continue;
            }
            switch (key) {
                case "paused" -> {
                    if ("true".equalsIgnoreCase(raw)) {
                        container.pause();
                        applied.put(key, Boolean.TRUE);
                    } else if ("false".equalsIgnoreCase(raw)) {
                        container.resume();
                        applied.put(key, Boolean.FALSE);
                    } else {
                        rejected.put(key, "expected 'true' or 'false', got '" + raw + "'");
                    }
                }
                case "inflightCapacity" ->
                        applyLong(
                                key,
                                raw,
                                0,
                                Integer.MAX_VALUE,
                                v -> container.setInflightCapacity((int) v),
                                applied,
                                rejected);
                case "pausedSleepMillis" ->
                        applyLong(
                                key,
                                raw,
                                1,
                                Long.MAX_VALUE,
                                container::setPausedSleepMillis,
                                applied,
                                rejected);
                case "brokerErrorBackoffMillis" ->
                        applyLong(
                                key,
                                raw,
                                1,
                                Long.MAX_VALUE,
                                container::setBrokerErrorBackoffMillis,
                                applied,
                                rejected);
                case "timeoutCancelGraceMillis" ->
                        applyLong(
                                key,
                                raw,
                                1,
                                Long.MAX_VALUE,
                                container::setTimeoutCancelGraceMillis,
                                applied,
                                rejected);
                default ->
                        rejected.put(
                                key,
                                "not runtime-mutable. Supported keys: "
                                        + String.join(", ", MUTABLE_GROUP_CONFIG_KEYS)
                                        + ". Other parameters (e.g. consumeThreadMin,"
                                        + " maxReconsumeTimes) require a restart.");
            }
        }

        result.put("applied", applied);
        result.put("rejected", rejected);
        result.put("success", rejected.isEmpty());
        result.put("group", group);
        if (!rejected.isEmpty()) {
            LOG.warn(
                    "Group config partially rejected: group={}, rejected={}",
                    group,
                    rejected.keySet());
        }
        if (!applied.isEmpty()) {
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info("Group config applied: group={}, keys={}", group, applied.keySet());
        } else {
            failureRetryLimiter.recordFailure(limitKey);
        }
        return result;
    }

    /**
     * 运行期可安全变更的消费组配置键集合。
     *
     * <p>属于管理端点的对外契约：新增/移除键需记入 CHANGELOG。
     */
    public static final java.util.Set<String> MUTABLE_GROUP_CONFIG_KEYS =
            java.util.Set.of(
                    "paused",
                    "inflightCapacity",
                    "pausedSleepMillis",
                    "brokerErrorBackoffMillis",
                    "timeoutCancelGraceMillis");

    /** 解析并应用一个数值型配置项；解析失败或越界时记入 {@code rejected}。 */
    private void applyLong(
            String key,
            String raw,
            long min,
            long max,
            java.util.function.LongConsumer setter,
            Map<String, Object> applied,
            Map<String, String> rejected) {
        long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            rejected.put(key, "not a valid integer: '" + raw + "'");
            return;
        }
        if (value < min || value > max) {
            rejected.put(key, "out of range [" + min + ", " + max + "]: " + value);
            return;
        }
        setter.accept(value);
        applied.put(key, value);
    }

    /** 解析心跳时间戳字符串（非法值视为 0）。 */
    private static long parseTimestamp(String value) {
        if (StringUtils.isEmpty(value)) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            return 0L;
        }
    }

    private static StreamMessageId parseId(String msgId) {
        int dashIdx = msgId.indexOf('-');
        if (dashIdx < 0) {
            throw new IllegalArgumentException("Invalid message id: " + msgId);
        }
        long ts = Long.parseLong(msgId.substring(0, dashIdx));
        long seq = Long.parseLong(msgId.substring(dashIdx + 1));
        return new StreamMessageId(ts, seq);
    }

    /**
     * 写操作统一守卫：目标处于失败冷却期时写入限流响应并返回 true（调用方应直接返回该 result）。
     *
     * <p>冷却期内不触碰 Redis，避免对失效目标反复重试放大故障负载。
     *
     * @param result 待填充的操作结果
     * @param limitKey 限流标识（操作名 + 目标）
     * @return true 表示已写入限流响应，调用方应终止本次操作
     */
    private boolean isFailureCooldown(Map<String, Object> result, String limitKey) {
        long remaining = failureRetryLimiter.remainingCooldownMillis(limitKey);
        if (remaining <= 0) {
            return false;
        }
        result.put("success", false);
        result.put("rateLimited", true);
        result.put("retryAfterMs", remaining);
        result.put(
                "error",
                "该操作此前失败，处于冷却期（剩余 " + String.format("%.1f", remaining / 1000.0) + "s），请稍后重试");
        LOG.debug(
                "Admin write operation rate-limited: key={}, retryAfterMs={}", limitKey, remaining);
        return true;
    }
}
