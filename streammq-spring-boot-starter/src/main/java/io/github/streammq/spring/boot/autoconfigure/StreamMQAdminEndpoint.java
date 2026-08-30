/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.converter.MessageFields;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.util.*;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
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
    private final DefaultStreamMQListenerContainer container;
    private final String namespace;
    private final FailureRetryLimiter failureRetryLimiter;

    /** Topic 占位消息字段：占位标记（写入 Stream 以创建 Stream） */
    private static final String FIELD_PLACEHOLDER = "__placeholder";

    /** Topic 占位消息字段：出生时间戳 */
    private static final String FIELD_PLACEHOLDER_BORN_TS = MessageFields.BORN_TS;

    public StreamMQAdminEndpoint(
            RedissonClient redisson, DefaultStreamMQListenerContainer container, String namespace) {
        this(redisson, container, namespace, FailureRetryLimiter.DEFAULT_COOLDOWN_MILLIS);
    }

    /**
     * 构造管理端点。
     *
     * @param redisson Redis 客户端
     * @param container 监听容器（可为 null，表示未装配）
     * @param namespace 命名空间
     * @param failureRetryCooldownMillis 写操作失败后的重试冷却期（毫秒）；0 表示禁用限流
     */
    public StreamMQAdminEndpoint(
            RedissonClient redisson,
            DefaultStreamMQListenerContainer container,
            String namespace,
            long failureRetryCooldownMillis) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.container = container;
        this.namespace = namespace == null ? "" : namespace;
        this.failureRetryLimiter = new FailureRetryLimiter(failureRetryCooldownMillis);
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
            return io.github.streammq.adapter.redisson.listener.RedissonStreamListener
                    .countBroadcastGroups(redisson, namespace);
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
            Map<String, String> fields = entries.values().iterator().next();
            // 移除 DLQ 元数据
            fields.remove(RetryScheduler.FIELD_DLQ_REASON);
            fields.remove(MessageFields.ORIGINAL_MESSAGE_ID);
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

    /** 列出所有已知 Topic。 */
    public List<String> listTopics() {
        List<String> topics = new ArrayList<>();
        if (container == null) {
            LOG.debug("Listener container not available, returning empty topics");
            return topics;
        }
        for (var meta : container.getConsumers()) {
            if (!topics.contains(meta.topic())) {
                topics.add(meta.topic());
            }
        }
        return topics;
    }

    /** 获取运行时统计。 */
    public Map<String, Object> getStats(String group, String topic) {
        Map<String, Object> stats = new LinkedHashMap<>();
        String statsKey = StreamMQKeys.metaStats(namespace, group, topic);
        try {
            RMap<String, String> statsMap = redisson.getMap(statsKey);
            stats.putAll(statsMap.readAllMap());
        } catch (RuntimeException ex) {
            stats.put("error", ex.getMessage());
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
     * 创建 Topic。
     *
     * <p>通过向目标 Stream 写入一条占位消息来创建 Stream（Redis Stream 在首次 XADD 时自动创建）。
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
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            Map<String, String> placeholder = new HashMap<>(2);
            placeholder.put(FIELD_PLACEHOLDER, "true");
            placeholder.put(FIELD_PLACEHOLDER_BORN_TS, Long.toString(System.currentTimeMillis()));
            StreamMessageId id = stream.add(StreamAddArgs.entries(placeholder));
            result.put("success", true);
            result.put("topic", topic);
            result.put("streamKey", streamKey);
            result.put("placeholderId", id.toString());
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info(
                    "Topic created: topic={}, streamKey={}, placeholderId={}",
                    topic,
                    streamKey,
                    id);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn("Create topic failed: topic={}: {}", topic, ex.getMessage());
        }
        return result;
    }

    /**
     * 删除 Topic。
     *
     * <p>删除目标 Stream 及其所有数据，操作不可逆。
     *
     * @param topic 主题名
     * @return 操作结果
     */
    public Map<String, Object> deleteTopic(String topic) {
        Map<String, Object> result = new LinkedHashMap<>();
        String limitKey = "deleteTopic:" + topic;
        if (isFailureCooldown(result, limitKey)) {
            return result;
        }
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            boolean deleted = stream.delete();
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
     * <p>将配置写入 {@code streammq:{ns}:meta:config:{group}} Hash， 支持动态调整消费组参数（如并发度、最大重试次数等）。
     *
     * @param group 消费者组名
     * @param config 配置键值对
     * @return 操作结果
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
        String configKey = StreamMQKeys.metaConfig(namespace, group);
        try {
            RMap<String, String> configMap = redisson.getMap(configKey);
            configMap.putAll(config);
            result.put("success", true);
            result.put("group", group);
            result.put("updatedKeys", config.keySet());
            failureRetryLimiter.recordSuccess(limitKey);
            LOG.info("Group config updated: group={}, keys={}", group, config.keySet());
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            failureRetryLimiter.recordFailure(limitKey);
            LOG.warn("Update group config failed: group={}: {}", group, ex.getMessage());
        }
        return result;
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
        result.put("error", "该操作此前失败，处于冷却期（剩余 " + String.format("%.1f", remaining / 1000.0) + "s），请稍后重试");
        LOG.debug("Admin write operation rate-limited: key={}, retryAfterMs={}", limitKey, remaining);
        return true;
    }
}
