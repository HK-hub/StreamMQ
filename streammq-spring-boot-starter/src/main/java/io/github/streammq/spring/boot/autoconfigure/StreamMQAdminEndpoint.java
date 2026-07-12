package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.util.CollectionUtils;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * StreamMQ 运维管理 REST 端点，通过 Spring Boot Actuator 暴露。
 *
 * <p>提供以下端点（对齐 PRD §6.6）：
 * <ul>
 *   <li>{@code GET /actuator/streammq/groups} - 列出所有 ConsumerGroup 及状态</li>
 *   <li>{@code GET /actuator/streammq/groups/{group}/pending} - 列出 pending 消息</li>
 *   <li>{@code GET /actuator/streammq/dlq/{group}} - 列出 DLQ 消息</li>
 *   <li>{@code POST /actuator/streammq/dlq/{group}/requeue} - 重投 DLQ 消息</li>
 *   <li>{@code DELETE /actuator/streammq/dlq/{group}/{msgId}} - 删除指定 DLQ 消息</li>
 *   <li>{@code GET /actuator/streammq/topics} - 列出已创建的 Topic</li>
 *   <li>{@code GET /actuator/streammq/stats/{group}/{topic}} - 查询运行时统计</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.2.0
 */
public class StreamMQAdminEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQAdminEndpoint.class);

    private final RedissonClient redisson;
    private final DefaultStreamMQListenerContainer container;
    private final String namespace;

    public StreamMQAdminEndpoint(RedissonClient redisson, DefaultStreamMQListenerContainer container,
                                  String namespace) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.container = container;
        this.namespace = namespace == null ? "" : namespace;
    }

    /**
     * 列出所有已注册的 ConsumerGroup 及其实例数、pending 消息数。
     */
    public List<Map<String, Object>> listGroups() {
        List<Map<String, Object>> result = new ArrayList<>();
        var consumers = container.getConsumers();
        for (var meta : consumers) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("topic", meta.topic());
            info.put("group", meta.consumerGroup());
            info.put("consumerClass", meta.consumerType().getName());
            info.put("bodyType", meta.bodyType().getName());
            info.put("containerRunning", container.isRunning());
            // 查询实例数
            String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, meta.consumerGroup());
            try {
                RMap<String, Long> instances = redisson.getMap(instancesKey);
                Map<String, Long> all = instances.readAllMap();
                info.put("activeInstances", all != null ? all.size() : 0);
                if (CollectionUtils.isNotEmpty(all)) {
                    List<Map<String, Object>> instList = new ArrayList<>();
                    for (var e : all.entrySet()) {
                        Map<String, Object> inst = new LinkedHashMap<>();
                        inst.put("instanceId", e.getKey());
                        inst.put("lastHeartbeat", e.getValue());
                        inst.put("ageMs", System.currentTimeMillis() - e.getValue());
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
                var pendingInfo = stream.listPending(meta.consumerGroup(), StreamMessageId.MIN, StreamMessageId.MAX, 1000);
                info.put("pendingCount", pendingInfo.size());
            } catch (RuntimeException ex) {
                info.put("pendingCount", "N/A: " + ex.getMessage());
            }
            result.add(info);
        }
        return result;
    }

    /**
     * 列出指定 ConsumerGroup 的 pending 消息。
     */
    public List<Map<String, Object>> listPending(String group, String topic, int count) {
        List<Map<String, Object>> result = new ArrayList<>();
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        RStream<String, String> stream = redisson.getStream(streamKey);
        try {
            var pendingInfo = stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX,
                Math.min(count, 1000));
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

    /**
     * 列出 DLQ 消息。
     */
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
     * 将 DLQ 消息重投到原 Topic。
     */
    public Map<String, Object> requeueDlq(String group, String msgId, String targetTopic) {
        Map<String, Object> result = new LinkedHashMap<>();
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
            fields.remove("dlqReason");
            fields.remove("originalMessageId");
            // XADD 到目标 Stream
            String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
            RStream<String, String> targetStream = redisson.getStream(targetStreamKey);
            StreamMessageId newId = targetStream.add(StreamAddArgs.entries(fields));
            // XDEL DLQ 消息
            dlqStream.remove(streamMsgId);
            result.put("success", true);
            result.put("newMessageId", newId.toString());
            result.put("targetTopic", targetTopic);
            LOG.info("DLQ message requeued: group={}, oldId={}, newId={}, targetTopic={}",
                group, msgId, newId, targetTopic);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            LOG.warn("DLQ requeue failed: group={}, msgId={}: {}", group, msgId, ex.getMessage());
        }
        return result;
    }

    /**
     * 删除指定 DLQ 消息。
     */
    public Map<String, Object> deleteDlq(String group, String msgId) {
        Map<String, Object> result = new LinkedHashMap<>();
        String dlqKey = StreamMQKeys.dlqStream(namespace, group);
        try {
            RStream<String, String> dlqStream = redisson.getStream(dlqKey);
            StreamMessageId streamMsgId = parseId(msgId);
            long deleted = dlqStream.remove(streamMsgId);
            result.put("success", deleted > 0);
            result.put("deleted", deleted);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
        }
        return result;
    }

    /**
     * 列出所有已知 Topic。
     */
    public List<String> listTopics() {
        List<String> topics = new ArrayList<>();
        for (var meta : container.getConsumers()) {
            if (!topics.contains(meta.topic())) {
                topics.add(meta.topic());
            }
        }
        return topics;
    }

    /**
     * 获取运行时统计。
     */
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
     * <p>通过 {@code RStream.ack(group, streamMessageId)} 确认消息已处理完成，
     * 将其从 PEL（Pending Entry List）中移除。
     *
     * @param group 消费者组名
     * @param topic 主题
     * @param msgId 消息 ID（格式：{@code ts-seq}）
     * @return 操作结果
     */
    public Map<String, Object> ackPending(String group, String topic, String msgId) {
        Map<String, Object> result = new LinkedHashMap<>();
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            StreamMessageId streamMsgId = parseId(msgId);
            long acked = stream.ack(group, streamMsgId);
            result.put("success", acked > 0);
            result.put("acked", acked);
            LOG.info("Pending message acked: group={}, topic={}, msgId={}", group, topic, msgId);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            LOG.warn("Ack pending failed: group={}, topic={}, msgId={}: {}", group, topic, msgId, ex.getMessage());
        }
        return result;
    }

    /**
     * 触发消费组重平衡。
     *
     * <p>通过清除 {@code streammq:{ns}:cg:{group}:instances} 中的实例注册信息，
     * 使所有实例在下次心跳时重新注册并触发分片重新分配。
     *
     * @param group 消费者组名
     * @return 操作结果
     */
    public Map<String, Object> triggerRebalance(String group) {
        Map<String, Object> result = new LinkedHashMap<>();
        String instancesKey = StreamMQKeys.consumerGroupInstances(namespace, group);
        try {
            RMap<String, Long> instances = redisson.getMap(instancesKey);
            int cleared = instances.size();
            instances.delete();
            result.put("success", true);
            result.put("clearedInstances", cleared);
            LOG.info("Rebalance triggered: group={}, clearedInstances={}", group, cleared);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
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
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            Map<String, String> placeholder = new HashMap<>(2);
            placeholder.put("__placeholder", "true");
            placeholder.put("bornTs", Long.toString(System.currentTimeMillis()));
            StreamMessageId id = stream.add(StreamAddArgs.entries(placeholder));
            result.put("success", true);
            result.put("topic", topic);
            result.put("streamKey", streamKey);
            result.put("placeholderId", id.toString());
            LOG.info("Topic created: topic={}, streamKey={}, placeholderId={}", topic, streamKey, id);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
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
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        try {
            RStream<String, String> stream = redisson.getStream(streamKey);
            boolean deleted = stream.delete();
            result.put("success", true);
            result.put("topic", topic);
            result.put("deleted", deleted);
            LOG.info("Topic deleted: topic={}, deleted={}", topic, deleted);
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            LOG.warn("Delete topic failed: topic={}: {}", topic, ex.getMessage());
        }
        return result;
    }

    /**
     * 更新消费组配置。
     *
     * <p>将配置写入 {@code streammq:{ns}:meta:config:{group}} Hash，
     * 支持动态调整消费组参数（如并发度、最大重试次数等）。
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
        String configKey = StreamMQKeys.metaConfig(namespace, group);
        try {
            RMap<String, String> configMap = redisson.getMap(configKey);
            configMap.putAll(config);
            result.put("success", true);
            result.put("group", group);
            result.put("updatedKeys", config.keySet());
            LOG.info("Group config updated: group={}, keys={}", group, config.keySet());
        } catch (RuntimeException ex) {
            result.put("success", false);
            result.put("error", ex.getMessage());
            LOG.warn("Update group config failed: group={}: {}", group, ex.getMessage());
        }
        return result;
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
}
