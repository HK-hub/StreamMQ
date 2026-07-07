package io.github.streammq.adapter.redisson.support;

import java.util.Objects;

/**
 * Redis Key 命名工具类，统一管理 StreamMQ 所有 Key 前缀与拼接规则。
 *
 * <p>所有 Key 命名遵循规范：{@code streammq:{namespace}:{type}:{...}}。
 * 当 {@code namespace} 为空字符串时，省略 namespace 段，避免出现连续冒号。
 *
 * <p>命名空间用途：多租户 / 多环境隔离前缀。
 *
 * <h2>Key 清单（对齐 04-detailed-design.md §6）</h2>
 * <ul>
 *   <li>业务消息：{@code streammq:{ns}:msg:{topic}}（Stream）</li>
 *   <li>顺序消息分片：{@code streammq:{ns}:msg:{topic}:shard{shardId}}（Stream）</li>
 *   <li>消费组实例列表：{@code streammq:{ns}:cg:{group}:instances}（Hash）</li>
 *   <li>消费组信号量：{@code streammq:{ns}:cg:{group}:semaphore}（String）</li>
 *   <li>消费组分片分配：{@code streammq:{ns}:cg:{group}:assignment}（Hash）</li>
 *   <li>消费组通知频道：{@code streammq:{ns}:cg:{group}:notify}（PubSub）</li>
 *   <li>重试队列：{@code streammq:{ns}:retry:{topic}:{group}}（ZSet）</li>
 *   <li>死信队列：{@code streammq:{ns}:dlq:{group}}（Stream，对齐 RocketMQ %DLQ%{group}）</li>
 *   <li>重试转移降级锁：{@code streammq:{ns}:retry:{topic}:{group}:transfer:lock}（String）</li>
 *   <li>延时级别队列：{@code streammq:{ns}:delay:{level}}（ZSet）</li>
 *   <li>延时已投递计数：{@code streammq:{ns}:delay:meta:delivered}（Hash）</li>
 *   <li>半消息暂存：{@code streammq:{ns}:half:{txGroup}}（Stream）</li>
 *   <li>事务状态：{@code streammq:{ns}:txstate:{txGroup}}（Hash）</li>
 *   <li>事务回查扫描：{@code streammq:{ns}:txcheck:{txGroup}}（ZSet）</li>
 *   <li>事务回查计数：{@code streammq:{ns}:txcheck:{txGroup}:counter}（Hash）</li>
 *   <li>顺序消费分片锁：{@code streammq:{ns}:shardlock:{topic}:{group}:{shardId}}（String）</li>
 *   <li>消费位点：{@code streammq:{ns}:meta:offset:{group}:{topic}}（String）</li>
 *   <li>消费计数：{@code streammq:{ns}:meta:counter:{group}:{topic}}（Hash）</li>
 *   <li>运行时统计：{@code streammq:{ns}:meta:stats:{group}:{topic}}（Hash）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class StreamMQKeys {

    /** Key 全局前缀 */
    public static final String PREFIX = "streammq";

    /** 分隔符 */
    public static final String SEP = ":";

    /** 顺序消息分片 Stream 前缀（拼接在 topic 之后） */
    public static final String SHARD_PREFIX = ":shard";

    // ==================== Key 类型段（type segment） ====================
    /** 业务消息类型段 */
    public static final String TYPE_MSG = "msg";
    /** 消费组类型段 */
    public static final String TYPE_CG = "cg";
    /** 重试队列类型段 */
    public static final String TYPE_RETRY = "retry";
    /** 死信队列类型段 */
    public static final String TYPE_DLQ = "dlq";
    /** 延时类型段 */
    public static final String TYPE_DELAY = "delay";
    /** 半消息暂存类型段 */
    public static final String TYPE_HALF = "half";
    /** 事务状态类型段 */
    public static final String TYPE_TXSTATE = "txstate";
    /** 事务回查类型段 */
    public static final String TYPE_TXCHECK = "txcheck";
    /** 顺序消费分片锁类型段 */
    public static final String TYPE_SHARDLOCK = "shardlock";
    /** 元数据类型段 */
    public static final String TYPE_META = "meta";

    // ==================== Key 后缀段（suffix segment） ====================
    /** 实例列表后缀 */
    public static final String SEG_INSTANCES = "instances";
    /** 信号量后缀 */
    public static final String SEG_SEMAPHORE = "semaphore";
    /** 分片分配后缀 */
    public static final String SEG_ASSIGNMENT = "assignment";
    /** 通知频道后缀 */
    public static final String SEG_NOTIFY = "notify";
    /** 重试转移后缀 */
    public static final String SEG_TRANSFER = "transfer";
    /** 锁后缀 */
    public static final String SEG_LOCK = "lock";
    /** payload 后缀 */
    public static final String SEG_PAYLOAD = "payload";
    /** 已投递计数后缀 */
    public static final String SEG_DELIVERED = "delivered";
    /** 计数后缀 */
    public static final String SEG_COUNTER = "counter";
    /** 位点后缀 */
    public static final String SEG_OFFSET = "offset";
    /** 统计后缀 */
    public static final String SEG_STATS = "stats";

    private StreamMQKeys() {
    }

    /**
     * 拼接命名空间前缀段：{@code streammq:{ns}}。
     * 当 ns 为空时返回 {@code streammq}。
     *
     * @param namespace 命名空间，可为 null 或空字符串
     * @return 前缀段
     */
    public static String prefix(String namespace) {
        if (namespace == null || namespace.isEmpty()) {
            return PREFIX;
        }
        return PREFIX + SEP + namespace;
    }

    /**
     * 业务消息 Stream Key：{@code streammq:{ns}:msg:{topic}}。
     *
     * @param namespace 命名空间
     * @param topic 主题
     * @return Stream Key
     */
    public static String topicStream(String namespace, String topic) {
        return prefix(namespace) + SEP + TYPE_MSG + SEP + requireNonEmpty(topic, "topic");
    }

    /**
     * 顺序消息分片 Stream Key：{@code streammq:{ns}:msg:{topic}:shard{shardId}}。
     *
     * @param namespace 命名空间
     * @param topic 主题
     * @param shardId 分片 ID
     * @return 分片 Stream Key
     */
    public static String shardStream(String namespace, String topic, int shardId) {
        return topicStream(namespace, topic) + SHARD_PREFIX + shardId;
    }

    /**
     * 消费组实例列表 Hash Key：{@code streammq:{ns}:cg:{group}:instances}。
     */
    public static String consumerGroupInstances(String namespace, String group) {
        return prefix(namespace) + SEP + TYPE_CG + SEP + requireNonEmpty(group, "group") + SEP + SEG_INSTANCES;
    }

    /**
     * 消费组信号量 Key：{@code streammq:{ns}:cg:{group}:semaphore}。
     */
    public static String consumerGroupSemaphore(String namespace, String group) {
        return prefix(namespace) + SEP + TYPE_CG + SEP + requireNonEmpty(group, "group") + SEP + SEG_SEMAPHORE;
    }

    /**
     * 消费组分片分配 Hash Key：{@code streammq:{ns}:cg:{group}:assignment}。
     */
    public static String consumerGroupAssignment(String namespace, String group) {
        return prefix(namespace) + SEP + TYPE_CG + SEP + requireNonEmpty(group, "group") + SEP + SEG_ASSIGNMENT;
    }

    /**
     * 消费组通知频道 Key：{@code streammq:{ns}:cg:{group}:notify}。
     */
    public static String consumerGroupNotify(String namespace, String group) {
        return prefix(namespace) + SEP + TYPE_CG + SEP + requireNonEmpty(group, "group") + SEP + SEG_NOTIFY;
    }

    /**
     * 重试队列 ZSet Key：{@code streammq:{ns}:retry:{topic}:{group}}。
     * 用于调度延迟重试，score=nextRetryAt(ms)，member=msgId。
     */
    public static String retryZSet(String namespace, String topic, String group) {
        return prefix(namespace) + SEP + TYPE_RETRY + SEP + requireNonEmpty(topic, "topic")
            + SEP + requireNonEmpty(group, "group");
    }

    /**
     * 重试消息 Stream Key：{@code streammq:{ns}:retry:msg:{topic}:{group}}（对齐 RocketMQ %RETRY%{group}%）。
     *
     * <p>并发消费失败的消息经 RetryScheduler 延迟后 XADD 到此 Stream（非原 topic Stream），
     * 消费者同时订阅原 Stream 和此 retry Stream，实现 retry 与新消息隔离。
     * 消费组与原 Stream 相同，retryTimes 字段递增标识重试次数。
     *
     * @param namespace 命名空间
     * @param topic 原始主题
     * @param group 消费者组名
     * @return retry Stream Key
     */
    public static String retryStream(String namespace, String topic, String group) {
        return prefix(namespace) + SEP + TYPE_RETRY + SEP + TYPE_MSG + SEP
            + requireNonEmpty(topic, "topic") + SEP + requireNonEmpty(group, "group");
    }

    /**
     * 死信队列 Stream Key：{@code streammq:{ns}:dlq:{group}}（对齐 RocketMQ %DLQ%{group}）。
     * 按消费者组隔离，一个组的所有 topic 的死信消息混在同一 DLQ Stream 中，
     * 消息本身携带 topic 字段，消费时可从消息字段获取原 topic。
     */
    public static String dlqStream(String namespace, String group) {
        return prefix(namespace) + SEP + TYPE_DLQ + SEP + requireNonEmpty(group, "group");
    }

    /**
     * 重试转移降级锁 Key：{@code streammq:{ns}:retry:{topic}:{group}:transfer:lock}。
     */
    public static String retryTransferLock(String namespace, String topic, String group) {
        return retryZSet(namespace, topic, group) + SEP + SEG_TRANSFER + SEP + SEG_LOCK;
    }

    /**
     * 延时级别 ZSet Key：{@code streammq:{ns}:delay:{level}}。
     *
     * @param namespace 命名空间
     * @param level 延时级别标识（如 {@code SEC_1}, {@code MINUTE_5}）
     * @return ZSet Key
     */
    public static String delayZSet(String namespace, String level) {
        return prefix(namespace) + SEP + TYPE_DELAY + SEP + requireNonEmpty(level, "level");
    }

    /**
     * 延时消息 payload Hash Key：{@code streammq:{ns}:delay:payload:{msgId}}。
     *
     * @param namespace 命名空间
     * @param msgId 消息 ID
     * @return Hash Key
     */
    public static String delayPayloadHash(String namespace, String msgId) {
        return prefix(namespace) + SEP + TYPE_DELAY + SEP + SEG_PAYLOAD + SEP + requireNonEmpty(msgId, "msgId");
    }

    /**
     * 延时已投递计数 Hash Key：{@code streammq:{ns}:delay:meta:delivered}。
     */
    public static String delayDeliveredCounter(String namespace) {
        return prefix(namespace) + SEP + TYPE_DELAY + SEP + TYPE_META + SEP + SEG_DELIVERED;
    }

    /**
     * 半消息暂存 Stream Key：{@code streammq:{ns}:half:{txGroup}}。
     */
    public static String halfStream(String namespace, String txGroup) {
        return prefix(namespace) + SEP + TYPE_HALF + SEP + requireNonEmpty(txGroup, "txGroup");
    }

    /**
     * 事务状态 Hash Key：{@code streammq:{ns}:txstate:{txGroup}}。
     */
    public static String transactionStateHash(String namespace, String txGroup) {
        return prefix(namespace) + SEP + TYPE_TXSTATE + SEP + requireNonEmpty(txGroup, "txGroup");
    }

    /**
     * 事务回查 ZSet Key：{@code streammq:{ns}:txcheck:{txGroup}}。
     */
    public static String transactionCheckZSet(String namespace, String txGroup) {
        return prefix(namespace) + SEP + TYPE_TXCHECK + SEP + requireNonEmpty(txGroup, "txGroup");
    }

    /**
     * 事务回查计数 Hash Key：{@code streammq:{ns}:txcheck:{txGroup}:counter}。
     */
    public static String transactionCheckCounter(String namespace, String txGroup) {
        return transactionCheckZSet(namespace, txGroup) + SEP + SEG_COUNTER;
    }

    /**
     * 顺序消费分片锁 Key：{@code streammq:{ns}:shardlock:{topic}:{group}:{shardId}}。
     */
    public static String shardLock(String namespace, String topic, String group, int shardId) {
        return prefix(namespace) + SEP + TYPE_SHARDLOCK + SEP + requireNonEmpty(topic, "topic")
            + SEP + requireNonEmpty(group, "group") + SEP + shardId;
    }

    /**
     * 消费位点 String Key：{@code streammq:{ns}:meta:offset:{group}:{topic}}。
     */
    public static String metaOffset(String namespace, String group, String topic) {
        return prefix(namespace) + SEP + TYPE_META + SEP + SEG_OFFSET + SEP
            + requireNonEmpty(group, "group") + SEP + requireNonEmpty(topic, "topic");
    }

    /**
     * 消费计数 Hash Key：{@code streammq:{ns}:meta:counter:{group}:{topic}}。
     */
    public static String metaCounter(String namespace, String group, String topic) {
        return prefix(namespace) + SEP + TYPE_META + SEP + SEG_COUNTER + SEP
            + requireNonEmpty(group, "group") + SEP + requireNonEmpty(topic, "topic");
    }

    /**
     * 运行时统计 Hash Key：{@code streammq:{ns}:meta:stats:{group}:{topic}}。
     */
    public static String metaStats(String namespace, String group, String topic) {
        return prefix(namespace) + SEP + TYPE_META + SEP + SEG_STATS + SEP
            + requireNonEmpty(group, "group") + SEP + requireNonEmpty(topic, "topic");
    }

    private static String requireNonEmpty(String value, String name) {
        return Objects.requireNonNull(value, name + " must not be null").isEmpty()
            ? throwEmpty(name) : value;
    }

    private static String throwEmpty(String name) {
        throw new IllegalArgumentException(name + " must not be empty");
    }
}
