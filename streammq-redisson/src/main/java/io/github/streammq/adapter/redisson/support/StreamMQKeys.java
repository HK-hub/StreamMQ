/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import lombok.experimental.UtilityClass;

/**
 * Redis Key 命名工具类，统一管理 StreamMQ 所有 Key 前缀与拼接规则。
 *
 * <p>所有 Key 命名遵循规范：{@code streammq:{namespace}:{type}:{...}}。 当 {@code namespace} 为空字符串时，省略
 * namespace 段，避免出现连续冒号。
 *
 * <p>命名空间用途：多租户 / 多环境隔离前缀。
 *
 * <h2>Key 清单（对齐 04-detailed-design.md §6）</h2>
 *
 * <ul>
 *   <li>业务消息：{@code streammq:{ns}:msg:{topic}}（Stream）
 *   <li>顺序消息分片：{@code streammq:{ns}:msg:{topic}:shard{shardId}}（Stream）
 *   <li>消费组实例列表：{@code streammq:{ns}:cg:{group}:instances}（Hash）
 *   <li>消费组信号量：{@code streammq:{ns}:cg:{group}:semaphore}（String）
 *   <li>消费组分片分配：{@code streammq:{ns}:cg:{group}:assignment}（Hash）
 *   <li>消费组通知频道：{@code streammq:{ns}:cg:{group}:notify}（PubSub）
 *   <li>重试队列：{@code streammq:{ns}:retry:{topic}:{group}}（ZSet）
 *   <li>死信队列：{@code streammq:{ns}:dlq:{group}}（Stream，对齐 RocketMQ %DLQ%{group}）
 *   <li>重试转移降级锁：{@code streammq:{ns}:retry:{topic}:{group}:transfer:lock}（String）
 *   <li>延时级别队列：{@code streammq:{ns}:delay:{level}}（ZSet）
 *   <li>延时已投递计数：{@code streammq:{ns}:delay:meta:delivered}（Hash）
 *   <li>半消息暂存：{@code streammq:{ns}:half:{txGroup}}（Stream）
 *   <li>事务状态：{@code streammq:{ns}:txstate:{txGroup}}（Hash）
 *   <li>事务回查扫描：{@code streammq:{ns}:txcheck:{txGroup}}（ZSet）
 *   <li>事务回查计数：{@code streammq:{ns}:txcheck:{txGroup}:counter}（Hash）
 *   <li>顺序消费分片锁：{@code streammq:{ns}:shardlock:{topic}:{group}:{shardId}}（String）
 *   <li>消费位点：{@code streammq:{ns}:meta:offset:{group}:{topic}}（String）
 *   <li>消费计数：{@code streammq:{ns}:meta:counter:{group}:{topic}}（Hash）
 *   <li>运行时统计：{@code streammq:{ns}:meta:stats:{group}:{topic}}（Hash）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@UtilityClass
public class StreamMQKeys {

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

    /** PEL 认领互斥锁 Key 段 */
    public static final String TYPE_PELCLAIM_LOCK = "pelclaim-lock";

    /** 广播组注册表 Key 段 */
    public static final String TYPE_BROADCAST = "broadcast";

    /** 广播组注册表后缀段 */
    public static final String SEG_REGISTRY = "-registry";

    /**
     * 广播组注册表 Key：{@code
     * streammq:{ns}:broadcast-registry}（ZSet，member={topic}|{effectiveGroup}，score=最后心跳毫秒）
     */
    public static String broadcastRegistry(String namespace) {
        return prefix(namespace) + SEP + TYPE_BROADCAST + SEG_REGISTRY;
    }

    /** 事务锁类型段 */
    public static final String TYPE_TXLOCK = "txlock";

    /** 元数据类型段 */
    public static final String TYPE_META = "meta";

    /** 追踪类型段 */
    public static final String TYPE_TRACE = "trace";

    /** 隔离区类型段（调度条目 payload 丢失时的隔离登记） */
    public static final String TYPE_QUARANTINE = "quarantine";

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

    /** 执行权 claim 后缀 */
    public static final String SEG_CLAIM = "claim";

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

    /** 配置后缀 */
    public static final String SEG_CONFIG = "config";

    /** 自定义延时等级段 */
    public static final String SEG_CUSTOM = "custom";

    /**
     * 拼接命名空间前缀段：{@code streammq:{ns}}。 当 ns 为空时返回 {@code streammq}。
     *
     * @param namespace 命名空间，可为 null 或空字符串
     * @return 前缀段
     */
    public static String prefix(String namespace) {
        if (StringUtils.isEmpty(namespace)) {
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

    /** 消费组实例列表 Hash Key：{@code streammq:{ns}:cg:{group}:instances}。 */
    public static String consumerGroupInstances(String namespace, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_CG
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + SEG_INSTANCES;
    }

    /** 消费组信号量 Key：{@code streammq:{ns}:cg:{group}:semaphore}。 */
    public static String consumerGroupSemaphore(String namespace, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_CG
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + SEG_SEMAPHORE;
    }

    /** 消费组分片分配 Hash Key：{@code streammq:{ns}:cg:{group}:assignment}。 */
    public static String consumerGroupAssignment(String namespace, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_CG
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + SEG_ASSIGNMENT;
    }

    /** 消费组通知频道 Key：{@code streammq:{ns}:cg:{group}:notify}。 */
    public static String consumerGroupNotify(String namespace, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_CG
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + SEG_NOTIFY;
    }

    /**
     * 重试队列 ZSet Key：{@code streammq:{ns}:retry:{topic}:{group}}。
     * 用于调度延迟重试，score=nextRetryAt(ms)，member=msgId。
     */
    public static String retryZSet(String namespace, String topic, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_RETRY
                + SEP
                + requireNonEmpty(topic, "topic")
                + SEP
                + requireNonEmpty(group, "group");
    }

    /**
     * 重试消息 Stream Key：{@code streammq:{ns}:retry:msg:{topic}:{group}}（对齐 RocketMQ %RETRY%{group}%）。
     *
     * <p>并发消费失败的消息经 RetryScheduler 延迟后 XADD 到此 Stream（非原 topic Stream）， 消费者同时订阅原 Stream 和此 retry
     * Stream，实现 retry 与新消息隔离。 消费组与原 Stream 相同，retryTimes 字段递增标识重试次数。
     *
     * @param namespace 命名空间
     * @param topic 原始主题
     * @param group 消费者组名
     * @return retry Stream Key
     */
    public static String retryStream(String namespace, String topic, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_RETRY
                + SEP
                + TYPE_MSG
                + SEP
                + requireNonEmpty(topic, "topic")
                + SEP
                + requireNonEmpty(group, "group");
    }

    /**
     * 死信队列 Stream Key：{@code streammq:{ns}:dlq:{group}}（对齐 RocketMQ %DLQ%{group}）。 按消费者组隔离，一个组的所有
     * topic 的死信消息混在同一 DLQ Stream 中， 消息本身携带 topic 字段，消费时可从消息字段获取原 topic。
     */
    public static String dlqStream(String namespace, String group) {
        return prefix(namespace) + SEP + TYPE_DLQ + SEP + requireNonEmpty(group, "group");
    }

    /**
     * 二级死信队列 Stream Key：{@code streammq:{ns}:{prefix}:{group}}。 当 DLQ 消费重试耗尽时（配合 {@code
     * SecondaryDlqFailureStrategy}），消息转投到此 Stream。
     *
     * @param namespace 命名空间
     * @param group 消费者组名
     * @param prefix 二级 DLQ 前缀段（默认 "dlq2"）
     * @return 二级 DLQ Stream Key
     */
    public static String secondaryDlqStream(String namespace, String group, String prefix) {
        return prefix(namespace)
                + SEP
                + requireNonEmpty(prefix, "prefix")
                + SEP
                + requireNonEmpty(group, "group");
    }

    /** 重试转移降级锁 Key：{@code streammq:{ns}:retry:{topic}:{group}:transfer:lock}。 */
    public static String retryTransferLock(String namespace, String topic, String group) {
        return retryZSet(namespace, topic, group) + SEP + SEG_TRANSFER + SEP + SEG_LOCK;
    }

    /**
     * 转移执行权 claim Key：{@code streammq:{ns}:transfer:claim:{kind}:{scope}:{msgId}}。
     *
     * <p>Retry/Delay 调度器在「读取 payload → 原子批转投」临界区前通过 SETNX+TTL 获取， 用于多实例互斥；持有者崩溃后 claim 随 TTL
     * 过期，其它实例可接管，消息不丢失。
     *
     * <p>scope 内部多段以 {@code ':'} 连接（如 {@code topic:group}）——topic/group 命名校验禁止冒号， 因此 {@code
     * ("a_b","c")} 与 {@code ("a","b_c")} 不会拼接出相同 Key（旧实现用 {@code '_'} 存在碰撞）。
     *
     * @param namespace 命名空间
     * @param kind 转移类型（如 {@code retry} / {@code delay}）
     * @param scope 作用域标识（如 {@code topic:group} 或延时 level）
     * @param msgId 消息 ID
     * @return claim Key
     */
    public static String transferClaim(String namespace, String kind, String scope, String msgId) {
        return prefix(namespace)
                + SEP
                + SEG_TRANSFER
                + SEP
                + SEG_CLAIM
                + SEP
                + requireNonEmpty(kind, "kind")
                + SEP
                + requireNonEmpty(scope, "scope")
                + SEP
                + requireNonEmpty(msgId, "msgId");
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
     * 自定义延时 ZSet Key（v1.0+ 任意延时）：{@code streammq:{ns}:delay:custom}。
     *
     * <p>用于支持 {@code delayTimeMillis} 任意延时，不依赖固定 {@link io.github.streammq.core.enums.DelayLevel}。
     *
     * @param namespace 命名空间
     * @return ZSet Key
     */
    public static String delayCustomZSet(String namespace) {
        return prefix(namespace) + SEP + TYPE_DELAY + SEP + SEG_CUSTOM;
    }

    /**
     * 延时消息 payload Hash Key：{@code streammq:{ns}:delay:payload:{msgId}}。
     *
     * @param namespace 命名空间
     * @param msgId 消息 ID
     * @return Hash Key
     */
    public static String delayPayloadHash(String namespace, String msgId) {
        return prefix(namespace)
                + SEP
                + TYPE_DELAY
                + SEP
                + SEG_PAYLOAD
                + SEP
                + requireNonEmpty(msgId, "msgId");
    }

    /**
     * 重试消息 payload Hash Key：{@code streammq:{ns}:retry:payload:{topic}:{group}:{msgId}}。
     *
     * <p>与延时消息 payload 分离，避免两种调度在同一 Key 空间中混淆或潜在冲突。
     *
     * <p>键中必须包含 topic 与 group：Redis Stream Entry ID 仅在单个 Stream 内唯一， 不同 Topic 的 Stream 可能产生形如
     * {@code 1730000000000-0} 的相同 ID 字符串；若仅以 msgId 作键， 多 Topic 并发重试时会互相覆盖/误读 payload，造成跨 Topic
     * 消息错投。
     *
     * @param namespace 命名空间
     * @param topic 消息所属 Topic
     * @param group 消费者组
     * @param msgId 消息 ID
     * @return Hash Key
     */
    public static String retryPayloadHash(
            String namespace, String topic, String group, String msgId) {
        return prefix(namespace)
                + SEP
                + TYPE_RETRY
                + SEP
                + SEG_PAYLOAD
                + SEP
                + requireNonEmpty(topic, "topic")
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + requireNonEmpty(msgId, "msgId");
    }

    /** 延时已投递计数 Hash Key：{@code streammq:{ns}:delay:meta:delivered}。 */
    public static String delayDeliveredCounter(String namespace) {
        return prefix(namespace) + SEP + TYPE_DELAY + SEP + TYPE_META + SEP + SEG_DELIVERED;
    }

    /** 半消息暂存 Stream Key：{@code streammq:{ns}:half:{txGroup}}。 */
    public static String halfStream(String namespace, String txGroup) {
        return prefix(namespace) + SEP + TYPE_HALF + SEP + requireNonEmpty(txGroup, "txGroup");
    }

    /** 事务状态 Hash Key：{@code streammq:{ns}:txstate:{txGroup}}。 */
    public static String transactionStateHash(String namespace, String txGroup) {
        return prefix(namespace) + SEP + TYPE_TXSTATE + SEP + requireNonEmpty(txGroup, "txGroup");
    }

    /** 事务回查 ZSet Key：{@code streammq:{ns}:txcheck:{txGroup}}。 */
    public static String transactionCheckZSet(String namespace, String txGroup) {
        return prefix(namespace) + SEP + TYPE_TXCHECK + SEP + requireNonEmpty(txGroup, "txGroup");
    }

    /** 事务回查计数 Hash Key：{@code streammq:{ns}:txcheck:{txGroup}:counter}。 */
    public static String transactionCheckCounter(String namespace, String txGroup) {
        return transactionCheckZSet(namespace, txGroup) + SEP + SEG_COUNTER;
    }

    /**
     * 事务分布式锁 Key：{@code streammq:{ns}:txlock:{txGroup}:{txId}}。
     *
     * <p>用于防止多实例并发提交/回滚同一事务（TOCTOU 保护）。
     *
     * @param namespace 命名空间
     * @param txGroup 事务组名
     * @param txId 事务 ID
     * @return 锁 Key
     */
    public static String transactionLock(String namespace, String txGroup, String txId) {
        return prefix(namespace)
                + SEP
                + TYPE_TXLOCK
                + SEP
                + requireNonEmpty(txGroup, "txGroup")
                + SEP
                + requireNonEmpty(txId, "txId");
    }

    /** 顺序消费分片锁 Key：{@code streammq:{ns}:shardlock:{topic}:{group}:{shardId}}。 */
    public static String shardLock(String namespace, String topic, String group, int shardId) {
        return prefix(namespace)
                + SEP
                + TYPE_SHARDLOCK
                + SEP
                + requireNonEmpty(topic, "topic")
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + shardId;
    }

    /** PEL 认领互斥锁 Key：{@code streammq:{ns}:pelclaim-lock:{topic}:{group}}。 */
    public static String pelClaimLock(String namespace, String topic, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_PELCLAIM_LOCK
                + SEP
                + requireNonEmpty(topic, "topic")
                + SEP
                + requireNonEmpty(group, "group");
    }

    /** 消费位点 String Key：{@code streammq:{ns}:meta:offset:{group}:{topic}}。 */
    public static String metaOffset(String namespace, String group, String topic) {
        return prefix(namespace)
                + SEP
                + TYPE_META
                + SEP
                + SEG_OFFSET
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + requireNonEmpty(topic, "topic");
    }

    /** 消费计数 Hash Key：{@code streammq:{ns}:meta:counter:{group}:{topic}}。 */
    public static String metaCounter(String namespace, String group, String topic) {
        return prefix(namespace)
                + SEP
                + TYPE_META
                + SEP
                + SEG_COUNTER
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + requireNonEmpty(topic, "topic");
    }

    /** 运行时统计 Hash Key：{@code streammq:{ns}:meta:stats:{group}:{topic}}。 */
    public static String metaStats(String namespace, String group, String topic) {
        return prefix(namespace)
                + SEP
                + TYPE_META
                + SEP
                + SEG_STATS
                + SEP
                + requireNonEmpty(group, "group")
                + SEP
                + requireNonEmpty(topic, "topic");
    }

    /**
     * 消费组配置 Hash Key：{@code streammq:{ns}:meta:config:{group}}。
     *
     * <p>用于存储消费组的可变配置（如并发度、重试次数等），通过管理端点动态更新。
     *
     * @param namespace 命名空间
     * @param group 消费者组名
     * @return 配置 Hash Key
     */
    public static String metaConfig(String namespace, String group) {
        return prefix(namespace)
                + SEP
                + TYPE_META
                + SEP
                + SEG_CONFIG
                + SEP
                + requireNonEmpty(group, "group");
    }

    /**
     * 追踪数据 Stream Key：{@code streammq:{ns}:trace:{date}}。
     *
     * <p>按日期分片存储追踪记录，date 格式为 {@code yyyyMMdd}。 便于按天查询与过期清理。
     *
     * @param namespace 命名空间
     * @param date 日期字符串（格式 yyyyMMdd）
     * @return 追踪 Stream Key
     */
    public static String traceStream(String namespace, String date) {
        return prefix(namespace) + SEP + TYPE_TRACE + SEP + requireNonEmpty(date, "date");
    }

    /**
     * 隔离区 ZSet Key：{@code streammq:{ns}:quarantine:{kind}}。
     *
     * <p>调度器（delay/retry）发现 payload Hash 已被 7 天 TTL 回收但调度条目仍存在时， 先把 {@code
     * member=msgId|kind、score=dueTime} 写入隔离区再移除活跃 ZSet 条目——静默删除改为 可观测的隔离登记，运维可排查/重放。
     *
     * @param namespace 命名空间
     * @param kind 调度类型（如 {@code delay} / {@code retry}）
     * @return 隔离区 ZSet Key
     */
    public static String quarantineZset(String namespace, String kind) {
        return prefix(namespace) + SEP + TYPE_QUARANTINE + SEP + requireNonEmpty(kind, "kind");
    }

    private static String requireNonEmpty(String value, String name) {
        return Objects.requireNonNull(value, name + " must not be null").isEmpty()
                ? throwEmpty(name)
                : value;
    }

    private static String throwEmpty(String name) {
        throw new IllegalArgumentException(name + " must not be empty");
    }
}
