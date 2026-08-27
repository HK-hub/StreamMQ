/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.properties;

import io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy;
import io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy;
import io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ 配置属性，绑定前缀 {@code streammq}。
 *
 * <p>典型 {@code application.yml} 示例：
 *
 * <pre>{@code
 * streammq:
 *   enabled: true
 *   namespace: streammq
 *   producer:
 *     group: default-producer
 *     send-message-timeout: 3000
 *     retry-times: 2
 *     stream-max-len: 10000
 *     serializer: io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer
 *   consumer:
 *     poll-timeout: 1s
 *     batch-size: 32
 *     timeout-cancel-grace-millis: 2000
 *   group:
 *     heartbeat-interval-ms: 5000
 *     instance-timeout-ms: 20000
 *   retry:
 *     policy: io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy
 *     max-reconsume-times: 16
 *     failure-requeue-backoff-ms: 5000
 *   delay:
 *     enabled: true
 *     scan-interval: 1s
 *     batch-size: 100
 *   dlq:
 *     min-retry-delay-ms: 1000
 *   transaction:
 *     default-group: default-tx-group
 *     check-interval: 60s
 *     max-check-times: 15
 *   health:
 *     enabled: true
 *   admin:
 *     list-page-size: 100
 *     max-pending-query-size: 1000
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ConfigurationProperties(prefix = StreamMQSpringConstants.PROP_PREFIX)
@Data
public class StreamMQProperties {

    /** 是否启用 StreamMQ 自动装配，默认 true */
    private boolean enabled = true;

    /** 命名空间（用于多租户/多环境隔离），默认空字符串 */
    private String namespace = "";

    /** 生产者配置 */
    private Producer producer = new Producer();

    /** 消费者配置 */
    private Consumer consumer = new Consumer();

    /** 消费者组管理配置（心跳与实例存活判定） */
    private Group group = new Group();

    /** 重试策略配置 */
    private Retry retry = new Retry();

    /** 延时消息调度器配置 */
    private Delay delay = new Delay();

    /** 死信队列配置 */
    private Dlq dlq = new Dlq();

    /** 事务消息配置 */
    private Transaction transaction = new Transaction();

    /** 健康检查配置 */
    private Health health = new Health();

    /** 重平衡策略配置 */
    private Rebalance rebalance = new Rebalance();

    /** 追踪配置 */
    private Tracing tracing = new Tracing();

    /** 追踪存储与查询配置（v1.0+） */
    private Trace trace = new Trace();

    /** 管理端点配置 */
    private Admin admin = new Admin();

    // ===================== 子配置 =====================

    /** 生产者配置。 */
    @Data
    public static class Producer {
        /**
         * 默认生产者组名。
         *
         * <p>命名规则：仅允许字母、数字、连字符（-）和下划线（_），长度不超过 128 字符。 中文和特殊字符可能导致 Redis 操作失败。
         */
        private String group = StreamMQConstants.DEFAULT_PRODUCER_GROUP;

        /** 默认发送超时（毫秒） */
        private long sendMessageTimeout = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

        /** 默认同步发送重试次数 */
        private int retryTimes = StreamMQConstants.DEFAULT_SYNC_RETRY_TIMES;

        /** Stream 最大长度（0 = 不限制） */
        private int streamMaxLen = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

        /** 序列化器实现类，默认 {@link JacksonJsonSerializer} */
        private Class<? extends MessageSerializer> serializer = JacksonJsonSerializer.class;

        /** 消息体压缩阈值（字节），body 超过此值时触发压缩，0 = 禁用（默认禁用） */
        private int compressThreshold = StreamMQConstants.DEFAULT_COMPRESS_THRESHOLD_BYTES;

        /** 单条消息最大大小（字节），发送时校验。 默认 512MB（Redis Stream 上限），推荐不超过 1MB。 */
        private long maxMessageSize = StreamMQConstants.MAX_MESSAGE_SIZE_BYTES;
    }

    /** 消费者配置。 */
    @Data
    public static class Consumer {
        /** 单次拉取阻塞超时 */
        private Duration pollTimeout =
                Duration.ofMillis(StreamMQConstants.DEFAULT_PULL_BLOCK_TIMEOUT_MS);

        /** 单次拉取批量大小 */
        private int batchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

        /** 拉取间隔（毫秒），0=不间隔 */
        private long pullInterval = StreamMQConstants.DEFAULT_PULL_INTERVAL_MS;

        /** 暂停休眠间隔（毫秒） */
        private long pausedSleepMillis = StreamMQConstants.DEFAULT_PAUSED_SLEEP_MS;

        /** Broker 异常退避间隔（毫秒） */
        private long brokerErrorBackoffMillis = StreamMQConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;

        /** 最大拉取批量上界 */
        private int maxBatchSizeLimit = StreamMQConstants.MAX_BATCH_SIZE_LIMIT;

        /**
         * 背压队列容量：{@code >0} 启用拉取/处理解耦（队列满时拉取阻塞），{@code 0} 禁用。 默认 0（禁用，与 DEFAULT_INFLIGHT_CAPACITY
         * 一致）。
         */
        private int inflightCapacity = StreamMQConstants.DEFAULT_INFLIGHT_CAPACITY;

        /**
         * 消费超时取消后的宽限期（毫秒）：等待业务线程真正终止， 用于缩小与重试副本的重叠窗口。 默认 {@link
         * StreamMQConstants#DEFAULT_TIMEOUT_CANCEL_GRACE_MS}。
         */
        private long timeoutCancelGraceMillis = StreamMQConstants.DEFAULT_TIMEOUT_CANCEL_GRACE_MS;
    }

    /** 消费者组管理配置（心跳与实例存活判定）。 */
    @Data
    public static class Group {
        /** 心跳上报间隔（毫秒），默认 {@link StreamMQConstants#DEFAULT_HEARTBEAT_INTERVAL_MS} */
        private long heartbeatIntervalMs = StreamMQConstants.DEFAULT_HEARTBEAT_INTERVAL_MS;

        /**
         * 实例超时时间（毫秒），超过该时长无心跳的实例将被移出分配， 默认 {@link StreamMQConstants#DEFAULT_INSTANCE_TIMEOUT_MS}。
         */
        private long instanceTimeoutMs = StreamMQConstants.DEFAULT_INSTANCE_TIMEOUT_MS;
    }

    /** 死信队列配置。 */
    @Data
    public static class Dlq {
        /** 死信消费失败处理策略实现类，默认 {@link LogAndDropDlqFailureStrategy} */
        private Class<? extends DlqFailureStrategy> failureStrategy =
                LogAndDropDlqFailureStrategy.class;

        /** DLQ 消费失败后的最大重试次数（默认 3） */
        private int maxDlqRetryAttempts = StreamMQConstants.DEFAULT_DLQ_MAX_RETRY_ATTEMPTS;

        /** DLQ 消费重试延迟（毫秒，默认 10000） */
        private long dlqRetryDelayMs = StreamMQConstants.DEFAULT_DLQ_RETRY_DELAY_MS;

        /** 是否启用二级死信队列（默认 false） */
        private boolean secondaryDlqEnabled = StreamMQConstants.DEFAULT_SECONDARY_DLQ_ENABLED;

        /** 二级死信 Stream Key 前缀段（默认 "dlq2"） */
        private String secondaryDlqKeyPrefix = StreamMQConstants.DEFAULT_SECONDARY_DLQ_KEY_PREFIX;

        /** 告警阈值（默认 1） */
        private int alertThreshold = StreamMQConstants.DEFAULT_DLQ_ALERT_THRESHOLD;

        /** 重试退避倍数（默认 1.0 = 固定延迟） */
        private double retryBackoffMultiplier =
                StreamMQConstants.DEFAULT_DLQ_RETRY_BACKOFF_MULTIPLIER;

        /** 重试延迟上限（毫秒，默认 300000） */
        private long retryMaxDelayMs = StreamMQConstants.DEFAULT_DLQ_RETRY_MAX_DELAY_MS;

        /** DLQ 重试最小延迟下限（毫秒），默认 {@link StreamMQConstants#MIN_DLQ_RETRY_DELAY_MS} */
        private long minRetryDelayMs = StreamMQConstants.MIN_DLQ_RETRY_DELAY_MS;
    }

    /** 重试策略配置。 */
    @Data
    public static class Retry {
        /** 重试功能开关 */
        private boolean enabled = true;

        /** 重试策略实现类，默认 {@link FixedArrayRetryPolicy} */
        private Class<? extends RetryPolicy> policy = FixedArrayRetryPolicy.class;

        /** 默认最大重试次数 */
        private int maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

        /** 重试 ZSet 扫描间隔 */
        private Duration scanInterval =
                Duration.ofMillis(StreamMQConstants.DEFAULT_SCAN_INTERVAL_MS);

        /** 单次扫描批量 */
        private int batchSize = StreamMQConstants.DEFAULT_BATCH_SIZE;

        /** 自定义重试延时数组（逗号分隔的毫秒值，如 1000,5000,10000） */
        private String delayArray = "";

        /** retry Stream 最大长度（0=不限制），对齐 RocketMQ retry topic 容量控制 */
        private int streamMaxLen = StreamMQConstants.DEFAULT_RETRY_STREAM_MAX_LEN;

        /** PEL 认领扫描间隔（顺序消费专用，默认 5s） */
        private Duration pelClaimScanInterval =
                Duration.ofMillis(StreamMQConstants.DEFAULT_PEL_CLAIM_SCAN_INTERVAL_MS);

        /** PEL 认领空闲阈值（顺序消费专用，默认 30s） */
        private long pelClaimMinIdleMs = StreamMQConstants.DEFAULT_PEL_CLAIM_MIN_IDLE_MS;

        /**
         * 转移失败后的回写退避间隔（毫秒）：避免 Redis 故障时以扫描间隔高频热循环重试， 默认 {@link
         * StreamMQConstants#DEFAULT_FAILURE_REQUEUE_BACKOFF_MS}。
         */
        private long failureRequeueBackoffMs = StreamMQConstants.DEFAULT_FAILURE_REQUEUE_BACKOFF_MS;
    }

    /** 延时消息调度器配置。 */
    @Data
    public static class Delay {
        /** 是否启用延时消息调度器，默认 true */
        private boolean enabled = true;

        /** 扫描间隔 */
        private Duration scanInterval =
                Duration.ofMillis(StreamMQConstants.DEFAULT_SCAN_INTERVAL_MS);

        /** 单次扫描批量 */
        private int batchSize = StreamMQConstants.DEFAULT_BATCH_SIZE;

        /**
         * 转移失败后的回写退避间隔（毫秒）：避免 Redis 故障时以扫描间隔高频热循环重试， 默认 {@link
         * StreamMQConstants#DEFAULT_FAILURE_REQUEUE_BACKOFF_MS}。
         */
        private long failureRequeueBackoffMs = StreamMQConstants.DEFAULT_FAILURE_REQUEUE_BACKOFF_MS;
    }

    /** 事务消息配置。 */
    @Data
    public static class Transaction {
        /** 事务消息功能开关 */
        private boolean enabled = true;

        /** 默认事务组名 */
        private String defaultGroup = StreamMQConstants.DEFAULT_TX_GROUP;

        /** 事务回查间隔 */
        private Duration checkInterval =
                Duration.ofMillis(StreamMQConstants.DEFAULT_CHECK_INTERVAL_MS);

        /** 最大回查次数 */
        private int maxCheckTimes = StreamMQConstants.DEFAULT_MAX_CHECK_TIMES;
    }

    /** 健康检查配置。 */
    @Data
    public static class Health {
        /** 是否启用健康检查，默认 true（仅在 Actuator 在 classpath 时生效） */
        private boolean enabled = true;
    }

    /** 重平衡策略配置。 */
    @Data
    public static class Rebalance {
        /** 重平衡策略实现类，默认 {@link ConsistentHashRebalanceStrategy} */
        private Class<? extends RebalanceStrategy> strategy = ConsistentHashRebalanceStrategy.class;

        /** 虚拟节点数（仅一致性哈希策略生效） */
        private int virtualNodes = StreamMQConstants.DEFAULT_VIRTUAL_NODES;
    }

    /** 追踪配置。 */
    @Data
    public static class Tracing {
        /** 是否启用追踪，对应 {@code streammq.tracing.enabled}（默认 false） */
        private boolean enabled = false;
    }

    /**
     * 追踪存储与查询配置（v1.0+）。
     *
     * <p>与 {@link Tracing} 区别：Tracing 控制日志级别的追踪输出， Trace 控制追踪数据的持久化存储与查询能力。
     */
    @Data
    public static class Trace {
        /** 是否启用追踪存储与查询服务 */
        private boolean enabled = false;

        /**
         * 追踪存储方式（{@link io.github.streammq.core.enums.TraceStorageType#REDIS} 启用 Redis Stream
         * 存储，其他值禁用）
         */
        private String storage = io.github.streammq.core.enums.TraceStorageType.NONE.getCode();

        /** 单日单次追踪查询最大读取条数，超出部分静默截断。 默认 {@link StreamMQConstants#DEFAULT_TRACE_MAX_READ_COUNT}。 */
        private int maxReadCount = StreamMQConstants.DEFAULT_TRACE_MAX_READ_COUNT;
    }

    /** 管理端点配置（Actuator 运维接口）。 */
    @Data
    public static class Admin {
        /** 管理端点开关：与 streammq.health.enabled 解耦，false 时仅关闭管理/运维 REST 端点（健康检查不受影响） */
        private boolean enabled = true;

        /** 管理端点列表默认页大小 */
        private int listPageSize = StreamMQSpringConstants.DEFAULT_LIST_PAGE_SIZE;

        /** pending 列表单次最大拉取条数 */
        private int maxPendingQuerySize = StreamMQSpringConstants.MAX_PENDING_QUERY_SIZE;
    }

    /**
     * 校验配置属性的合法性，在自动装配时调用。
     *
     * @throws IllegalArgumentException 如果配置值不合法
     */
    public void validate() {
        if (producer.sendMessageTimeout <= 0) {
            throw new IllegalArgumentException(
                    "streammq.producer.send-message-timeout must be > 0, got: "
                            + producer.sendMessageTimeout);
        }
        if (producer.retryTimes < 0) {
            throw new IllegalArgumentException(
                    "streammq.producer.retry-times must be >= 0, got: " + producer.retryTimes);
        }
        if (producer.streamMaxLen < 0) {
            throw new IllegalArgumentException(
                    "streammq.producer.stream-max-len must be >= 0, got: " + producer.streamMaxLen);
        }
        if (producer.maxMessageSize <= 0) {
            throw new IllegalArgumentException(
                    "streammq.producer.max-message-size must be > 0, got: "
                            + producer.maxMessageSize);
        }
        if (consumer.batchSize <= 0) {
            throw new IllegalArgumentException(
                    "streammq.consumer.batch-size must be > 0, got: " + consumer.batchSize);
        }
        if (consumer.pullInterval < 0) {
            throw new IllegalArgumentException(
                    "streammq.consumer.pull-interval must be >= 0, got: " + consumer.pullInterval);
        }
        if (consumer.pausedSleepMillis <= 0) {
            throw new IllegalArgumentException(
                    "streammq.consumer.paused-sleep-millis must be > 0, got: "
                            + consumer.pausedSleepMillis);
        }
        if (consumer.brokerErrorBackoffMillis <= 0) {
            throw new IllegalArgumentException(
                    "streammq.consumer.broker-error-backoff-millis must be > 0, got: "
                            + consumer.brokerErrorBackoffMillis);
        }
        if (consumer.maxBatchSizeLimit <= 0) {
            throw new IllegalArgumentException(
                    "streammq.consumer.max-batch-size-limit must be > 0, got: "
                            + consumer.maxBatchSizeLimit);
        }
        if (transaction.maxCheckTimes <= 0) {
            throw new IllegalArgumentException(
                    "streammq.transaction.max-check-times must be > 0, got: "
                            + transaction.maxCheckTimes);
        }
        if (dlq.maxDlqRetryAttempts < 0) {
            throw new IllegalArgumentException(
                    "streammq.dlq.max-dlq-retry-attempts must be >= 0, got: "
                            + dlq.maxDlqRetryAttempts);
        }
        if (dlq.dlqRetryDelayMs < 0) {
            throw new IllegalArgumentException(
                    "streammq.dlq.dlq-retry-delay-ms must be >= 0, got: " + dlq.dlqRetryDelayMs);
        }
        if (retry.maxReconsumeTimes < 0) {
            throw new IllegalArgumentException(
                    "streammq.retry.max-reconsume-times must be >= 0, got: "
                            + retry.maxReconsumeTimes);
        }
        if (retry.batchSize <= 0) {
            throw new IllegalArgumentException(
                    "streammq.retry.batch-size must be > 0, got: " + retry.batchSize);
        }
        if (delay.batchSize <= 0) {
            throw new IllegalArgumentException(
                    "streammq.delay.batch-size must be > 0, got: " + delay.batchSize);
        }
        if (consumer.timeoutCancelGraceMillis <= 0) {
            throw new IllegalArgumentException(
                    "streammq.consumer.timeout-cancel-grace-millis must be > 0, got: "
                            + consumer.timeoutCancelGraceMillis);
        }
        if (group.heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "streammq.group.heartbeat-interval-ms must be > 0, got: "
                            + group.heartbeatIntervalMs);
        }
        if (group.instanceTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "streammq.group.instance-timeout-ms must be > 0, got: "
                            + group.instanceTimeoutMs);
        }
        if (group.instanceTimeoutMs < group.heartbeatIntervalMs) {
            throw new IllegalArgumentException(
                    "streammq.group.instance-timeout-ms must be >= heartbeat-interval-ms, got: "
                            + group.instanceTimeoutMs);
        }
        if (retry.failureRequeueBackoffMs <= 0) {
            throw new IllegalArgumentException(
                    "streammq.retry.failure-requeue-backoff-ms must be > 0, got: "
                            + retry.failureRequeueBackoffMs);
        }
        if (delay.failureRequeueBackoffMs <= 0) {
            throw new IllegalArgumentException(
                    "streammq.delay.failure-requeue-backoff-ms must be > 0, got: "
                            + delay.failureRequeueBackoffMs);
        }
        if (dlq.minRetryDelayMs <= 0) {
            throw new IllegalArgumentException(
                    "streammq.dlq.min-retry-delay-ms must be > 0, got: " + dlq.minRetryDelayMs);
        }
        if (trace.maxReadCount <= 0) {
            throw new IllegalArgumentException(
                    "streammq.trace.max-read-count must be > 0, got: " + trace.maxReadCount);
        }
        if (admin.listPageSize <= 0) {
            throw new IllegalArgumentException(
                    "streammq.admin.list-page-size must be > 0, got: " + admin.listPageSize);
        }
        if (admin.maxPendingQuerySize <= 0) {
            throw new IllegalArgumentException(
                    "streammq.admin.max-pending-query-size must be > 0, got: "
                            + admin.maxPendingQuerySize);
        }
    }
}
