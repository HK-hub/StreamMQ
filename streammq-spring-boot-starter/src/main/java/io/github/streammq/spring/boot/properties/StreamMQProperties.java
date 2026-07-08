package io.github.streammq.spring.boot.properties;

import io.github.streammq.core.StreamMQConstants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * StreamMQ 配置属性，绑定前缀 {@code streammq}。
 *
 * <p>典型 {@code application.yml} 示例：
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
 *   retry:
 *     policy: io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy
 *     max-reconsume-times: 16
 *   delay:
 *     enabled: true
 *     scan-interval: 1s
 *     batch-size: 100
 *   transaction:
 *     default-group: default-tx-group
 *     check-interval: 60s
 *     max-check-times: 15
 *   health:
 *     enabled: true
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ConfigurationProperties(prefix = "streammq")
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

    /** Redis 鉴权 accessKey */
    private String accessKey = "";

    /** Redis 鉴权 secretKey */
    private String secretKey = "";

    /** 线程名前缀 */
    private String threadNamePrefix = StreamMQConstants.THREAD_PREFIX;

    /** 全局追踪开关 */
    private boolean tracingEnabled = false;

    /** 重平衡策略配置 */
    private Rebalance rebalance = new Rebalance();

    /** 追踪配置 */
    private Tracing tracing = new Tracing();

    // ===================== 子配置 =====================

    /**
     * 生产者配置。
     */
    @Data
    public static class Producer {
        /** 默认生产者组名 */
        private String group = StreamMQConstants.DEFAULT_PRODUCER_GROUP;
        /** 默认发送超时（毫秒） */
        private long sendMessageTimeout = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;
        /** 默认同步发送重试次数 */
        private int retryTimes = StreamMQConstants.DEFAULT_SYNC_RETRY_TIMES;
        /** Stream 最大长度（0 = 不限制） */
        private int streamMaxLen = 0;
        /** 序列化器实现类全限定名（默认 JacksonJsonSerializer） */
        private String serializer = "io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer";
    }

    /**
     * 消费者配置。
     */
    @Data
    public static class Consumer {
        /** 单次拉取阻塞超时 */
        private Duration pollTimeout = Duration.ofSeconds(1);
        /** 单次拉取批量大小 */
        private int batchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;
        /** 拉取间隔（毫秒），0=不间隔 */
        private long pullInterval = 0L;
        /** 暂停休眠间隔（毫秒） */
        private long pausedSleepMillis = StreamMQConstants.DEFAULT_PAUSED_SLEEP_MS;
        /** Broker 异常退避间隔（毫秒） */
        private long brokerErrorBackoffMillis = StreamMQConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;
        /** 最大拉取批量上界 */
        private int maxBatchSizeLimit = StreamMQConstants.MAX_BATCH_SIZE_LIMIT;
    }

    /**
     * 死信队列配置。
     */
    @Data
    public static class Dlq {
        /** 死信消费失败处理策略实现类全限定名（默认 LogAndDropDlqFailureStrategy） */
        private String failureStrategy = StreamMQConstants.DEFAULT_DLQ_FAILURE_STRATEGY;
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
        private double retryBackoffMultiplier = StreamMQConstants.DEFAULT_DLQ_RETRY_BACKOFF_MULTIPLIER;
        /** 重试延迟上限（毫秒，默认 300000） */
        private long retryMaxDelayMs = StreamMQConstants.DEFAULT_DLQ_RETRY_MAX_DELAY_MS;
    }

    /**
     * 重试策略配置。
     */
    @Data
    public static class Retry {
        /** 重试功能开关 */
        private boolean enabled = true;
        /** 重试策略实现类全限定名 */
        private String policy = "io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy";
        /** 默认最大重试次数 */
        private int maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;
        /** 重试 ZSet 扫描间隔 */
        private Duration scanInterval = Duration.ofSeconds(1);
        /** 单次扫描批量 */
        private int batchSize = StreamMQConstants.DEFAULT_BATCH_SIZE;
        /** 自定义重试延时数组（逗号分隔的毫秒值，如 1000,5000,10000） */
        private String delayArray = "";
        /** retry Stream 最大长度（0=不限制），对齐 RocketMQ retry topic 容量控制 */
        private int streamMaxLen = StreamMQConstants.DEFAULT_RETRY_STREAM_MAX_LEN;
        /** PEL 认领扫描间隔（顺序消费专用，默认 5s） */
        private Duration pelClaimScanInterval = Duration.ofMillis(StreamMQConstants.DEFAULT_PEL_CLAIM_SCAN_INTERVAL_MS);
        /** PEL 认领空闲阈值（顺序消费专用，默认 30s） */
        private long pelClaimMinIdleMs = StreamMQConstants.DEFAULT_PEL_CLAIM_MIN_IDLE_MS;
    }

    /**
     * 延时消息调度器配置。
     */
    @Data
    public static class Delay {
        /** 是否启用延时消息调度器，默认 true */
        private boolean enabled = true;
        /** 扫描间隔 */
        private Duration scanInterval = Duration.ofSeconds(1);
        /** 单次扫描批量 */
        private int batchSize = StreamMQConstants.DEFAULT_BATCH_SIZE;
    }

    /**
     * 事务消息配置。
     */
    @Data
    public static class Transaction {
        /** 事务消息功能开关 */
        private boolean enabled = true;
        /** 默认事务组名 */
        private String defaultGroup = StreamMQConstants.DEFAULT_TX_GROUP;
        /** 事务回查间隔 */
        private Duration checkInterval = Duration.ofMillis(StreamMQConstants.DEFAULT_CHECK_INTERVAL_MS);
        /** 最大回查次数 */
        private int maxCheckTimes = StreamMQConstants.DEFAULT_MAX_CHECK_TIMES;
    }

    /**
     * 健康检查配置。
     */
    @Data
    public static class Health {
        /** 是否启用健康检查，默认 true（仅在 Actuator 在 classpath 时生效） */
        private boolean enabled = true;
    }

    /**
     * 重平衡策略配置。
     */
    @Data
    public static class Rebalance {
        /** 重平衡策略类名 */
        private String strategy = "io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy";
        /** 虚拟节点数（仅一致性哈希策略生效） */
        private int virtualNodes = StreamMQConstants.DEFAULT_VIRTUAL_NODES;
    }

    /**
     * 追踪配置。
     */
    @Data
    public static class Tracing {
        /** 追踪收集器类名 */
        private String collector = "io.github.streammq.adapter.redisson.trace.NoopTraceCollector";
        /** 追踪日志 Topic（仅 Slf4jTraceCollector 生效） */
        private String traceTopic = "";
    }
}
