package io.github.streammq.spring.boot.properties;

import io.github.streammq.core.StreamMqConstants;
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

    /** 事务消息配置 */
    private Transaction transaction = new Transaction();

    /** 健康检查配置 */
    private Health health = new Health();

    /** Redis 鉴权 accessKey */
    private String accessKey = "";

    /** Redis 鉴权 secretKey */
    private String secretKey = "";

    /** 线程名前缀 */
    private String threadNamePrefix = StreamMqConstants.THREAD_PREFIX;

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
        private String group = StreamMqConstants.DEFAULT_PRODUCER_GROUP;
        /** 默认发送超时（毫秒） */
        private long sendMessageTimeout = StreamMqConstants.DEFAULT_SEND_TIMEOUT_MS;
        /** 默认同步发送重试次数 */
        private int retryTimes = StreamMqConstants.DEFAULT_SYNC_RETRY_TIMES;
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
        private int batchSize = StreamMqConstants.DEFAULT_CONSUME_BATCH_SIZE;
        /** 拉取间隔（毫秒），0=不间隔 */
        private long pullInterval = 0L;
        /** 暂停休眠间隔（毫秒） */
        private long pausedSleepMillis = StreamMqConstants.DEFAULT_PAUSED_SLEEP_MS;
        /** Broker 异常退避间隔（毫秒） */
        private long brokerErrorBackoffMillis = StreamMqConstants.DEFAULT_BROKER_ERROR_BACKOFF_MS;
        /** 最大拉取批量上界 */
        private int maxBatchSizeLimit = StreamMqConstants.MAX_BATCH_SIZE_LIMIT;
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
        private int maxReconsumeTimes = StreamMqConstants.DEFAULT_MAX_RECONSUME_TIMES;
        /** 重试 ZSet 扫描间隔 */
        private Duration scanInterval = Duration.ofSeconds(1);
        /** 单次扫描批量 */
        private int batchSize = StreamMqConstants.DEFAULT_BATCH_SIZE;
        /** 自定义重试延时数组（逗号分隔的毫秒值，如 1000,5000,10000） */
        private String delayArray = "";
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
        private int batchSize = StreamMqConstants.DEFAULT_BATCH_SIZE;
    }

    /**
     * 事务消息配置。
     */
    @Data
    public static class Transaction {
        /** 事务消息功能开关 */
        private boolean enabled = true;
        /** 默认事务组名 */
        private String defaultGroup = StreamMqConstants.DEFAULT_TX_GROUP;
        /** 事务回查间隔 */
        private Duration checkInterval = Duration.ofMillis(StreamMqConstants.DEFAULT_CHECK_INTERVAL_MS);
        /** 最大回查次数 */
        private int maxCheckTimes = StreamMqConstants.DEFAULT_MAX_CHECK_TIMES;
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
        private int virtualNodes = StreamMqConstants.DEFAULT_VIRTUAL_NODES;
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
