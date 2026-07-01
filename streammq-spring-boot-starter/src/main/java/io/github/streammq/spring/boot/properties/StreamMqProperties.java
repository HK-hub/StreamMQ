package io.github.streammq.spring.boot.properties;

import io.github.streammq.core.StreamMqConstants;
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
public class StreamMqProperties {

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

    /** Redisson 配置（仅用于内部 RedissonClient 创建，默认使用用户已注册的 RedissonClient Bean） */
    private Redisson redisson = new Redisson();

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

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public Producer getProducer() {
        return producer;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Delay getDelay() {
        return delay;
    }

    public void setDelay(Delay delay) {
        this.delay = delay;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public Health getHealth() {
        return health;
    }

    public void setHealth(Health health) {
        this.health = health;
    }

    public Redisson getRedisson() {
        return redisson;
    }

    public void setRedisson(Redisson redisson) {
        this.redisson = redisson;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getThreadNamePrefix() {
        return threadNamePrefix;
    }

    public void setThreadNamePrefix(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
    }

    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    public Rebalance getRebalance() {
        return rebalance;
    }

    public void setRebalance(Rebalance rebalance) {
        this.rebalance = rebalance;
    }

    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }

    // ===================== 子配置 =====================

    /**
     * 生产者配置。
     */
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

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public long getSendMessageTimeout() {
            return sendMessageTimeout;
        }

        public void setSendMessageTimeout(long sendMessageTimeout) {
            this.sendMessageTimeout = sendMessageTimeout;
        }

        public int getRetryTimes() {
            return retryTimes;
        }

        public void setRetryTimes(int retryTimes) {
            this.retryTimes = retryTimes;
        }

        public int getStreamMaxLen() {
            return streamMaxLen;
        }

        public void setStreamMaxLen(int streamMaxLen) {
            this.streamMaxLen = streamMaxLen;
        }

        public String getSerializer() {
            return serializer;
        }

        public void setSerializer(String serializer) {
            this.serializer = serializer;
        }
    }

    /**
     * 消费者配置。
     */
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

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getPullInterval() {
            return pullInterval;
        }

        public void setPullInterval(long pullInterval) {
            this.pullInterval = pullInterval;
        }

        public long getPausedSleepMillis() {
            return pausedSleepMillis;
        }

        public void setPausedSleepMillis(long pausedSleepMillis) {
            this.pausedSleepMillis = pausedSleepMillis;
        }

        public long getBrokerErrorBackoffMillis() {
            return brokerErrorBackoffMillis;
        }

        public void setBrokerErrorBackoffMillis(long brokerErrorBackoffMillis) {
            this.brokerErrorBackoffMillis = brokerErrorBackoffMillis;
        }

        public int getMaxBatchSizeLimit() {
            return maxBatchSizeLimit;
        }

        public void setMaxBatchSizeLimit(int maxBatchSizeLimit) {
            this.maxBatchSizeLimit = maxBatchSizeLimit;
        }
    }

    /**
     * 重试策略配置。
     */
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

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getPolicy() {
            return policy;
        }

        public void setPolicy(String policy) {
            this.policy = policy;
        }

        public int getMaxReconsumeTimes() {
            return maxReconsumeTimes;
        }

        public void setMaxReconsumeTimes(int maxReconsumeTimes) {
            this.maxReconsumeTimes = maxReconsumeTimes;
        }

        public Duration getScanInterval() {
            return scanInterval;
        }

        public void setScanInterval(Duration scanInterval) {
            this.scanInterval = scanInterval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public String getDelayArray() {
            return delayArray;
        }

        public void setDelayArray(String delayArray) {
            this.delayArray = delayArray;
        }
    }

    /**
     * 延时消息调度器配置。
     */
    public static class Delay {
        /** 是否启用延时消息调度器，默认 true */
        private boolean enabled = true;
        /** 扫描间隔 */
        private Duration scanInterval = Duration.ofSeconds(1);
        /** 单次扫描批量 */
        private int batchSize = StreamMqConstants.DEFAULT_BATCH_SIZE;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getScanInterval() {
            return scanInterval;
        }

        public void setScanInterval(Duration scanInterval) {
            this.scanInterval = scanInterval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    /**
     * 事务消息配置。
     */
    public static class Transaction {
        /** 事务消息功能开关 */
        private boolean enabled = true;
        /** 默认事务组名 */
        private String defaultGroup = StreamMqConstants.DEFAULT_TX_GROUP;
        /** 事务回查间隔 */
        private Duration checkInterval = Duration.ofMillis(StreamMqConstants.DEFAULT_CHECK_INTERVAL_MS);
        /** 最大回查次数 */
        private int maxCheckTimes = StreamMqConstants.DEFAULT_MAX_CHECK_TIMES;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getDefaultGroup() {
            return defaultGroup;
        }

        public void setDefaultGroup(String defaultGroup) {
            this.defaultGroup = defaultGroup;
        }

        public Duration getCheckInterval() {
            return checkInterval;
        }

        public void setCheckInterval(Duration checkInterval) {
            this.checkInterval = checkInterval;
        }

        public int getMaxCheckTimes() {
            return maxCheckTimes;
        }

        public void setMaxCheckTimes(int maxCheckTimes) {
            this.maxCheckTimes = maxCheckTimes;
        }
    }

    /**
     * 健康检查配置。
     */
    public static class Health {
        /** 是否启用健康检查，默认 true（仅在 Actuator 在 classpath 时生效） */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * Redisson 配置。
     *
     * <p>仅当用户未提供 {@code RedissonClient} Bean 时使用。
     * 默认行为：依赖用户应用中已注册的 {@code RedissonClient}（通常来自 {@code redisson-spring-boot-starter}）。
     */
    public static class Redisson {
        /** Redisson 配置文件路径（如 classpath:redisson.yaml），为空则使用用户已注册的 Bean */
        private String config = "";

        public String getConfig() {
            return config;
        }

        public void setConfig(String config) {
            this.config = config;
        }
    }

    /**
     * 重平衡策略配置。
     */
    public static class Rebalance {
        /** 重平衡策略类名 */
        private String strategy = "io.github.streammq.adapter.redisson.rebalance.ConsistentHashRebalanceStrategy";
        /** 虚拟节点数（仅一致性哈希策略生效） */
        private int virtualNodes = StreamMqConstants.DEFAULT_VIRTUAL_NODES;

        public String getStrategy() {
            return strategy;
        }

        public void setStrategy(String strategy) {
            this.strategy = strategy;
        }

        public int getVirtualNodes() {
            return virtualNodes;
        }

        public void setVirtualNodes(int virtualNodes) {
            this.virtualNodes = virtualNodes;
        }
    }

    /**
     * 追踪配置。
     */
    public static class Tracing {
        /** 追踪收集器类名 */
        private String collector = "io.github.streammq.adapter.redisson.trace.NoopTraceCollector";
        /** 追踪日志 Topic（仅 Slf4jTraceCollector 生效） */
        private String traceTopic = "";

        public String getCollector() {
            return collector;
        }

        public void setCollector(String collector) {
            this.collector = collector;
        }

        public String getTraceTopic() {
            return traceTopic;
        }

        public void setTraceTopic(String traceTopic) {
            this.traceTopic = traceTopic;
        }
    }
}
