package io.github.streammq.spring.boot.properties;

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

    // ===================== 子配置 =====================

    /**
     * 生产者配置。
     */
    public static class Producer {
        /** 默认生产者组名 */
        private String group = "default-producer";
        /** 默认发送超时（毫秒） */
        private long sendMessageTimeout = 3000L;
        /** 默认同步发送重试次数 */
        private int retryTimes = 2;
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
        private int batchSize = 32;

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
    }

    /**
     * 重试策略配置。
     */
    public static class Retry {
        /** 重试策略实现类全限定名 */
        private String policy = "io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy";
        /** 默认最大重试次数 */
        private int maxReconsumeTimes = 16;
        /** 重试 ZSet 扫描间隔 */
        private Duration scanInterval = Duration.ofSeconds(1);
        /** 单次扫描批量 */
        private int batchSize = 100;

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
        private int batchSize = 100;

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
        /** 默认事务组名 */
        private String defaultGroup = "default-tx-group";
        /** 事务回查间隔 */
        private Duration checkInterval = Duration.ofSeconds(60);
        /** 最大回查次数 */
        private int maxCheckTimes = 15;

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
}
