package io.github.streammq.core;

/**
 * StreamMQ 全局常量定义。
 * 集中管理跨模块共享的默认值、属性 key、Bean 名前缀、线程名前缀等常量。
 */
public final class StreamMqConstants {

    private StreamMqConstants() {}

    // ==================== 默认值常量 ====================
    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_SEND_TIMEOUT_MS = 3000L;
    /** 默认同步重试次数 */
    public static final int DEFAULT_SYNC_RETRY_TIMES = 2;
    /** 默认异步重试次数 */
    public static final int DEFAULT_ASYNC_RETRY_TIMES = 0;
    /** 默认最大重试次数 */
    public static final int DEFAULT_MAX_RECONSUME_TIMES = 16;
    /** 默认消费超时（毫秒） */
    public static final long DEFAULT_CONSUME_TIMEOUT_MS = 30000L;
    /** 默认回查间隔（毫秒） */
    public static final long DEFAULT_CHECK_INTERVAL_MS = 60_000L;
    /** 默认最大回查次数 */
    public static final int DEFAULT_MAX_CHECK_TIMES = 15;
    /** 默认扫描批量 */
    public static final int DEFAULT_BATCH_SIZE = 100;
    /** 默认消费拉取批量 */
    public static final int DEFAULT_CONSUME_BATCH_SIZE = 32;
    /** 默认最大消费线程数 */
    public static final int DEFAULT_CONSUME_THREAD_MAX = 64;
    /** 默认顺序消费分片数 */
    public static final int DEFAULT_SHARD_COUNT = 4;
    /** 默认虚拟节点数（一致性哈希） */
    public static final int DEFAULT_VIRTUAL_NODES = 160;
    /** 默认扫描间隔（毫秒） */
    public static final long DEFAULT_SCAN_INTERVAL_MS = 1000L;
    /** 关闭线程池等待超时（秒） */
    public static final long DEFAULT_AWAIT_TERMINATION_SECONDS = 5L;
    /** 暂停休眠间隔（毫秒） */
    public static final long DEFAULT_PAUSED_SLEEP_MS = 100L;
    /** Broker 异常退避间隔（毫秒） */
    public static final long DEFAULT_BROKER_ERROR_BACKOFF_MS = 500L;
    /** 消费者最大批量大小上界 */
    public static final int MAX_BATCH_SIZE_LIMIT = 1000;
    /** 默认拉取阻塞超时（毫秒） */
    public static final long DEFAULT_PULL_BLOCK_TIMEOUT_MS = 1000L;
    /** 默认拉取间隔（毫秒） */
    public static final long DEFAULT_PULL_INTERVAL_MS = 0L;
    /** 默认顺序消费挂起时长（毫秒） */
    public static final long DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS = 1000L;
    /** 默认 Stream 最大长度（0=不限制） */
    public static final int DEFAULT_STREAM_MAX_LEN = 0;

    // ==================== 属性 Key 常量 ====================
    /** 属性 key：topic */
    public static final String PROP_TOPIC = "topic";
    /** 属性 key：consumer group */
    public static final String PROP_CONSUMER_GROUP = "consumer-group";
    /** 属性 key：consumer name */
    public static final String PROP_CONSUMER_NAME = "consumer-name";
    /** 属性 key：namespace */
    public static final String PROP_NAMESPACE = "namespace";
    /** 属性 key：group（producer） */
    public static final String PROP_GROUP = "group";
    /** 属性 key：send message timeout */
    public static final String PROP_SEND_MESSAGE_TIMEOUT = "send-message-timeout";
    /** 属性 key：stream max len */
    public static final String PROP_STREAM_MAX_LEN = "stream.max-len";

    // ==================== 线程名前缀 ====================
    public static final String THREAD_PREFIX = "streammq";
    public static final String THREAD_RETRY_SCHEDULER = "streammq-retry-scheduler";
    public static final String THREAD_TXCHECK_SCHEDULER = "streammq-txcheck-scheduler";
    public static final String THREAD_DELAY_SCHEDULER = "streammq-delay-scheduler";

    // ==================== Bean 名前缀 ====================
    public static final String BEAN_PRODUCER_PREFIX = "streamMqTemplate-";

    // ==================== 默认组名 ====================
    public static final String DEFAULT_PRODUCER_GROUP = "default-producer";
    public static final String DEFAULT_TX_GROUP = "default-tx-group";

    // ==================== 启用模式 ====================
    public static final String MODE_STANDARD = "STANDARD";
    public static final String MODE_LITE = "LITE";
}
