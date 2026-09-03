/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core;

/** StreamMQ 全局常量定义。 集中管理跨模块共享的默认值、属性 key、Bean 名前缀、线程名前缀等常量。 */
public final class StreamMQConstants {

    private StreamMQConstants() {}

    // ==================== 注解属性"未设置"哨兵 ====================
    /**
     * {@code @StreamMQConsumer} 数值属性的"未设置"哨兵（int 型）：表示回落到全局配置。
     *
     * <p><b>统一口径（配置值 → 默认值 → 实际值 三方对等）：</b>
     *
     * <pre>
     *   注解属性 = UNSET        → 取 streammq.* 全局配置
     *   全局配置 = 常量默认值   → 取 StreamMQConstants.DEFAULT_*
     *   注解属性 != UNSET       → 注解优先（用户显式声明最高优先级）
     * </pre>
     *
     * <p>使用哨兵而非"与常量默认值比较"的原因：后者会产生哨兵碰撞—— 当注解显式写上与常量默认值相同的数值时 （例如 {@code pullBatchSize = 32} 恰等于
     * {@link #DEFAULT_CONSUME_BATCH_SIZE}）， 框架无法区分"用户显式指定"与"使用注解默认值"，
     * 会被全局配置静默覆盖，即典型的配置失效。独立哨兵值彻底消除该歧义。
     */
    public static final int ANNOTATION_UNSET_INT = -1;

    /** {@code @StreamMQConsumer} 数值属性的"未设置"哨兵（long 型），语义同 {@link #ANNOTATION_UNSET_INT}。 */
    public static final long ANNOTATION_UNSET_LONG = -1L;

    // ==================== 默认值常量 ====================
    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_SEND_TIMEOUT_MS = 3000L;

    /** 默认同步重试次数 */
    public static final int DEFAULT_SYNC_RETRY_TIMES = 2;

    /** 同步重试次数上限。超过此值的配置将被夹取为该值，防止误配置（如 {@code Integer.MAX_VALUE}）导致无限重试、 阻塞业务线程数十分钟。 */
    public static final int MAX_SYNC_RETRY_TIMES = 16;

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

    /** 默认 retry Stream 最大长度（0=不限制） */
    public static final int DEFAULT_RETRY_STREAM_MAX_LEN = 0;

    /** 默认 DLQ Stream 最大长度（0=不限制，对齐 retry Stream 的默认姿态） */
    public static final int DEFAULT_DLQ_STREAM_MAX_LEN = 0;

    /**
     * PEL 认领空闲阈值默认值（毫秒）：60 秒。
     *
     * <p><b>下界约束（安全不变量）：</b>必须显著大于「消费超时（默认 30s）+ 消费超时取消宽限期（默认 2s）」，
     * 否则调度器会把<b>仍在正常处理中</b>的消息判定为"孤儿"并复制重投，造成重复消费与顺序破坏。 配置校验见 {@code
     * StreamMQProperties#validate}：低于 {@link #MIN_PEL_CLAIM_MIN_IDLE_MS} 时启动失败。
     */
    public static final long DEFAULT_PEL_CLAIM_MIN_IDLE_MS = 60_000L;

    /**
     * PEL 认领空闲阈值的硬性下界（毫秒）：{@code 消费超时默认 30s + 取消宽限期默认 2s + 3s 安全余量 = 35s}。
     *
     * <p>低于该值意味着"消息可能还在处理就被认领重投"，是数据正确性风险而非性能取舍， 因此不允许通过配置突破。
     */
    public static final long MIN_PEL_CLAIM_MIN_IDLE_MS = 35_000L;

    /** 默认 PEL 认领扫描间隔（毫秒） */
    public static final long DEFAULT_PEL_CLAIM_SCAN_INTERVAL_MS = 5_000L;

    /** 默认背压队列容量（0=不启用背压） */
    public static final int DEFAULT_INFLIGHT_CAPACITY = 0;

    /** 默认单次事务回查超时（毫秒） */
    public static final long DEFAULT_CHECK_TIMEOUT_MS = 60_000L;

    // ==================== 延时消息边界 ====================
    /**
     * 单条延时消息允许的最大延时时长（毫秒）：7 天。
     *
     * <p>与 {@code RedissonStreamProducer#DELAY_PAYLOAD_TTL} 的来源常量 {@link
     * #DEFAULT_DELAY_PAYLOAD_TTL_MS} 强绑定——延时 payload 是 Redis Hash + TTL， 一旦延时时长超过 TTL，payload
     * 会先于投递过期， 调度器只能把该条目移入隔离区（消息事实丢失，仅留痕）。因此这里把"允许配置的最大延时" 显式上界化，在<b>发送侧</b>快速失败，而不是等到投递时才发现消息已经消失。
     */
    public static final long MAX_DELAY_TIME_MILLIS = 7L * 24 * 60 * 60 * 1000;

    /**
     * 延时消息 payload Hash 的保留时长（毫秒）：7 天。
     *
     * <p>正常流程中 payload 在转投成功后即被 DEL；TTL 仅用于兜底清理异常场景残留的孤儿 payload。 该值同时定义了 {@link
     * #MAX_DELAY_TIME_MILLIS} 的上界，二者必须一致。
     */
    public static final long DEFAULT_DELAY_PAYLOAD_TTL_MS = MAX_DELAY_TIME_MILLIS;

    // ==================== 新消费者组起始位点 ====================
    /**
     * 新消费者组的默认起始消费位点：{@link io.github.streammq.core.enums.ConsumeFromWhere#CONSUME_FROM_LAST}。
     *
     * <p>常量与枚举 {@link io.github.streammq.core.enums.ConsumeFromWhere#DEFAULT}
     * 保持单一来源——配置默认值、注解默认值、代码回退值三处一律引用本常量，避免漂移。
     */
    public static final io.github.streammq.core.enums.ConsumeFromWhere DEFAULT_CONSUME_FROM_WHERE =
            io.github.streammq.core.enums.ConsumeFromWhere.DEFAULT;

    /** 默认心跳存活窗口（毫秒），超过该时间无心跳视为不活跃 */
    public static final long DEFAULT_HEARTBEAT_ALIVE_WINDOW_MS = 30_000L;

    /** 默认心跳上报间隔（毫秒） */
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000L;

    /** 默认消费者实例超时时间（毫秒） */
    public static final long DEFAULT_INSTANCE_TIMEOUT_MS = 20_000L;

    /** 调度失败重新入队退避间隔（毫秒） */
    public static final long DEFAULT_FAILURE_REQUEUE_BACKOFF_MS = 5_000L;

    /** 消费超时取消后的宽限期（毫秒） */
    public static final long DEFAULT_TIMEOUT_CANCEL_GRACE_MS = 2_000L;

    /** 默认固定间隔重试策略的间隔（毫秒） */
    public static final long DEFAULT_RETRY_INTERVAL_MS = 10_000L;

    /** 默认消费线程下限 */
    public static final int DEFAULT_CONSUME_THREAD_MIN = 1;

    /** 实例标识系统属性名，用户可通过 -Dstreammq.instance.id=xxx 指定 */
    public static final String INSTANCE_ID_SYSTEM_PROPERTY = "streammq.instance.id";

    /** 实例标识环境变量名，用户可通过 STREAMMQ_INSTANCE_ID=xxx 指定 */
    public static final String INSTANCE_ID_ENV_VARIABLE = "STREAMMQ_INSTANCE_ID";

    /** 默认单次追踪读取最大条数（Trace 扫描批量上限，默认 10000） */
    public static final int DEFAULT_TRACE_MAX_READ_COUNT = 10_000;

    /** DLQ 重试最小延迟下限（毫秒） */
    public static final long MIN_DLQ_RETRY_DELAY_MS = 1_000L;

    /** 诊断默认统计窗口（毫秒，5 分钟） */
    public static final long DEFAULT_DIAGNOSTIC_WINDOW_MS = 5 * 60 * 1000L;

    // ==================== 默认序列化器 ====================
    /**
     * 默认消息体序列化器实现类全限定名：Apache Fury（{@code
     * io.github.streammq.adapter.redisson.serializer.FurySerializer}）。
     *
     * <p>以字符串形式定义，避免 core 模块反向依赖 redisson 适配器；Spring Boot Starter 按此默认值装配 {@code
     * streammq.producer.serializer}。
     */
    public static final String DEFAULT_SERIALIZER =
            "io.github.streammq.adapter.redisson.serializer.FurySerializer";

    /** 默认序列化器名称（对应 {@code MessageSerializer#name()}），用于日志与监控标识 */
    public static final String DEFAULT_SERIALIZER_NAME = "fury";

    // ==================== 消息大小限制 ====================
    /** Redis Stream 单条消息最大大小（字节），512MB。 实际建议不超过 1MB，超大消息会增加网络传输和内存压力。 */
    public static final long MAX_MESSAGE_SIZE_BYTES = 512L * 1024 * 1024;

    /** 推荐的消息体最大大小（字节），超过此值建议使用压缩或分片。 */
    public static final long RECOMMENDED_MAX_BODY_SIZE_BYTES = 1024L * 1024;

    /** 默认消息体压缩阈值（字节），0 = 禁用压缩 */
    public static final int DEFAULT_COMPRESS_THRESHOLD_BYTES = 0;

    // ==================== DLQ 配置默认值 ====================
    /** 默认 DLQ 失败策略实现类全限定名（LogAndDropDlqFailureStrategy） */
    public static final String DEFAULT_DLQ_FAILURE_STRATEGY =
            "io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy";

    /** DLQ 消费失败后的最大重试次数 */
    public static final int DEFAULT_DLQ_MAX_RETRY_ATTEMPTS = 3;

    /** DLQ 消费重试延迟（毫秒） */
    public static final long DEFAULT_DLQ_RETRY_DELAY_MS = 10_000L;

    /** 是否启用二级死信队列 */
    public static final boolean DEFAULT_SECONDARY_DLQ_ENABLED = false;

    /** 二级死信 Stream Key 前缀段 */
    public static final String DEFAULT_SECONDARY_DLQ_KEY_PREFIX = "dlq2";

    /** DLQ 告警阈值 */
    public static final int DEFAULT_DLQ_ALERT_THRESHOLD = 1;

    /** DLQ 重试退避倍数 */
    public static final double DEFAULT_DLQ_RETRY_BACKOFF_MULTIPLIER = 1.0;

    /** DLQ 重试延迟上限（毫秒） */
    public static final long DEFAULT_DLQ_RETRY_MAX_DELAY_MS = 300_000L;

    /** DLQ 消息重试计数字段名（Stream Entry fields） */
    public static final String FIELD_DLQ_RETRY_COUNT = "__dlqRetryCount";

    /** DEFER 调度标记字段名（payload Hash）：标记本轮调度来自业务 DEFER 而非消费失败重试 */
    public static final String FIELD_DEFERRED = "__deferred";

    /** 转移任务执行权锁默认 TTL（毫秒）：持有者崩溃后其它实例可在 TTL 过期后接管 */
    public static final long DEFAULT_TRANSFER_CLAIM_TTL_MS = 30_000L;

    /** 顺序消费分片锁默认获取等待上限（毫秒）：超时未获得则转 RECONSUME_LATER，防止挂死的持有者造成分片永久停摆 */
    public static final long DEFAULT_ORDERLY_LOCK_ACQUIRE_TIMEOUT_MS = 5_000L;

    /** 内部保留属性前缀：解码时捕获到用户属性、编码时随 props JSON 往返的 SDK 元数据均以此开头 */
    public static final String RESERVED_PROPERTY_PREFIX = "__";

    /** DLQ 重试目标 topic 哨兵值（RetryScheduler 检测到此值时 XADD 到 dlqStream 而非 retryStream） */
    public static final String DLQ_RETRY_TARGET_TOPIC_SENTINEL = "__dlq__";

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
    public static final String THREAD_PELCLAIM_SCHEDULER = "streammq-pelclaim-scheduler";
    public static final String THREAD_HEARTBEAT_PREFIX = "streammq-hb-";
    public static final String THREAD_PROCESS_PREFIX = "streammq-process-";

    // ==================== Bean 名前缀 ====================
    public static final String BEAN_PRODUCER_PREFIX = "streamMQTemplate-";

    // ==================== 默认组名 ====================
    public static final String DEFAULT_PRODUCER_GROUP = "default-producer";
    public static final String DEFAULT_TX_GROUP = "default-tx-group";

    // ==================== 启用模式 ====================
    public static final String MODE_STANDARD = "STANDARD";
    public static final String MODE_LITE = "LITE";

    // ==================== Redis Key / Field 常量 ====================
    /** 健康检查 Redis Key */
    public static final String HEALTH_CHECK_KEY = "streammq:health-check";

    /** 事务状态 Hash 中目标 Topic 字段后缀 */
    public static final String TX_FIELD_TARGET_SUFFIX = ".target";

    /** 事务状态 Hash 中半消息 Stream Entry ID 字段后缀 */
    public static final String TX_FIELD_HALF_ID_SUFFIX = ".halfId";

    /** 事务状态 Hash 中终态时间戳字段后缀（值 = 终态写入时的 epoch 毫秒，供保留期清理扫描） */
    public static final String TX_FIELD_DONE_SUFFIX = ".done";

    // ==================== 消息字段 / 协议常量 ====================
    /** Stream Entry 字段：原始消息 ID（DLQ / 重试场景） */
    public static final String FIELD_ORIGINAL_MESSAGE_ID = "originalMessageId";

    /** 延时消息 Hash 载荷字段：目标 topic */
    public static final String FIELD_TARGET_TOPIC = "targetTopic";

    /** 延时消息 Hash 载荷字段：投递时间 */
    public static final String FIELD_DELIVER_AT = "deliverAt";

    /** DLQ 条目元数据字段：死信原因 */
    public static final String FIELD_DLQ_REASON = "dlqReason";

    /** 选择器通配符表达式（订阅全部消息） */
    public static final String SELECTOR_WILDCARD = "*";

    /** 广播消费模式下的生效分组分隔符（group:consumerName） */
    public static final String BROADCAST_GROUP_SEPARATOR = ":";

    /** 一致性哈希虚拟节点名称分隔符 */
    public static final String VIRTUAL_NODE_SEPARATOR = "#";

    // ==================== 追踪属性 Key（Trace 属性契约） ====================
    /** 追踪属性：traceId */
    public static final String TRACE_ATTR_TRACE_ID = "traceId";

    /** 追踪属性：errorMessage */
    public static final String TRACE_ATTR_ERROR_MESSAGE = "errorMessage";

    /** 追踪属性：regionId */
    public static final String TRACE_ATTR_REGION_ID = "regionId";

    /** 追踪属性：keys */
    public static final String TRACE_ATTR_KEYS = "keys";

    /** 追踪属性：tag */
    public static final String TRACE_ATTR_TAG = "tag";

    /** 追踪属性：consumerName */
    public static final String TRACE_ATTR_CONSUMER_NAME = "consumerName";

    /** 追踪属性：reconsumeTimes */
    public static final String TRACE_ATTR_RECONSUME_TIMES = "reconsumeTimes";

    /** 追踪属性：action */
    public static final String TRACE_ATTR_ACTION = "action";

    /** 追踪属性：delayLevel */
    public static final String TRACE_ATTR_DELAY_LEVEL = "delayLevel";

    /** 追踪属性：bodyType */
    public static final String TRACE_ATTR_BODY_TYPE = "bodyType";

    /** 追踪属性：bornHost */
    public static final String TRACE_ATTR_BORN_HOST = "bornHost";
}
