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

    /**
     * 默认并发消费超时（毫秒）：{@code 0} = 默认<b>不启用</b>每条消息的超时包装。
     *
     * <p><b>为什么默认是 0（0.1.2 起的变更）：</b>启用超时后，框架必须为<b>每一条</b>消息执行一次 {@code executor.submit()} + {@code
     * Future.get(timeout)}，并在超时后 {@code join} 等待业务线程—— 即每条消息额外承担一次虚拟线程创建、一个 Future、一次 park/unpark
     * 交接， 而 99.99% 的消息会在毫秒级内完成，这笔开销纯属浪费。 因此把"超时保护"改为显式 opt-in：只有确实存在慢/卡死 handler 的消费者才为它付费。
     *
     * <p><b>关闭后消息会不会卡死？不会。</b>未 ACK 的消息始终留在 PEL 中，由 {@code PelClaimScheduler} 在空闲阈值（{@link
     * #DEFAULT_PEL_CLAIM_MIN_IDLE_MS}，默认 60s）后认领重投， at-least-once 语义不变；代价只是"卡死消息"的恢复延迟从 30s 变为
     * 60s+。
     *
     * <p><b>需要更快的恢复时：</b>设置 {@code streammq.consumer.consume-timeout-millis} 或
     * {@code @StreamMQConsumer#consumeTimeout()} 为一个正毫秒值。
     */
    public static final long DEFAULT_CONSUME_TIMEOUT_MS = 0L;

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

    /** 广播组僵尸回收扫描间隔（毫秒） */
    public static final long DEFAULT_BROADCAST_SWEEP_INTERVAL_MS = 30_000L;

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
     * 单条延时消息允许的最大延时时长（毫秒）：7 天（产品边界）。
     *
     * <p>发送侧对超过该值的延时快速失败。payload 的 TTL 不再与最大延时时长绑定相等—— 实际 TTL = 投递时刻 + {@link
     * #DEFAULT_DELAY_PAYLOAD_TTL_GRACE_MS} 宽限， 因此在该边界内 payload 永远不会先于投递过期。
     */
    public static final long MAX_DELAY_TIME_MILLIS = 7L * 24 * 60 * 60 * 1000;

    /**
     * 延时消息 payload Hash 的基准保留时长（毫秒）：7 天。
     *
     * <p>正常流程中 payload 在转投成功后即被 DEL；TTL 仅用于兜底清理异常场景残留的孤儿 payload。 发送侧实际写入的 TTL = 延时时长 + {@link
     * #DEFAULT_DELAY_PAYLOAD_TTL_GRACE_MS}， 保证 payload 覆盖到投递之后（见该常量说明）。
     */
    public static final long DEFAULT_DELAY_PAYLOAD_TTL_MS = MAX_DELAY_TIME_MILLIS;

    /**
     * 延时 payload TTL 的「投递后宽限」（毫秒）：1 小时。
     *
     * <p>TTL 在投递时刻之上再叠加该宽限，覆盖调度扫描间隔、转投耗时与节点间时钟偏差， 确保到期消息的 payload 绝不会先于投递过期（历史上 TTL 与 deliverAt
     * 重合时， 到期边界存在 payload 先过期、消息被隔离而事实丢失的竞争窗口）。
     */
    public static final long DEFAULT_DELAY_PAYLOAD_TTL_GRACE_MS = 3_600_000L;

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

    /**
     * 默认广播实例租约超时（毫秒）：空闲超过该时长的广播实例槽位进入"可回收"窗口。
     *
     * <p>与集群消费的实例超时同值，语义一致：超过该时长无心跳即认为实例已停止。
     */
    public static final long DEFAULT_BROADCAST_LEASE_TIMEOUT_MS = 20_000L;

    /**
     * 默认广播实例回收宽限期（毫秒，自最后心跳起算）。
     *
     * <p>该窗口是"重启后能否保住 PEL"的关键：空闲超过租约超时、但未超过宽限期时， 槽位<b>仅允许相同 host 的实例回收</b>，其 Redis 消费者组与 PEL 完整保留。
     *
     * <p>默认 7 天：足以覆盖滚动发布、节点驱逐、周末停机等常规运维窗口； 超过后才由清扫任务 {@code XGROUP DESTROY} 释放内存。
     */
    public static final long DEFAULT_BROADCAST_RECLAIM_GRACE_MS = 7L * 24 * 60 * 60 * 1000;

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
     * 默认消息体序列化器实现类全限定名：{@code JacksonJsonSerializer}（基于 Jackson 的 JSON 序列化，跨语言/可读性优先、严格类型、无多态反序列化）。
     *
     * <p><b>为什么默认是 Jackson 而不是 Apache Fory（原 Apache Fury，0.1.2 起的变更）：</b>0.1.1 曾把默认设为 Apache Fory（原
     * Fury）的宽松模式（{@code requireClassRegistration=false}）。Fory 吞吐确实约为 Jackson 的 7~13 倍， 但宽松模式下 Redis
     * 中的字节流可被反序列化为 classpath 上的任意类——在<b>共享/多租户 Redis</b> 上是反序列化 RCE 攻击面，而这个风险是通过本 SDK
     * <b>传播给所有下游应用</b>的。安全默认值不应该依赖用户先读完 README 的警告段， 因此默认值回退为 {@code
     * JacksonJsonSerializer}：严格类型、无多态反序列化、无 gadget 面， 且消息体在 Redis 中是人类可读的 JSON（便于排障与跨语言消费）。
     *
     * <p><b>需要更高吞吐时：</b>显式配置 {@code streammq.producer.serializer} 为 {@code
     * io.github.streammq.adapter.redisson.serializer.FurySerializer}（类名保持 {@code FurySerializer}
     * 不变以兼容已发布配置，底层为 Apache Fory，原名 Fury；坐标 {@code org.apache.fory:fory-core >= 1.1.0}，即
     * CVE-2026-50076 的修复版）， 并建议同时开启 {@code streammq.producer.fury-require-class-registration=true}
     * 与预注册业务类型； 宽松模式（{@code requireClassRegistration=false}）受系统属性门禁保护，仅在显式放开后可用。 或改用 {@code
     * ProtostuffSerializer}。二者在 {@code streammq-redisson} 中为 optional 依赖，使用前需自行加入 classpath。
     *
     * <p>以字符串形式定义此默认值，避免 core 模块反向依赖 redisson 适配器； Spring Boot Starter 按此默认值装配 {@code
     * streammq.producer.serializer}。
     */
    public static final String DEFAULT_SERIALIZER =
            "io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer";

    /** 默认序列化器名称（对应 {@code MessageSerializer#name()}），用于日志与监控标识 */
    public static final String DEFAULT_SERIALIZER_NAME = "jackson-json";

    // ==================== 消息大小限制 ====================
    /** Redis Stream 单条消息最大大小（字节），512MB。 实际建议不超过 1MB，超大消息会增加网络传输和内存压力。 */
    public static final long MAX_MESSAGE_SIZE_BYTES = 512L * 1024 * 1024;

    /** 推荐的消息体最大大小（字节），超过此值建议使用压缩或分片。 */
    public static final long RECOMMENDED_MAX_BODY_SIZE_BYTES = 1024L * 1024;

    /** 默认消息体压缩阈值（字节），0 = 禁用压缩 */
    public static final int DEFAULT_COMPRESS_THRESHOLD_BYTES = 0;

    // ==================== DLQ 配置默认值 ====================
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

    /** 顺序消费分片锁单轮获取等待上限（毫秒）：单轮超时后进入下一轮，防止挂死的持有者造成分片永久停摆 */
    public static final long DEFAULT_ORDERLY_LOCK_ACQUIRE_TIMEOUT_MS = 5_000L;

    /**
     * 顺序消费分片锁竞争的默认等待轮数：每轮各等待一次 {@link #DEFAULT_ORDERLY_LOCK_ACQUIRE_TIMEOUT_MS}，轮间休眠一次 {@link
     * #DEFAULT_ORDERLY_LOCK_WAIT_INTERVAL_MS}；全部轮次仍拿不到锁时抛 {@code
     * OrderlyShardBusyException}（不消耗业务重试预算、不进 DLQ）。
     */
    public static final int DEFAULT_ORDERLY_LOCK_WAIT_ROUNDS = 3;

    /** 顺序消费分片锁竞争的轮间等待间隔（毫秒） */
    public static final long DEFAULT_ORDERLY_LOCK_WAIT_INTERVAL_MS = 200L;

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
    public static final String THREAD_BROADCAST_SWEEP_SCHEDULER =
            "streammq-broadcast-sweep-scheduler";
    public static final String THREAD_BROADCAST_LEASE_HEARTBEAT =
            "streammq-broadcast-lease-heartbeat";
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

    /** 事务状态 Hash 中「强制终结原因」字段后缀（有界重试耗尽后写入，供运维定位卡死事务） */
    public static final String TX_FIELD_FAILURE_REASON_SUFFIX = ".failureReason";

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
