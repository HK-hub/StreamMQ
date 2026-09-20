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
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.exception.StreamMQClientException;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 *     fury-require-class-registration: false
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
@Getter
@Setter
public class StreamMQProperties {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQProperties.class);

    /**
     * 顺序消费分片锁租约的建议下限（毫秒）：低于该值会在启动时 WARN。
     *
     * <p>正常慢 handler 的耗时量级之上才不会被误判为卡死；与 {@code
     * io.github.streammq.adapter.redisson.container.DefaultConsumerTuning} 的推荐值保持一致。
     */
    public static final long MIN_ORDERLY_SHARD_LOCK_LEASE_MILLIS = 5_000L;

    /** 是否启用 StreamMQ 自动装配，默认 true */
    private boolean enabled = true;

    /** 命名空间（用于多租户/多环境隔离），默认空字符串 */
    private String namespace = "";

    /**
     * 实例唯一标识（广播消费模式下用于构造持久化消费者组名）。
     *
     * <p>若显式配置，则使用该值；否则按以下优先级自动推导：
     *
     * <ol>
     *   <li>系统属性 {@code streammq.instance.id}
     *   <li>环境变量 {@code STREAMMQ_INSTANCE_ID}
     *   <li>本地主机名（{@code InetAddress.getLocalHost().getHostName()}）
     *   <li>UUID 回退
     * </ol>
     *
     * <p>持久化标识保证容器重启后广播消费者组名不变，避免每次重启产生新组导致 PEL 内存泄漏。
     */
    private String instanceId = "";

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
    @Getter
    @Setter
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

        /**
         * 消息体序列化器实现类（填写全限定类名），默认 {@link JacksonJsonSerializer}。
         *
         * <p><b>为什么默认 Jackson（0.1.2 起）：</b>0.1.1 曾默认 Apache Fury 宽松模式。Fury 吞吐更高（约为 Jackson 的 7~13
         * 倍），但宽松模式下 Redis 中的字节流可反序列化为 classpath 上的任意类—— 在<b>共享/多租户 Redis</b> 上是反序列化 RCE
         * 攻击面，并且这个风险会通过本 SDK <b>传播给所有下游应用</b>。 安全默认值不应依赖用户先读完 README 的警告段，故默认值回退为 Jackson：
         * 严格类型、无多态反序列化、无 gadget 面，且消息体在 Redis 中是人类可读 JSON（便于排障与跨语言消费）。
         *
         * <p><b>需要更高吞吐时显式 opt-in：</b>{@code
         * io.github.streammq.adapter.redisson.serializer.FurySerializer}（建议同时开启 {@code
         * fury-require-class-registration=true} 并预注册业务类型）或 {@code
         * io.github.streammq.adapter.redisson.serializer.ProtostuffSerializer}。 二者在 {@code
         * streammq-redisson} 中为 <b>optional</b> 依赖，需自行加入 classpath：Fory 需 {@code
         * org.apache.fory:fory-core}（>= 1.1.0）；Protostuff 需 {@code io.protostuff:protostuff-core} 与
         * {@code protostuff-runtime}。
         *
         * <p>对应全局默认值常量： {@link StreamMQConstants#DEFAULT_SERIALIZER}。
         */
        private Class<? extends MessageSerializer> serializer = JacksonJsonSerializer.class;

        /**
         * Fury 是否强制类注册白名单（仅当 {@code producer.serializer} 为 {@code FurySerializer} 时生效）。
         *
         * <p>默认 {@code true}（强制类注册白名单）：仅允许显式注册过的类型反序列化，未注册的业务 POJO 会被拒绝，需通过 {@code new
         * FurySerializer<>(Xxx.class)} 或 {@code register(Class)} 预注册；设为 {@code false} 即宽松模式（任意 POJO
         * 可反序列化， Redis 中字节流可解析为 classpath 上任意类，在<b>共享/多租户 Redis</b>上是反序列化 RCE 攻击面）。
         *
         * <p><b>缓解：</b>显式选用 Fury 时建议设为 {@code true} 开启类注册白名单，并通过 {@code new
         * FurySerializer<>(Xxx.class)} 或 {@code register(Class)} / {@code registerAll(Class...)}
         * 预注册业务消息体类型； 若不能接受任何 RCE 面，保持默认的 {@code JacksonJsonSerializer} 或改用 {@code
         * ProtostuffSerializer}。
         */
        private boolean furyRequireClassRegistration = true;

        /**
         * Fury 白名单模式下预注册的业务消息体类型（全限定类名列表）。
         *
         * <p>仅当 {@code producer.serializer} 为 {@code FurySerializer} 且 {@code
         * fury-require-class-registration=true}（默认）时生效：此列表中的类会被注册，<b>其它类型的 body
         * 反序列化将被拒绝</b>。默认空列表——此时业务 POJO 会反序列化失败（启动/反序列化日志会给出提示），因此 opt-in Fury 时请一并声明。
         *
         * <p>示例：{@code streammq.producer.fury-registered-classes=com.acme.Order,com.acme.Payment}
         */
        private java.util.List<Class<?>> furyRegisteredClasses = java.util.List.of();

        /**
         * 消息体压缩阈值（字节），body 超过此值时触发压缩，0 = 禁用（默认禁用）。
         *
         * <p>与 {@link #compressionCodec} 配合：{@code > 0} 时必须能确定唯一的默认压缩 Codec（见下）。
         */
        private int compressThreshold = StreamMQConstants.DEFAULT_COMPRESS_THRESHOLD_BYTES;

        /**
         * 默认压缩 Codec 名称（可选，默认空 = 自动解析）。
         *
         * <p>仅当注册了<b>多个</b> {@link io.github.streammq.core.compression.CompressionCodec} Bean 时才有必要
         * 显式指定；解析顺序为：本键精确匹配（显式配置优先）→ {@code @Primary} → 唯一候选。多候选且无法消歧时： {@code compress-threshold >
         * 0}（压缩已启用）会启动失败并列出候选（不静默取任意一个）；{@code compress-threshold = 0} 则记录 WARN 并保持"无默认
         * Codec"（按名解压仍由注册表负责）。
         *
         * <p>取值可以是任意候选 Bean 的 {@code name()}（如 {@code zstd}），也可以是注册表内置名称 {@code gzip} / classpath
         * 存在 lz4-java 时的 {@code lz4}。写成不存在的名称会导致启动失败（并列出全部可选名称）。
         */
        private String compressionCodec = "";

        /** 单条消息最大大小（字节），发送时校验。 默认 512MB（Redis Stream 上限），推荐不超过 1MB。 */
        private long maxMessageSize = StreamMQConstants.MAX_MESSAGE_SIZE_BYTES;
    }

    /** 消费者配置。 */
    @Getter
    @Setter
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

        /**
         * 全局顺序消费超时（毫秒），默认 0（不启用）。
         *
         * <p><b>生效条件（易误判，务必注意）：</b>本键<b>只对「注解 {@code orderlyConsumeTimeout} 显式写 {@code 0}」
         * 的消费者生效</b>。注解默认值是 {@code StreamMQConstants.ANNOTATION_UNSET_LONG}（{@code -1}），语义为
         * 「显式关闭该消费者的顺序消费超时保护」——因此<b>只改全局键而不动注解，对所有消费者都不生效</b>。 三分支：注解 {@code >0} 覆盖全局；注解 {@code =0}
         * 继承本键；注解 {@code <0}（默认）关闭。 需要全局开启时，请同时把目标消费者的注解写成
         * {@code @StreamMQConsumer(orderlyConsumeTimeout = 0)}。
         *
         * <p><b>为什么默认关闭，而非复用 {@code consumeTimeout}：</b>{@code consumeTimeout} 默认 30000
         * 且作用于并发消费——超时只是单条消息重投，不影响其它消息。顺序消费是「分片锁 + 串行重试」： 超时后原地在当前线程重试，每次失败挂起 {@code
         * suspendCurrentQueueTimeMillis}，耗尽 {@code maxReconsumeTimes} 即进 DLQ。 若复用 {@code
         * consumeTimeout} 的非零默认值，等于把所有存量顺序消费者的慢消息系统性送入 DLQ—— 这是破坏性变更。因此独立属性 + 默认关闭，
         * 需要保护分片可用性时在此一次性全局开启，或在具体消费者注解上开启。
         */
        private long orderlyConsumeTimeoutMillis = 0L;

        /**
         * 顺序消费分片锁的租约（毫秒），默认 {@code 0}。
         *
         * <p><b>语义与权衡（§13 顺序锁闭环项，R6-S8）：</b>
         *
         * <ul>
         *   <li>{@code 0}（默认）：Redisson 看门狗持续续期 + <b>严格有序</b>。持有分片锁的实例即使 handler
         *       卡死（不响应中断）也一直持有锁，其它实例无法接管——顺序性最强，但卡死会阻塞该分片直到进程重启；
         *   <li>{@code > 0}：有限租约、不续期。持有者超过租约未完成会<b>被其它实例接管</b>，卡死不再永久阻塞分片，
         *       代价是语义降级为"至多一次重叠执行、可能乱序"（同一分片的消息可能被两个实例短暂并行消费）。
         * </ul>
         *
         * <p><b>取值建议：</b>不小于 5000（正常慢 handler 的完成时间量级之上）；{@code 0 < v < 5000} 会在启动时 记录 WARN（正常慢
         * handler 可能被判为卡死并让位）。负值配置非法，启动失败。
         *
         * <p>本键为"逃生舱"参数：只有在确实存在不可中断的卡死 handler、且业务能接受重叠/乱序时才应开启。
         */
        private long orderlyShardLockLeaseMillis = 0L;

        /**
         * 广播消费实例身份（可选）：显式指定后，广播消费者组名跨重启恒定。
         *
         * <p>留空时按「本地持久文件 → Redis 注册中心回收 → Redis 注册中心分配」自动解析， 重启后仍保持稳定；仅当 Redis 与本地文件同时不可用时才会退化为随机值。
         *
         * <p><b>何时需要显式配置：</b>K8s StatefulSet 有稳定序号（{@code streammq-0}、{@code streammq-1}）时， 直接绑定
         * Pod 名可获得完全确定性的组名，便于运维对账。
         */
        private String broadcastInstanceId = "";

        /**
         * 广播实例身份本地持久化文件路径（可选）。
         *
         * <p>默认按 namespace+group 分片：{@code ${user.home}/.streammq/instance-id-<ns>_<group>}—— 同一 OS
         * 用户下的多个 StreamMQ 应用互不共享身份文件；文件内按 {@code id pid timestamp}
         * 多记录存储，重启复用已退出进程的身份、不覆盖仍在运行进程的身份。显式设置本属性时为 全实例共用的单一文件（跨应用隔离由使用方自行保证）。设为 {@code none} 或
         * {@code false} 可禁用本地文件（只读根文件系统场景，此时完全依赖 Redis 注册中心回收）。
         */
        private String broadcastInstanceIdFile = "";

        /**
         * 广播实例租约超时：空闲超过该时长的身份槽位进入「可被同主机实例回收」窗口。
         *
         * <p>默认 {@link StreamMQConstants#DEFAULT_BROADCAST_LEASE_TIMEOUT_MS}（20s）。
         */
        private Duration broadcastLeaseTimeout =
                Duration.ofMillis(StreamMQConstants.DEFAULT_BROADCAST_LEASE_TIMEOUT_MS);

        /**
         * 广播实例回收宽限期：自最后心跳起算，超过后槽位及其消费者组被销毁。
         *
         * <p>该窗口决定「重启多久后仍能保住 PEL 与消费位点」。 默认 {@link
         * StreamMQConstants#DEFAULT_BROADCAST_RECLAIM_GRACE_MS} （7 天），足以覆盖滚动发布、节点驱逐与常规停机； 调小可更快释放
         * Redis 内存，代价是长停机后广播消费会从当前时间点重新开始。
         */
        private Duration broadcastReclaimGrace =
                Duration.ofMillis(StreamMQConstants.DEFAULT_BROADCAST_RECLAIM_GRACE_MS);

        /**
         * 新消费者组起始消费位点，默认 {@link ConsumeFromWhere#DEFAULT}（= {@code CONSUME_FROM_LAST}）。
         *
         * <p><b>仅在该 Redis 消费者组首次创建时生效</b>；已存在的组不受此值影响。 默认值常量 {@link
         * StreamMQConstants#DEFAULT_CONSUME_FROM_WHERE} 与枚举 {@link ConsumeFromWhere#DEFAULT} 单一来源，
         * 保证「配置默认值 / 注解默认值 / 运行实际值」三方一致。
         *
         * <p>可选值：
         *
         * <ul>
         *   <li>{@code CONSUME_FROM_LAST}：只消费组创建之后写入的消息（安全默认，向长期运行 Topic 追加组不会重放历史）
         *   <li>{@code CONSUME_FROM_FIRST}：重放该 Topic 全部历史消息
         * </ul>
         */
        private ConsumeFromWhere consumeFromWhere = StreamMQConstants.DEFAULT_CONSUME_FROM_WHERE;

        /**
         * 全局并发消费超时（毫秒），默认 {@link StreamMQConstants#DEFAULT_CONSUME_TIMEOUT_MS}（{@code 0} = 不启用）。
         *
         * <p>仅作为 {@code @StreamMQConsumer#consumeTimeout()} 未显式声明（为 -1）时的回落值； 注解显式声明 {@code >= 0}
         * 时始终优先，per-consumer 可覆盖全局。
         *
         * <p><b>性能含义（务必知悉）：</b>设为正数后，框架会为<b>每一条</b>消息执行一次 {@code executor.submit()} + {@code
         * Future.get(timeout)}（+ 超时后的 {@code join} 等待），用于中断卡死的 handler。 这是每条消息的固定成本，而绝大多数消息毫秒级即完成。
         * 因此默认关闭：卡死消息由 {@code PelClaimScheduler} 在空闲阈值（默认 60s）后认领重投兜底，at-least-once 语义不变，仅恢复延迟更长。
         * 只有确实存在慢/卡死 handler 的场景才应开启。
         */
        private long consumeTimeoutMillis = StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;
    }

    /** 消费者组管理配置（心跳与实例存活判定）。 */
    @Getter
    @Setter
    public static class Group {
        /** 心跳上报间隔（毫秒），默认 {@link StreamMQConstants#DEFAULT_HEARTBEAT_INTERVAL_MS} */
        private long heartbeatIntervalMs = StreamMQConstants.DEFAULT_HEARTBEAT_INTERVAL_MS;

        /**
         * 实例超时时间（毫秒），超过该时长无心跳的实例将被移出分配， 默认 {@link StreamMQConstants#DEFAULT_INSTANCE_TIMEOUT_MS}。
         */
        private long instanceTimeoutMs = StreamMQConstants.DEFAULT_INSTANCE_TIMEOUT_MS;
    }

    /** 死信队列配置。 */
    @Getter
    @Setter
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

        /**
         * DLQ Stream 最大长度（0=不限制，默认）。对齐 retry Stream 的默认姿态—— 默认不限制，由 Redis 自身 maxlen 策略兜底；
         * 需要硬上限时显式配置为正值。
         */
        private int streamMaxLen = StreamMQConstants.DEFAULT_DLQ_STREAM_MAX_LEN;
    }

    /** 重试策略配置。 */
    @Getter
    @Setter
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

        /** PEL 认领空闲阈值（顺序消费专用，默认 60s） */
        private long pelClaimMinIdleMs = StreamMQConstants.DEFAULT_PEL_CLAIM_MIN_IDLE_MS;

        /**
         * 转移失败后的回写退避间隔（毫秒）：避免 Redis 故障时以扫描间隔高频热循环重试， 默认 {@link
         * StreamMQConstants#DEFAULT_FAILURE_REQUEUE_BACKOFF_MS}。
         */
        private long failureRequeueBackoffMs = StreamMQConstants.DEFAULT_FAILURE_REQUEUE_BACKOFF_MS;
    }

    /** 延时消息调度器配置。 */
    @Getter
    @Setter
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
    @Getter
    @Setter
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
    @Getter
    @Setter
    public static class Health {
        /** 是否启用健康检查，默认 true（仅在 Actuator 在 classpath 时生效） */
        private boolean enabled = true;
    }

    /** 重平衡策略配置。 */
    @Getter
    @Setter
    public static class Rebalance {
        /** 重平衡策略实现类，默认 {@link ConsistentHashRebalanceStrategy} */
        private Class<? extends RebalanceStrategy> strategy = ConsistentHashRebalanceStrategy.class;

        /**
         * 虚拟节点数（仅一致性哈希策略生效），必须 {@code > 0}，默认 {@link StreamMQConstants#DEFAULT_VIRTUAL_NODES}。
         *
         * <p>{@code <= 0} 属于非法值：容器会把非法值静默回退到默认值 160，用户看不出配置未生效。此处改为启动即拒绝。
         */
        private int virtualNodes = StreamMQConstants.DEFAULT_VIRTUAL_NODES;
    }

    /** 追踪配置。 */
    @Getter
    @Setter
    public static class Tracing {
        /** 是否启用追踪，对应 {@code streammq.tracing.enabled}（默认 false） */
        private boolean enabled = false;
    }

    /**
     * 追踪存储与查询配置（v1.0+）。
     *
     * <p>与 {@link Tracing} 区别：Tracing 控制日志级别的追踪输出， Trace 控制追踪数据的持久化存储与查询能力。
     */
    @Getter
    @Setter
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
    @Getter
    @Setter
    public static class Admin {
        /** 管理端点开关：与 streammq.health.enabled 解耦，false 时仅关闭管理/运维 REST 端点（健康检查不受影响） */
        private boolean enabled = true;

        /**
         * 管理端点列表默认页大小，签发范围 {@code [1, 10000]}（上界见 {@link
         * StreamMQSpringConstants#MAX_ADMIN_LIST_LIMIT}）。
         */
        private int listPageSize = StreamMQSpringConstants.DEFAULT_LIST_PAGE_SIZE;

        /**
         * pending 列表单次最大拉取条数，签发范围 {@code [1, 10000]}（上界见 {@link
         * StreamMQSpringConstants#MAX_ADMIN_LIST_LIMIT}）。
         */
        private int maxPendingQuerySize = StreamMQSpringConstants.MAX_PENDING_QUERY_SIZE;

        /** 写操作（重投/删除/ACK/重平衡/建删 Topic/改配置）失败后的重试冷却期（毫秒），0 表示禁用 */
        private long failureRetryCooldownMillis =
                io.github.streammq.spring.boot.autoconfigure.FailureRetryLimiter
                        .DEFAULT_COOLDOWN_MILLIS;

        /**
         * 是否信任 {@code X-Forwarded-For} 请求头用于鉴权失败限流的来源聚合。
         *
         * <p><b>安全默认值 {@code false}：</b>XFF 完全由客户端可控，未经校验直接采用会让"失败 N 次锁定"的
         * 限流被一行请求头绕过。仅当管理/诊断端点部署在<b>受控代理</b>之后时才应开启，且必须同时配置 {@link #getTrustedProxies()} 声明可信代理
         * CIDR。
         */
        private boolean trustForwardedHeaders = false;

        /**
         * 可信代理 CIDR 列表（IPv4/IPv6，如 {@code 10.0.0.0/8}、{@code 192.168.1.0/24}）。
         *
         * <p>仅当 {@link #isTrustForwardedHeaders()} 为 true 时生效；留空表示只信任回环地址（{@code 127.0.0.1} /
         * {@code ::1}）。直连对端命中该列表时，才采用 XFF 首值作为真实客户端地址。
         */
        private java.util.List<String> trustedProxies = java.util.List.of();
    }

    /**
     * 校验配置属性的合法性，在自动装配时调用。
     *
     * <p><b>异常类型（0.1.2）：</b>统一抛 {@link StreamMQClientException}（{@link
     * io.github.streammq.core.exception.StreamMQException} 子类）而非 {@link
     * IllegalArgumentException}——配置错误属于框架运行时的"配置错误"口径，业务层可统一 {@code catch (StreamMQException)}
     * 处理；方法参数 / 契约违反才用 JDK 异常。消息文本保持逐项可定位（含完整配置键名）。
     *
     * @throws StreamMQClientException 如果配置值不合法
     */
    public void validate() {
        if (producer.sendMessageTimeout <= 0) {
            throw new StreamMQClientException(
                    "streammq.producer.send-message-timeout must be > 0, got: "
                            + producer.sendMessageTimeout);
        }
        if (producer.retryTimes < 0) {
            throw new StreamMQClientException(
                    "streammq.producer.retry-times must be >= 0, got: " + producer.retryTimes);
        }
        if (producer.streamMaxLen < 0) {
            throw new StreamMQClientException(
                    "streammq.producer.stream-max-len must be >= 0, got: " + producer.streamMaxLen);
        }
        if (producer.maxMessageSize <= 0) {
            throw new StreamMQClientException(
                    "streammq.producer.max-message-size must be > 0, got: "
                            + producer.maxMessageSize);
        }
        if (consumer.batchSize <= 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.batch-size must be > 0, got: " + consumer.batchSize);
        }
        if (consumer.pullInterval < 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.pull-interval must be >= 0, got: " + consumer.pullInterval);
        }
        if (consumer.pausedSleepMillis <= 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.paused-sleep-millis must be > 0, got: "
                            + consumer.pausedSleepMillis);
        }
        if (consumer.brokerErrorBackoffMillis <= 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.broker-error-backoff-millis must be > 0, got: "
                            + consumer.brokerErrorBackoffMillis);
        }
        if (consumer.maxBatchSizeLimit <= 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.max-batch-size-limit must be > 0, got: "
                            + consumer.maxBatchSizeLimit);
        }
        if (consumer.batchSize > consumer.maxBatchSizeLimit) {
            // R6-S7：容器内部会把批量夹取到 max-batch-size-limit。此前只夹取 + 静默，用户配 5000 却实际 1000
            // 完全无感知。这里改为启动即失败（可操作：把 batch-size 调小或把 max-batch-size-limit 调大）。
            throw new StreamMQClientException(
                    "streammq.consumer.batch-size ("
                            + consumer.batchSize
                            + ") must be <= streammq.consumer.max-batch-size-limit ("
                            + consumer.maxBatchSizeLimit
                            + "), otherwise the effective pull batch would be silently clamped."
                            + " Lower batch-size, or raise max-batch-size-limit up to"
                            + " streammq.consumer.max-batch-size-limit's own ceiling.");
        }
        if (transaction.maxCheckTimes <= 0) {
            throw new StreamMQClientException(
                    "streammq.transaction.max-check-times must be > 0, got: "
                            + transaction.maxCheckTimes);
        }
        if (dlq.maxDlqRetryAttempts < 0) {
            throw new StreamMQClientException(
                    "streammq.dlq.max-dlq-retry-attempts must be >= 0, got: "
                            + dlq.maxDlqRetryAttempts);
        }
        if (dlq.dlqRetryDelayMs < 0) {
            throw new StreamMQClientException(
                    "streammq.dlq.dlq-retry-delay-ms must be >= 0, got: " + dlq.dlqRetryDelayMs);
        }
        if (retry.maxReconsumeTimes < 0) {
            throw new StreamMQClientException(
                    "streammq.retry.max-reconsume-times must be >= 0, got: "
                            + retry.maxReconsumeTimes);
        }
        if (retry.batchSize <= 0) {
            throw new StreamMQClientException(
                    "streammq.retry.batch-size must be > 0, got: " + retry.batchSize);
        }
        if (delay.batchSize <= 0) {
            throw new StreamMQClientException(
                    "streammq.delay.batch-size must be > 0, got: " + delay.batchSize);
        }
        if (consumer.timeoutCancelGraceMillis <= 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.timeout-cancel-grace-millis must be > 0, got: "
                            + consumer.timeoutCancelGraceMillis);
        }
        if (group.heartbeatIntervalMs <= 0) {
            throw new StreamMQClientException(
                    "streammq.group.heartbeat-interval-ms must be > 0, got: "
                            + group.heartbeatIntervalMs);
        }
        if (group.instanceTimeoutMs <= 0) {
            throw new StreamMQClientException(
                    "streammq.group.instance-timeout-ms must be > 0, got: "
                            + group.instanceTimeoutMs);
        }
        if (group.instanceTimeoutMs < group.heartbeatIntervalMs) {
            throw new StreamMQClientException(
                    "streammq.group.instance-timeout-ms must be >= heartbeat-interval-ms, got: "
                            + group.instanceTimeoutMs);
        }
        if (retry.failureRequeueBackoffMs <= 0) {
            throw new StreamMQClientException(
                    "streammq.retry.failure-requeue-backoff-ms must be > 0, got: "
                            + retry.failureRequeueBackoffMs);
        }
        if (delay.failureRequeueBackoffMs <= 0) {
            throw new StreamMQClientException(
                    "streammq.delay.failure-requeue-backoff-ms must be > 0, got: "
                            + delay.failureRequeueBackoffMs);
        }
        if (dlq.minRetryDelayMs <= 0) {
            throw new StreamMQClientException(
                    "streammq.dlq.min-retry-delay-ms must be > 0, got: " + dlq.minRetryDelayMs);
        }
        if (trace.maxReadCount <= 0) {
            throw new StreamMQClientException(
                    "streammq.trace.max-read-count must be > 0, got: " + trace.maxReadCount);
        }
        if (admin.listPageSize <= 0) {
            throw new StreamMQClientException(
                    "streammq.admin.list-page-size must be > 0, got: " + admin.listPageSize);
        }
        if (admin.maxPendingQuerySize <= 0) {
            throw new StreamMQClientException(
                    "streammq.admin.max-pending-query-size must be > 0, got: "
                            + admin.maxPendingQuerySize);
        }
        if (admin.listPageSize > StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT) {
            throw new StreamMQClientException(
                    "streammq.admin.list-page-size must be <= "
                            + StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT
                            + " (upper bound protects the admin surface from unbounded responses),"
                            + " got: "
                            + admin.listPageSize);
        }
        if (admin.maxPendingQuerySize > StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT) {
            throw new StreamMQClientException(
                    "streammq.admin.max-pending-query-size must be <= "
                            + StreamMQSpringConstants.MAX_ADMIN_LIST_LIMIT
                            + " (upper bound protects the admin surface from unbounded responses),"
                            + " got: "
                            + admin.maxPendingQuerySize);
        }
        if (admin.failureRetryCooldownMillis < 0) {
            throw new StreamMQClientException(
                    "streammq.admin.failure-retry-cooldown-millis must be >= 0, got: "
                            + admin.failureRetryCooldownMillis);
        }
        if (admin.trustedProxies != null) {
            for (String cidr : admin.trustedProxies) {
                if (!io.github.streammq.core.util.WebRequestAuthSupport.isValidCidr(cidr)) {
                    throw new StreamMQClientException(
                            "streammq.admin.trusted-proxies contains invalid CIDR: '"
                                    + cidr
                                    + "' (expected IPv4/IPv6 CIDR like 10.0.0.0/8 or"
                                    + " 2001:db8::/32)");
                }
            }
        }
        if (consumer.pollTimeout.toMillis() <= 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.poll-timeout must be > 0, got: " + consumer.pollTimeout);
        }
        if (consumer.inflightCapacity < 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.inflight-capacity must be >= 0, got: "
                            + consumer.inflightCapacity);
        }
        if (consumer.orderlyConsumeTimeoutMillis < 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.orderly-consume-timeout-millis must be >= 0, got: "
                            + consumer.orderlyConsumeTimeoutMillis);
        }
        if (consumer.orderlyShardLockLeaseMillis < 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.orderly-shard-lock-lease-millis must be >= 0 (0 = watchdog"
                            + " lease with strict ordering), got: "
                            + consumer.orderlyShardLockLeaseMillis);
        }
        if (consumer.orderlyShardLockLeaseMillis > 0
                && consumer.orderlyShardLockLeaseMillis < MIN_ORDERLY_SHARD_LOCK_LEASE_MILLIS) {
            LOG.warn(
                    "streammq.consumer.orderly-shard-lock-lease-millis={} is below the recommended"
                        + " minimum {}ms: slow handlers may be treated as stuck and another"
                        + " instance may take over the shard, allowing overlapping execution and"
                        + " out-of-order consumption. Set 0 for watchdog + strict ordering, or"
                        + " configure >= {}ms.",
                    consumer.orderlyShardLockLeaseMillis,
                    MIN_ORDERLY_SHARD_LOCK_LEASE_MILLIS,
                    MIN_ORDERLY_SHARD_LOCK_LEASE_MILLIS);
        }
        if (consumer.consumeTimeoutMillis < 0) {
            throw new StreamMQClientException(
                    "streammq.consumer.consume-timeout-millis must be >= 0, got: "
                            + consumer.consumeTimeoutMillis);
        }
        if (consumer.consumeFromWhere == null) {
            throw new StreamMQClientException(
                    "streammq.consumer.consume-from-where must not be null");
        }
        if (rebalance.virtualNodes <= 0) {
            // R6-S6：<=0 此前被容器静默回退到默认 160，用户无从发现配置未生效
            throw new StreamMQClientException(
                    "streammq.rebalance.virtual-nodes must be > 0 (consistent-hash virtual node"
                            + " count; the container silently falls back to "
                            + StreamMQConstants.DEFAULT_VIRTUAL_NODES
                            + " when <= 0), got: "
                            + rebalance.virtualNodes);
        }
        if (dlq.streamMaxLen < 0) {
            throw new StreamMQClientException(
                    "streammq.dlq.stream-max-len must be >= 0, got: " + dlq.streamMaxLen);
        }
        if (retry.scanInterval.toMillis() <= 0) {
            throw new StreamMQClientException(
                    "streammq.retry.scan-interval must be > 0, got: " + retry.scanInterval);
        }
        if (retry.pelClaimScanInterval.toMillis() <= 0) {
            throw new StreamMQClientException(
                    "streammq.retry.pel-claim-scan-interval must be > 0, got: "
                            + retry.pelClaimScanInterval);
        }
        if (retry.pelClaimMinIdleMs <= 0) {
            throw new StreamMQClientException(
                    "streammq.retry.pel-claim-min-idle-ms must be > 0, got: "
                            + retry.pelClaimMinIdleMs);
        }
        if (retry.pelClaimMinIdleMs < StreamMQConstants.MIN_PEL_CLAIM_MIN_IDLE_MS) {
            throw new StreamMQClientException(
                    "streammq.retry.pel-claim-min-idle-ms must be >= "
                            + StreamMQConstants.MIN_PEL_CLAIM_MIN_IDLE_MS
                            + " (否则仍处理中的消息会被误判为孤儿并重投), got: "
                            + retry.pelClaimMinIdleMs);
        }
        // 判活窗口必须显著大于心跳间隔（R4-B04）：窗口取自 pel-claim-min-idle-ms，而"消费者是否存活"
        // 依据心跳新鲜度；若窗口小于心跳间隔，活跃慢消费者的心跳会被判为过期 → 复制重投，重试耗尽后
        // 已成功处理的消息会进 DLQ。3 倍覆盖瞬时抖动与网络延迟。
        long minAliveWindowMs = group.heartbeatIntervalMs * 3;
        if (retry.pelClaimMinIdleMs < minAliveWindowMs) {
            throw new StreamMQClientException(
                    "streammq.retry.pel-claim-min-idle-ms must be >= 3x"
                            + " streammq.group.heartbeat-interval-ms ("
                            + minAliveWindowMs
                            + "ms), otherwise live consumers are judged dead and their messages"
                            + " are duplicated into DLQ after retry budget is exhausted; got"
                            + " pel-claim-min-idle-ms="
                            + retry.pelClaimMinIdleMs
                            + ", heartbeat-interval-ms="
                            + group.heartbeatIntervalMs);
        }
        if (delay.scanInterval.toMillis() <= 0) {
            throw new StreamMQClientException(
                    "streammq.delay.scan-interval must be > 0, got: " + delay.scanInterval);
        }
        if (transaction.checkInterval.toMillis() <= 0) {
            throw new StreamMQClientException(
                    "streammq.transaction.check-interval must be > 0, got: "
                            + transaction.checkInterval);
        }
        if (producer.retryTimes > StreamMQConstants.MAX_SYNC_RETRY_TIMES) {
            throw new StreamMQClientException(
                    "streammq.producer.retry-times must be <= "
                            + StreamMQConstants.MAX_SYNC_RETRY_TIMES
                            + ", got: "
                            + producer.retryTimes);
        }
    }
}
