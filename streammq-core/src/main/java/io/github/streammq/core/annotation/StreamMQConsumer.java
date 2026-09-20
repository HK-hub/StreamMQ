/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.filter.ConsumerFilter;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.annotation.*;

/**
 * StreamMQ 消费者注解（类级），标注在 {@code StreamMessageConcurrentlyConsumer} /
 * {@code StreamMessageOrderlyConsumer} 实现类上。
 *
 * <p>对齐 RocketMQ {@code @RocketMQMessageListener} 体验。本注解为统一入口，
 * 通过 {@link #messageModel()} 区分并发 / 顺序消费。
 *
 * <p>使用示例：
 * <pre>{@code
 * // 并发消费
 * @Component
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg")
 * public class OrderConsumer implements StreamMessageConcurrentlyConsumer<Order> {
 *     @Override
 *     public ConsumeAction onMessage(Message<Order> message, ConsumeContext context) {
 *         processOrder(message.getBody());
 *         return ConsumeAction.SUCCESS;
 *     }
 * }
 *
 * // 顺序消费
 * @Component
 * @StreamMQConsumer(topic = "order-topic", consumerGroup = "order-cg",
 *                  messageModel = MessageModel.ORDERLY, shardCount = 8)
 * public class OrderOrderlyConsumer implements StreamMessageOrderlyConsumer<Order> {
 *     @Override
 *     public ConsumeAction onMessage(Message<Order> message, ConsumeOrderlyContext context) {
 *         processOrder(message.getBody());
 *         return ConsumeAction.SUCCESS;
 *     }
 * }
 *
 * // DLQ 消费请使用 @StreamMQDlqConsumer + DlqMessageConsumer 接口
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMQConsumer {

    /**
     * 主题（必填）。
     *
     * @return 主题
     */
    String topic();

    /**
     * 消费者组名（必填）。
     *
     * @return 消费者组
     */
    String consumerGroup();

    /**
     * 消费模式，默认 {@link ConsumeMode#CLUSTERING}。
     *
     * @return 消费模式
     */
    ConsumeMode consumeMode() default ConsumeMode.CLUSTERING;

    /**
     * 新消费者组的起始消费位点，<b>默认不声明</b>（{@link ConsumeFromWhere#ANNOTATION_DEFAULT}），即跟随全局配置 {@code
     * streammq.consumer.consume-from-where}（其默认值 {@link ConsumeFromWhere#DEFAULT} = {@link
     * ConsumeFromWhere#CONSUME_FROM_LAST}）。
     *
     * <p><b>仅在该消费者组首次创建时生效</b>——已存在的组不会因为本配置改变位点。
     *
     * <p>取值语义（三分支，{@link ConsumeFromWhere}）：
     *
     * <ul>
     *   <li><b>不声明</b>（默认 {@link ConsumeFromWhere#ANNOTATION_DEFAULT}）：跟随全局配置 {@code
     *       streammq.consumer.consume-from-where}（其默认 {@code CONSUME_FROM_LAST}）
     *   <li>{@link ConsumeFromWhere#CONSUME_FROM_LAST}：<b>显式覆盖</b>全局配置，只消费组创建之后写入的消息。
     *       安全默认语义——向长期运行的 Topic 追加消费者组不会触发历史重放
     *   <li>{@link ConsumeFromWhere#CONSUME_FROM_FIRST}：<b>显式覆盖</b>全局配置，重放该 Topic 的全部历史消息
     * </ul>
     *
     * <p><b>为什么默认值不是 {@code CONSUME_FROM_LAST}：</b>枚举属性无法用 {@code null} 表达"未声明"， 旧实现把默认值错设为 {@code
     * CONSUME_FROM_LAST}，于是"未声明"与"显式 LAST"不可区分—— 全局设为 {@code CONSUME_FROM_FIRST} 时，想单独强制回 {@code
     * LAST} 的消费者会被静默忽略（语义反向）。 独立哨兵使两条语义都可表达。
     *
     * <p><b>与广播消费的关系：</b>广播模式下每个实例使用独立组名，若 {@code instanceToken} 不稳定（UUID 回退）， 每次重启都会新建组，此时 {@code
     * CONSUME_FROM_FIRST} 会导致<b>每次重启重放全量历史</b>。 广播模式请先配置稳定的 {@code streammq.instanceId}。
     *
     * @return 起始消费位点策略；未声明时为 {@link ConsumeFromWhere#ANNOTATION_DEFAULT}
     */
    ConsumeFromWhere consumeFromWhere() default ConsumeFromWhere.ANNOTATION_DEFAULT;

    /**
     * 消息模型，默认 {@link MessageModel#CONCURRENT}。
     *
     * <p>设置为 {@link MessageModel#ORDERLY} 时表示顺序消费，需实现 {@link
     * io.github.streammq.core.consumer.StreamMessageOrderlyConsumer}， {@link #shardCount()} 生效。
     *
     * <p>顺序消费实现为「单 Stream + 分片分布式锁」：同一 {@code shardingKey} 路由到同一分片串行消费； 消费失败时在当前线程内按 {@link
     * #maxReconsumeTimes()} 重试，每次失败后按 {@link #suspendCurrentQueueTimeMillis()} 挂起， 保证同分片不越过失败消息
     * （严格有序）；重试耗尽后直接进入 DLQ。
     *
     * @return 消息模型
     */
    MessageModel messageModel() default MessageModel.CONCURRENT;

    /**
     * 并发消费循环数，默认 1。仅 {@link MessageModel#CONCURRENT} 且集群消费生效； 顺序 / DLQ / 广播消费固定为单循环。
     *
     * <p>每个循环独立执行 XREADGROUP 拉取（共享同一 consumer name，Redis 原子分配保证互不相交）， 提升单实例并行度。取值被夹取到 {@code [1,
     * 64]}。
     *
     * <p><b>语义澄清（0.1.2）：</b>本属性是"并发消费循环数"的唯一推荐写法，取代历史上语义相反的 {@code consumeThreadMin}（旧名，等价）与 {@code
     * consumeThreadMax}（旧名，<b>不再生效</b>）。
     *
     * @return 并发消费循环数
     * @since 0.1.2
     */
    int consumeThreads() default 1;

    /**
     * 并发消费循环数，默认 1。仅 {@link MessageModel#CONCURRENT} 且集群消费生效； 顺序 / DLQ / 广播消费固定为单循环。
     *
     * <p>每个循环独立执行 XREADGROUP 拉取（共享同一 consumer name，Redis 原子分配保证互不相交）， 提升单实例并行度。
     *
     * @return 并发消费循环数
     * @deprecated 名称沿用线程池语义但实际是"消费循环数"，与生态惯例（min 为下限、max 为上限）相反，容易 误配；等价于 {@link
     *     #consumeThreads()}，仅为源码兼容保留，请改用 {@link #consumeThreads()}。 当本属性被显式设置为非默认值 1
     *     时按本属性生效（兼容旧写法）；显式声明 {@link #consumeThreads()} 时后者优先。
     */
    @Deprecated(since = "0.1.2", forRemoval = false)
    int consumeThreadMin() default 1;

    /**
     * 已废弃且<b>不再生效</b>：并发消费循环数上限。
     *
     * <p>历史上它被 {@code consumeThreadMin} 夹取（实际并发数 = min），只配置本属性的用户会得到单循环消费且无任何提示。 0.1.2 起并发度只由
     * {@link #consumeThreads()}（或旧名 {@code consumeThreadMin}）决定，本属性被忽略；显式设置为非默认值 64 时会在注册期打 WARN
     * 提示。保留仅为源码兼容，将于 0.2.0 移除。
     *
     * @return 已废弃的并发上限（不再生效）
     * @deprecated 使用 {@link #consumeThreads()} 表达并发度；本属性不再影响并发数
     */
    @Deprecated(since = "0.1.2", forRemoval = true)
    int consumeThreadMax() default StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX;

    /**
     * 最大重试次数。
     *
     * <p><b>优先级（配置值 → 默认值 → 实际值 三方对等）：</b>
     *
     * <ul>
     *   <li>本属性 {@code >= 0}：以注解声明为准（用户显式优先）。{@code 0} 表示消费失败不重试、直接进 DLQ
     *   <li>本属性 = {@link StreamMQConstants#ANNOTATION_UNSET_INT}（默认 -1）：回落到全局配置 {@code
     *       streammq.retry.max-reconsume-times}
     *   <li>全局配置默认值 = {@link StreamMQConstants#DEFAULT_MAX_RECONSUME_TIMES}（16）
     * </ul>
     *
     * @return 最大重试次数；-1 表示使用全局配置
     */
    int maxReconsumeTimes() default StreamMQConstants.ANNOTATION_UNSET_INT;

    /**
     * 单条消息消费超时（毫秒）。<b>统一哨兵语义（0.1.2 定稿，发布即冻结）：{@code -1} = 未声明（默认，跟随全局配置）， {@code 0} =
     * 显式关闭超时保护，{@code >0} = 超时毫秒数。</b>
     *
     * <p>超时后框架会取消当前消费并调度重试投递。由于消费线程可能仍在执行业务逻辑， 重试消费与原消费可能并发执行，因此业务层必须实现幂等性。
     *
     * <p>仅对并发消费（{@link MessageModel#CONCURRENT}）生效，顺序消费请使用 {@link #orderlyConsumeTimeout()}。
     *
     * <p><b>性能含义（务必知悉）：</b>设为正数后，框架会为<b>每一条</b>消息执行一次 {@code executor.submit()} + {@code
     * Future.get(timeout)}（+ 超时后的 {@code join} 等待），用于中断卡死的 handler。 这是每条消息的固定成本。全局默认值为 {@link
     * StreamMQConstants#DEFAULT_CONSUME_TIMEOUT_MS}（{@code 0} = 不启用）； 关闭时卡死消息由 {@code
     * PelClaimScheduler} 在空闲阈值（默认 60s）后认领重投，at-least-once 语义不变。
     *
     * <p><b>注意默认即"未声明"：</b>默认值 {@link StreamMQConstants#ANNOTATION_UNSET_LONG}（{@code -1}）不是"关闭"，
     * 而是跟随全局配置；仅当全局也关闭（其默认 {@code 0}）时才等效于不启用。要<b>显式关闭</b>请写 {@code 0} （覆盖非 0 的全局配置），要启用请写正毫秒值。
     *
     * @return 超时毫秒数；{@code >0} 以注解为准；{@code 0} 显式关闭；{@code -1}（默认）跟随全局配置
     */
    long consumeTimeout() default StreamMQConstants.ANNOTATION_UNSET_LONG;

    /**
     * 顺序消费单条消息消费超时（毫秒）。<b>统一哨兵语义（0.1.2 定稿，发布即冻结）：{@code -1} = 未声明（默认，跟随全局配置）， {@code 0} =
     * 显式关闭超时保护，{@code >0} = 超时毫秒数。</b>
     *
     * <p>仅对顺序消费（{@link MessageModel#ORDERLY}）生效。顺序消费默认不设超时——卡死的 handler 会持有分片锁
     * 并阻塞消费循环，直到进程重启。设置本属性后：
     *
     * <ul>
     *   <li>单次消费超过该时长即视为失败：框架按 {@code RECONSUME_LATER} 处理并释放分片锁（消费循环不再被阻塞）
     *   <li>重试在 {@link #maxReconsumeTimes()} 次数内进行，耗尽后消息进入 DLQ
     *   <li>若业务 handler 不响应线程中断，原消费线程仍可能继续运行，因此业务层必须保证幂等
     * </ul>
     *
     * <p>注意：顺序消费的重试是严格串行的（同分片不越过失败消息），设置过小的超时可能将慢消息快速送入 DLQ， 建议按业务最慢耗时的 2 倍以上配置。
     *
     * <p><b>与全局配置的关系（0.1.2 统一哨兵语义）：</b>
     *
     * <ul>
     *   <li>本属性 {@code > 0}：以注解为准，覆盖全局配置
     *   <li>本属性 {@code = 0}：<b>显式关闭</b>该消费者的顺序消费超时保护，即使全局已开启也不生效
     *   <li>本属性 {@code < 0}（含默认值 {@link StreamMQConstants#ANNOTATION_UNSET_LONG} = -1）：<b>未声明</b>，
     *       跟随全局配置 {@code streammq.consumer.orderly-consume-timeout-millis}（其默认 {@code 0} = 不启用）
     * </ul>
     *
     * <p><b>注意默认即"未声明"：</b>注解默认值为 {@code -1}，未显式声明时该保护由全局配置决定（全局默认关闭）； 需要<b>显式关闭</b>（在全局开启时）请写
     * {@code 0}，需要启用请写正毫秒值。
     *
     * @return 超时毫秒数；{@code >0} 覆盖全局；{@code 0} 显式关闭；{@code -1}（默认）跟随全局配置
     */
    long orderlyConsumeTimeout() default StreamMQConstants.ANNOTATION_UNSET_LONG;

    /**
     * Tag 过滤表达式（SQL92 风格子集），默认 "*" 表示全部接收。 例如：{@code "tag1 || tag2"} / {@code "tag1 && tag2"}。
     *
     * @return 过滤表达式
     */
    String selectorExpression() default "*";

    /**
     * 序列化器实现类，默认使用全局配置。
     *
     * @return 序列化器类
     */
    Class<? extends MessageSerializer> serializer() default MessageSerializer.class;

    /**
     * 命名空间，默认使用全局配置。
     *
     * <p>命名空间用于隔离不同环境/租户的 Stream Key，避免 Key 冲突。 命名空间会附加到所有 Redis Key 前缀：{@code streammq:{ns}:...}。
     *
     * <p><b>作用域规则：</b>
     *
     * <ul>
     *   <li>注解中的 {@code namespace} 优先级高于配置文件中的 {@code streammq.namespace}
     *   <li>不同 namespace 下的消息完全隔离（不同 Stream、不同 Consumer Group、不同 Retry ZSet）
     *   <li>namespace 会影响 Consumer Group 命名：Group Key 包含 namespace 前缀
     * </ul>
     *
     * @return 命名空间，空字符串表示使用全局配置
     */
    String namespace() default "";

    /**
     * 消息过滤类型，默认 {@link SelectorType#TAG}。
     *
     * @return 过滤类型
     */
    SelectorType selectorType() default SelectorType.TAG;

    /**
     * 单次拉取批量大小。
     *
     * <p><b>优先级：</b>本属性 {@code > 0} 时以注解为准；为 {@link StreamMQConstants#ANNOTATION_UNSET_INT} （默认
     * -1）时回落全局配置 {@code streammq.consumer.batch-size}，其默认值为 {@link
     * StreamMQConstants#DEFAULT_CONSUME_BATCH_SIZE}（32）。最终值会被 {@code
     * streammq.consumer.max-batch-size-limit} 夹取上界。
     *
     * @return 拉取批量；-1 表示使用全局配置
     */
    int pullBatchSize() default StreamMQConstants.ANNOTATION_UNSET_INT;

    /**
     * 每个消费者专属重试策略类，默认 {@link RetryPolicy} 表示使用全局策略。
     *
     * <p>注：使用 raw type {@code Class<? extends RetryPolicy>}，因为 {@code RetryPolicy.class} 返回的是 raw
     * type，无法直接用于泛型 {@code Class<? extends RetryPolicy<?>>}。
     *
     * @return 重试策略类
     */
    Class<? extends RetryPolicy> retryPolicy() default RetryPolicy.class;

    /**
     * 是否启用消息追踪，默认 false。 设置为 true 时将覆盖全局追踪开关，对该消费者单独启用追踪。
     *
     * @return true 启用追踪
     */
    boolean enableMsgTrace() default false;

    /**
     * Stream 最大长度（0=不限制，per-topic 覆盖全局配置）。
     *
     * @return Stream 最大长度
     */
    int streamMaxLen() default 0;

    /**
     * 每个消费者专属消息转换器（默认表示使用全局）。
     *
     * <p>注：使用 raw type {@code Class<? extends MessageConverter>}，因为 {@code MessageConverter.class}
     * 返回的是 raw type，无法直接用于泛型 {@code Class<? extends MessageConverter<?>>}。
     *
     * @return 消息转换器类
     */
    Class<? extends MessageConverter> messageConverter() default MessageConverter.class;

    /**
     * 每个消费者专属重平衡策略（默认表示使用全局）。
     *
     * @return 重平衡策略类
     */
    Class<? extends RebalanceStrategy> rebalanceStrategy() default RebalanceStrategy.class;

    /**
     * 拉取间隔（毫秒）。
     *
     * <p><b>优先级：</b>本属性 {@code >= 0} 时以注解为准（{@code 0} = 不间隔，即拉取之间不主动休眠）； 为 {@link
     * StreamMQConstants#ANNOTATION_UNSET_LONG}（默认 -1）时回落全局配置 {@code
     * streammq.consumer.pull-interval}，其默认值为 {@link StreamMQConstants#DEFAULT_PULL_INTERVAL_MS}（0）。
     *
     * @return 拉取间隔毫秒；0 表示不间隔；-1 表示使用全局配置
     */
    long pullInterval() default StreamMQConstants.ANNOTATION_UNSET_LONG;

    /**
     * 顺序消费挂起时长（毫秒）。
     *
     * @return 挂起毫秒数
     */
    long suspendCurrentQueueTimeMillis() default
            StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;

    /**
     * retry Stream 最大长度（0=不限制，per-topic 覆盖全局配置）。
     *
     * <p>仅对并发消费生效。retry Stream 是 {@code streammq:{ns}:retry:msg:{topic}:{group}}， 设置上限可防止重试消息无限堆积。
     *
     * @return retry Stream 最大长度
     */
    int retryStreamMaxLen() default StreamMQConstants.DEFAULT_RETRY_STREAM_MAX_LEN;

    /**
     * 是否启用消费，默认 true。 设置为 false 时仅注册但不启动 Consumer。
     *
     * @return true 启用，false 仅注册
     */
    boolean enable() default true;

    /**
     * 最大 shard 数（顺序消费分区数），默认 4。
     *
     * <p>仅当 {@link #messageModel()} = {@link MessageModel#ORDERLY} 时生效。
     *
     * @return shard 数
     */
    int shardCount() default StreamMQConstants.DEFAULT_SHARD_COUNT;

    /**
     * 每个消费者专属过滤器（默认 {@link ConsumerFilter} 表示使用全局过滤器）。
     *
     * <p>过滤器从 Spring 容器中获取实例，支持多个过滤器（逗号分隔）。 过滤器执行顺序：先执行 {@link #selectorExpression()}
     * 对应的内置过滤器（order = -1）， 再按 {@link ConsumerFilter#order()} 升序执行自定义过滤器。
     *
     * <p><b>与 selectorExpression 的关系：</b>
     *
     * <ul>
     *   <li>{@code selectorExpression} 是内置的 Tag/SQL 过滤，执行优先级最高（order = -1）
     *   <li>{@code consumerFilter} 是自定义过滤器 SPI，执行优先级低于内置过滤器
     *   <li>两者是<b>串联</b>关系：先执行 selectorExpression 过滤，再执行 consumerFilter 过滤
     *   <li>如果用户同时配置了两者，只有同时通过两种过滤的消息才会被消费
     * </ul>
     *
     * @return 过滤器类
     */
    Class<? extends ConsumerFilter>[] consumerFilter() default {};

    /**
     * 消费者实例名（可选，默认空字符串表示自动生成）。
     *
     * @return 消费者实例名
     */
    String consumerName() default "";

    /**
     * 是否为 DLQ 消费者（默认 false）。
     *
     * <p>当设置为 true 时，消费者将从 DLQ Stream 读取消息（而不是原始 Topic Stream）。 适用于希望使用统一 {@link
     * StreamMessageConcurrentlyConsumer} 接口处理 DLQ 消息的场景。
     *
     * <p>注意：更推荐使用 {@link StreamMQDlqConsumer} 注解 + {@link
     * io.github.streammq.core.consumer.DlqMessageConsumer} 接口， 这是更类型安全的方式。
     *
     * @return true 表示 DLQ 消费者
     */
    boolean dlqMode() default false;
}
