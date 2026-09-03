/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.StreamMQConstants;

/**
 * 消费拉取运行参数读视图。
 *
 * <p><b>设计模式：Parameter Object。</b>实现类 {@link DefaultConsumerTuning} 提供带下界保护的写入方法；消费循环与注册流程仅依赖本接口。
 *
 * <h2>优先级统一口径（配置值 → 默认值 → 实际值 三方对等）</h2>
 *
 * <p>所有 {@code effective*} 方法遵循同一条规则：
 *
 * <pre>
 *   注解值 == StreamMQConstants.ANNOTATION_UNSET_*   →  取 streammq.* 全局配置
 *   全局配置未显式设置                                →  取 StreamMQConstants.DEFAULT_* 常量
 *   注解值 != UNSET                                   →  注解优先（用户显式声明最高优先级）
 * </pre>
 *
 * <p><b>为什么用独立哨兵而不用"与常量默认值比较"：</b>后者存在哨兵碰撞——当注解显式写上与常量默认值 相同的数值时（例如 {@code pullBatchSize = 32} 恰等于
 * {@link StreamMQConstants#DEFAULT_CONSUME_BATCH_SIZE}），
 * 框架无法区分"用户显式指定"与"使用注解默认值"，会被全局配置静默覆盖。这是典型的配置失效， 因此统一改用 {@link
 * StreamMQConstants#ANNOTATION_UNSET_INT} / {@link StreamMQConstants#ANNOTATION_UNSET_LONG}
 * 作为"未设置"标记。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerTuning {

    int defaultPullBatchSize();

    long defaultPullBlockTimeoutMillis();

    int inflightCapacity();

    /**
     * 返回当前生效的拉取批量上界（来自 {@code streammq.consumer.max-batch-size-limit}）。
     *
     * <p>底层 {@code RedissonStreamListener} 的批量校验必须使用该值而非 {@link
     * StreamMQConstants#MAX_BATCH_SIZE_LIMIT} 常量——否则用户把上界调大到超过常量时，
     * 全局配置生效、适配层校验却拒绝，形成自相矛盾的"配置值/实际值不对等"。
     *
     * @return 生效的拉取批量上界
     */
    int maxBatchSizeLimit();

    /**
     * 解析生效的拉取批量：注解 {@code > 0} 优先，否则取全局配置；最终夹取到 {@code [1, maxBatchSizeLimit]}。
     *
     * @param annotationValue 注解声明值；{@link StreamMQConstants#ANNOTATION_UNSET_INT} 表示未设置
     * @return 生效的拉取批量
     */
    int effectivePullBatchSize(int annotationValue);

    /**
     * 解析生效的拉取间隔：注解 {@code >= 0} 优先（0 = 不间隔），否则取全局配置。
     *
     * @param annotationValue 注解声明值；{@link StreamMQConstants#ANNOTATION_UNSET_LONG} 表示未设置
     * @return 生效的拉取间隔毫秒数
     */
    long effectivePullInterval(long annotationValue);

    /**
     * 解析生效的顺序消费超时（毫秒）：注解 {@code >= 0} 优先，否则取全局配置。
     *
     * <p><b>为什么默认关闭：</b>顺序消费超时后走串行重试，耗尽 {@code maxReconsumeTimes} 即进 DLQ。
     * 默认开启会把所有存量顺序消费者的慢消息系统性误杀，因此全局默认 0（不启用），由业务显式开启。
     *
     * <p><b>注解可单独关闭：</b>全局开启后，对某个消费者声明 {@code orderlyConsumeTimeout = 0} 即可 显式关闭该消费者的超时保护（0
     * 是"关闭"语义，不是"未设置"语义）。
     *
     * @param annotationValue 注解声明值；{@link StreamMQConstants#ANNOTATION_UNSET_LONG} 表示未设置
     * @return 生效的超时毫秒数；0 表示不启用顺序消费超时
     */
    long effectiveOrderlyConsumeTimeoutMillis(long annotationValue);

    /**
     * 解析生效的并发消费超时（毫秒）：注解 {@code >= 0} 优先（0 = 不超时），否则取全局配置。
     *
     * @param annotationValue 注解声明值；{@link StreamMQConstants#ANNOTATION_UNSET_LONG} 表示未设置
     * @return 生效的超时毫秒数；0 表示不设超时
     */
    long effectiveConsumeTimeoutMillis(long annotationValue);

    /**
     * 解析生效的最大重试次数：注解 {@code >= 0} 优先，否则取全局配置 {@code streammq.retry.max-reconsume-times}。
     *
     * <p>此前该全局配置声明后<b>从未被读取</b>（重试预算只取注解值），属于配置失效；本方法把它接入解析链。
     *
     * @param annotationValue 注解声明值；{@link StreamMQConstants#ANNOTATION_UNSET_INT} 表示未设置
     * @return 生效的最大重试次数
     */
    int effectiveMaxReconsumeTimes(int annotationValue);
}
