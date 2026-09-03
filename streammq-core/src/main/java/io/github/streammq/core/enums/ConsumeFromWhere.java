/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

/**
 * 新消费者组的起始消费位点策略（对齐 RocketMQ {@code ConsumeFromWhere}）。
 *
 * <p><b>生效时机：</b>仅当 Redis 消费者组<b>首次创建</b>时生效。已存在的消费者组不会因本配置改变位点—— Redis 消费者组的位点由组自身维护，重启应用不会重建位点。
 *
 * <h2>两种策略</h2>
 *
 * <ul>
 *   <li>{@link #CONSUME_FROM_LAST}（默认）：从 Stream 当前末尾开始消费，只接收组创建之后新写入的消息。 对应 Redis {@code XGROUP
 *       CREATE ... ID $}。这是 MQ 的安全默认——向一个已运行数月的 Topic 追加消费者组时不会触发历史重放风暴。
 *   <li>{@link #CONSUME_FROM_FIRST}：从 Stream 第一条消息开始消费，重放全部历史。 对应 Redis {@code XGROUP CREATE ... ID
 *       0-0}。适用于"补算历史数据""新建 Topic 即订阅"等场景。
 * </ul>
 *
 * <h2>与广播消费的关系（重要）</h2>
 *
 * <p>广播模式下每个容器实例使用独立的消费者组名（{@code {group}:{consumerName}}）。当 {@code instanceToken} 不稳定（如 UUID
 * 回退）时，每次重启都会产生一个新组，此时本配置决定新组的行为：
 *
 * <ul>
 *   <li>{@code CONSUME_FROM_LAST}：重启期间产生的消息不会被补投（新组从重启时刻之后开始消费）；
 *   <li>{@code CONSUME_FROM_FIRST}：每次重启都会重放该 Topic 的<b>全部历史</b>——生产环境慎用。
 * </ul>
 *
 * <p>因此广播模式强烈建议配置稳定的 {@code streammq.instanceId}（或 {@code -Dstreammq.instance.id} / {@code
 * STREAMMQ_INSTANCE_ID}），使组名跨重启保持不变，位点即可复用，本配置不再被触发。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public enum ConsumeFromWhere {

    /** 从 Stream 当前末尾开始消费（默认，安全）：只消费组创建之后写入的消息。 */
    CONSUME_FROM_LAST,

    /** 从 Stream 第一条消息开始消费：重放全部历史消息。 */
    CONSUME_FROM_FIRST;

    /**
     * 全局默认策略：{@link #CONSUME_FROM_LAST}。
     *
     * <p>与 {@code streammq.consumer.consume-from-where} 的默认值常量保持单一来源 （{@link
     * io.github.streammq.core.StreamMQConstants#DEFAULT_CONSUME_FROM_WHERE}）。
     */
    public static final ConsumeFromWhere DEFAULT = CONSUME_FROM_LAST;
}
