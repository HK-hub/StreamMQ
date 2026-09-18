/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.exception;

import java.io.Serial;

/**
 * 顺序消费分片锁繁忙异常：目标 shard 的分布式锁当前被其它实例（或本实例中不响应中断的"僵尸"消费线程）持有。
 *
 * <p><b>语义（与业务处理失败严格区分）：</b>
 *
 * <ul>
 *   <li>抛出本异常时，本条消息<b>从未被业务 handler 处理</b>——锁都未获得，{@code onMessage} 未执行；
 *   <li>调用方应<b>直接稍后重试</b>（{@code RECONSUME_LATER}），且<b>不得消耗业务重试预算</b> （{@code
 *       maxReconsumeTimes}）——否则多实例 rebalance 窗口内的锁竞争会耗尽预算， 把从未被处理过的消息误投进 DLQ；
 *   <li>消息保持未 ACK、留在消费者组 PEL 中，由 PEL 认领机制（{@code PelClaimScheduler}）兜底重投。
 * </ul>
 *
 * <p><b>典型出现场景：</b>
 *
 * <ul>
 *   <li>多实例 rebalance 窗口：同一 shard 在两个实例上短暂同时可见，后到者拿不到锁；
 *   <li>慢 handler 持锁：消费者配置了过小的顺序消费超时，handler 不响应中断而继续持锁， 被超时取消的任务既没有释放锁、也没有真正结束。
 * </ul>
 *
 * <p>与 {@link ConsumerInterruptedException}（消费线程被强制停止）不同：本异常表达
 * "锁竞争导致的短暂不可消费"，属于可自愈的正常调度状态，而非需要人工介入的故障。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public class OrderlyShardBusyException extends StreamMQException {

    @Serial private static final long serialVersionUID = 1L;

    /**
     * 构造异常。
     *
     * @param message 错误信息（建议包含 topic / group / shard 与等待参数，便于运维定位）
     */
    public OrderlyShardBusyException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param cause 原始异常（如锁等待被中断时的 {@code InterruptedException}）
     */
    public OrderlyShardBusyException(String message, Throwable cause) {
        super(message, cause);
    }
}
