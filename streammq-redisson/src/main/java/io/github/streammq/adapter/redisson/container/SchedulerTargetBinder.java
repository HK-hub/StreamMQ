/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.core.listener.ListenerRegistration;

/**
 * 调度器目标绑定器。
 *
 * <p><b>设计模式：Facade。</b><b>SPI：</b>容器仅依赖本接口； 默认实现 {@link DefaultSchedulerTargetBinder}。
 *
 * <p>绑定/解绑必须成对且覆盖全生命周期：容器启动时批量绑定、运行期动态注册单个绑定、 注销（或动态注册回滚）时解除绑定。缺少任一环节都会造成真实故障—— 未绑定 ⇒ 失败消息写进调度 ZSet
 * 后永不被扫描（静默不重投）；未解绑 ⇒ 调度目标无限增长。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface SchedulerTargetBinder {

    /** 将全部注册 Listener 绑定到重试调度器（DLQ 以 (group, group) 维度登记）。 */
    void bindRetryTargets(RetryScheduler scheduler);

    /** 将消费 Listener 绑定到 PEL 认领调度器。 */
    void bindPelClaimTargets(PelClaimScheduler scheduler);

    /**
     * 绑定<b>单个</b>注册项（运行期动态注册路径）。
     *
     * @param retryScheduler 重试调度器，可为 null（未装配时跳过）
     * @param pelClaimScheduler PEL 认领调度器，可为 null（未装配时跳过）
     * @param reg 新增的注册项
     */
    void bindTargets(
            RetryScheduler retryScheduler,
            PelClaimScheduler pelClaimScheduler,
            ListenerRegistration<?> reg);

    /**
     * 解除<b>单个</b>注册项的调度目标（与 {@link #bindTargets} 配对）。
     *
     * @param retryScheduler 重试调度器，可为 null
     * @param pelClaimScheduler PEL 认领调度器，可为 null
     * @param reg 被移除的注册项
     */
    void unbindTargets(
            RetryScheduler retryScheduler,
            PelClaimScheduler pelClaimScheduler,
            ListenerRegistration<?> reg);

    /** 手动触发指定 ORDERLY 组的重平衡；返回 false 表示未找到可执行的组。 */
    boolean rebalanceGroup(String group);
}
