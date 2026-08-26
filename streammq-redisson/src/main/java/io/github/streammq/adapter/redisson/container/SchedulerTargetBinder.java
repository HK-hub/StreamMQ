/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;

/**
 * 调度器目标绑定器。
 *
 * <p><b>设计模式：Facade。</b><b>SPI：</b>容器仅依赖本接口； 默认实现 {@link DefaultSchedulerTargetBinder}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface SchedulerTargetBinder {

    /** 将全部注册 Listener 绑定到重试调度器（DLQ 以 (group, group) 维度登记）。 */
    void bindRetryTargets(RetryScheduler scheduler);

    /** 将消费 Listener 绑定到 PEL 认领调度器。 */
    void bindPelClaimTargets(PelClaimScheduler scheduler);

    /** 手动触发指定 ORDERLY 组的重平衡；返回 false 表示未找到可执行的组。 */
    boolean rebalanceGroup(String group);
}
