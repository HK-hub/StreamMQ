/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

/**
 * 容器生命周期状态机。
 *
 * <p><b>SPI：</b>容器仅依赖本接口；默认实现 {@link DefaultContainerStateMachine} 以集中迁移表（State 模式）编码全部合法迁移：
 *
 * <pre>
 * INIT ─beginStart→ STARTING ─markRunning→ RUNNING ─tryBeginStop→ STOPPING ─finishStop→ STOPPED
 *  ▲                                                                  │
 *  └────────────── resetToInitIfStopped（assertRegistrable 内 CAS） ◀──┘
 * </pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ContainerStateMachine {

    ContainerState current();

    boolean isRunning();

    /** 注册前置校验；非法状态抛 {@link IllegalStateException}。 */
    void assertRegistrable();

    /** start 第一阶段：STOPPED→INIT 复位后 INIT→STARTING。 */
    void beginStart();

    /**
     * start 第二阶段：STARTING→RUNNING（仅限 CAS 迁移）。
     *
     * <p>返回 false 表示当前状态已非 STARTING（如并发 stop 已将容器置为 STOPPED）， 调用方必须放弃启动后续步骤 （登记组管理器、提交读循环等）——旧实现无条件
     * set(RUNNING) 会在竞态下「复活」已停止的容器。
     */
    boolean markRunning();

    /** stop 入口；返回 false 表示无需停机或竞态失败。 */
    boolean tryBeginStop();

    /** stop 收尾：STOPPING→STOPPED。 */
    void finishStop();
}
