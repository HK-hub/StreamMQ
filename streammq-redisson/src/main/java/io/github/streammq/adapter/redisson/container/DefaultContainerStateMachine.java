/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link ContainerStateMachine} 默认实现：以集中迁移表（State 模式）编码全部合法迁移， 非法迁移抛 {@link
 * IllegalStateException}，竞态迁移返回 false 由调用方决定语义。
 *
 * <p>线程安全：基于 {@link AtomicReference} CAS。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultContainerStateMachine implements ContainerStateMachine {

    private final AtomicReference<ContainerState> state =
            new AtomicReference<>(ContainerState.INIT);

    @Override
    public ContainerState current() {
        return state.get();
    }

    @Override
    public boolean isRunning() {
        return state.get() == ContainerState.RUNNING;
    }

    @Override
    public void assertRegistrable() {
        ContainerState current = state.get();
        switch (current) {
            case INIT, RUNNING -> {
                return;
            }
            case STOPPED -> {
                if (state.compareAndSet(ContainerState.STOPPED, ContainerState.INIT)) {
                    return;
                }
            }
            default -> {
                // fallthrough 到下方异常
            }
        }
        throw new IllegalStateException(
                "Cannot register listener in container state "
                        + state.get()
                        + " (rebinding in progress or container starting)");
    }

    @Override
    public void beginStart() {
        if (state.get() == ContainerState.STOPPED
                && !state.compareAndSet(ContainerState.STOPPED, ContainerState.INIT)) {
            throw new IllegalStateException(
                    "Container restart raced with another lifecycle change: " + state.get());
        }
        if (!state.compareAndSet(ContainerState.INIT, ContainerState.STARTING)) {
            throw new IllegalStateException(
                    "Container already started or in invalid state: " + state.get());
        }
    }

    /**
     * start 第二阶段：STARTING→RUNNING（CAS 迁移）。
     *
     * <p>仅当当前状态为 STARTING 时迁移成功；其它状态（并发 stop 已完成、重复启动等）返回 false， 由调用方中止启动——无条件 set 会把竞态中已 STOPPED
     * 的容器「复活」为 RUNNING（持有已关闭的执行器）。
     */
    @Override
    public boolean markRunning() {
        return state.compareAndSet(ContainerState.STARTING, ContainerState.RUNNING);
    }

    @Override
    public boolean tryBeginStop() {
        ContainerState current = state.get();
        if (current == ContainerState.STOPPED || current == ContainerState.INIT) {
            return false;
        }
        return state.compareAndSet(current, ContainerState.STOPPING);
    }

    @Override
    public void finishStop() {
        state.set(ContainerState.STOPPED);
    }
}
