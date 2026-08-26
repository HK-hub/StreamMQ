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

    @Override
    public void markRunning() {
        state.set(ContainerState.RUNNING);
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
