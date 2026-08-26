/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import java.util.concurrent.Future;

/**
 * 消费循环监督者。
 *
 * <p><b>设计模式：Mediator/Supervisor + Command。</b>读循环以 Command（Future）提交， 实现独占 Future 登记表的键约定（{@code
 * :retry} / {@code :cc-N} / {@code :inflight-processor}）、 提交幂等守卫与按注册前缀整体取消。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumeLoopSupervisor {

    /** 循环命令工厂：由容器绑定到注入的 ExecutorService。 */
    interface LoopFactory {
        Future<?> launch(ListenerRegistration<?> reg, boolean retryMode, boolean primaryLoop);
    }

    /** 为单个注册提交全部读循环（幂等）。 */
    void submitLoops(ListenerRegistration<?> reg);

    /** 登记 inflight 泵 Future（供 unregister 取消）。 */
    void registerInflightPump(String key, Future<?> pumpFuture);

    /** 按注册键取消全部相关任务。 */
    void cancelForRegistration(String key);

    /** 取消全部任务（容器 stop）。 */
    void cancelAll();
}
