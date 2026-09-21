/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

/**
 * StreamMQ 监听器工厂接口。
 *
 * <p>根据 {@link ListenerConfig} 创建 {@link StreamMQListener} 实例。 实现类位于 {@code streammq-redisson} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQListenerFactory extends AutoCloseable {

    /**
     * 创建监听器。
     *
     * @param config 监听器配置（主题、消费者组、命名空间等）
     * @return 监听器实例
     */
    StreamMQListener createListener(ListenerConfig config);

    /**
     * 关闭工厂，释放底层资源。
     *
     * <p>实现必须<b>幂等</b>；关闭后 {@link #createListener(ListenerConfig)} 必须快速失败（不得返回已逃逸出关闭流程的监听器）。 该签名把
     * {@link AutoCloseable#close()} 的 {@code throws Exception} 收窄为不抛出，因此支持 try-with-resources。
     */
    @Override
    void close();

    /**
     * 返回工厂是否已关闭。
     *
     * @return true 如果已关闭
     */
    boolean isClosed();
}
