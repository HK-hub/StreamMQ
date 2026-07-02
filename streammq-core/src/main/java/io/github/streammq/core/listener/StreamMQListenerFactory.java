package io.github.streammq.core.listener;

/**
 * StreamMQ 监听器工厂接口。
 *
 * <p>根据 {@link ListenerConfig} 创建 {@link StreamMQListener} 实例。
 * 实现类位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMQListenerFactory {

    /**
     * 创建监听器。
     *
     * @param config 监听器配置（主题、消费者组、命名空间等）
     * @return 监听器实例
     */
    StreamMQListener createListener(ListenerConfig config);

    /**
     * 关闭工厂，释放底层资源。
     */
    void close();

    /**
     * 返回工厂是否已关闭。
     *
     * @return true 如果已关闭
     */
    boolean isClosed();
}
