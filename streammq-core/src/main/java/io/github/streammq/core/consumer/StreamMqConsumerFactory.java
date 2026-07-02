package io.github.streammq.core.consumer;

/**
 * StreamMQ 消费者工厂接口。
 *
 * <p>根据 {@link ConsumerConfig} 创建 {@link StreamMqConsumer} 实例。
 * 实现类位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMqConsumerFactory {

    /**
     * 创建消费者。
     *
     * @param config 消费者配置（主题、消费者组、命名空间等）
     * @return 消费者实例
     */
    StreamMqConsumer createConsumer(ConsumerConfig config);

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
