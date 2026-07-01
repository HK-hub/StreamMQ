package io.github.streammq.core.consumer;

import java.util.Map;

/**
 * StreamMQ 消费者工厂接口。
 *
 * <p>根据属性创建 {@link StreamMqConsumer} 实例。
 * 实现类位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMqConsumerFactory {

    /**
     * 创建消费者。
     *
     * @param properties 消费者属性，常用 key：
     *                   <ul>
     *                     <li>{@code topic} - 主题</li>
     *                     <li>{@code consumer-group} - 消费者组名</li>
     *                     <li>{@code consumer-name} - 消费者实例名（默认自动生成）</li>
     *                     <li>{@code consume-mode} - CLUSTERING / BROADCASTING</li>
     *                     <li>{@code namespace} - 命名空间</li>
     *                   </ul>
     * @return 消费者实例
     */
    StreamMqConsumer createConsumer(Map<String, Object> properties);

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
