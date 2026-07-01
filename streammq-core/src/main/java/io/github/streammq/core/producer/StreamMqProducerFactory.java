package io.github.streammq.core.producer;

import io.github.streammq.core.message.MessageId;

import java.util.Map;

/**
 * StreamMQ 生产者工厂接口。
 *
 * <p>根据属性创建 {@link StreamMqProducer} 实例。
 * 实现类位于 {@code streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMqProducerFactory {

    /**
     * 创建生产者。
     *
     * @param properties 生产者属性，常用 key：
     *                   <ul>
     *                     <li>{@code group} - 生产者组名</li>
     *                     <li>{@code send-message-timeout} - 发送超时（毫秒）</li>
     *                     <li>{@code retry-times} - 同步发送重试次数</li>
     *                     <li>{@code namespace} - 命名空间</li>
     *                     <li>{@code serializer} - 序列化器类全限定名</li>
     *                   </ul>
     * @return 生产者实例
     */
    StreamMqProducer createProducer(Map<String, Object> properties);

    /**
     * 关闭工厂，释放底层资源（如 Redisson 连接）。
     */
    void close();

    /**
     * 返回工厂是否已关闭。
     *
     * @return true 如果已关闭
     */
    boolean isClosed();

    /**
     * 返回工厂创建的下一个消息 ID（仅用于测试与监控，生产代码不应依赖）。
     *
     * @return 消息 ID 字符串
     * @deprecated 仅用于内部测试
     */
    @Deprecated(since = "0.1.0", forRemoval = true)
    default MessageId nextMessageId() {
        throw new UnsupportedOperationException();
    }
}
