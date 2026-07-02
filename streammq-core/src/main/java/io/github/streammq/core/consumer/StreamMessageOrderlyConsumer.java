package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMQOrderlyConsumer;
import io.github.streammq.core.enums.Action;
import io.github.streammq.core.message.Message;

/**
 * 顺序消费回调接口。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQOrderlyConsumer} 注解即可注册为顺序消费者。
 * 框架保证同一 {@code shardingKey} 的消息在单线程内串行消费。
 *
 * <p>返回值含义：
 * <ul>
 *   <li>{@link Action#SUCCESS} - 消费成功，自动 ACK，下一条继续</li>
 *   <li>{@link Action#SUSPEND_CURRENT_QUEUE_A_MOMENT} - 暂停当前 shard 一小段时间（默认 10ms），然后重新消费同一消息</li>
 *   <li>{@link Action#RECONSUME_LATER} - 进入重试，可能破坏顺序（谨慎使用）</li>
 * </ul>
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageOrderlyConsumer<T> {

    /**
     * 处理单条顺序消息。
     *
     * @param message 消息载体
     * @param context 顺序消费上下文
     * @return 处理结果动作
     * @throws Exception 业务异常，框架将其视为 {@link Action#SUSPEND_CURRENT_QUEUE_A_MOMENT}
     */
    Action onMessage(Message<T> message, ConsumeOrderlyContext context) throws Exception;
}
