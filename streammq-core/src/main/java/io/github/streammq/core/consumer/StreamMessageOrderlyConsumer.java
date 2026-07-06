package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.OrderlyAction;
import io.github.streammq.core.message.Message;

/**
 * 顺序消费回调接口（返回值驱动 ACK / 挂起，对齐 RocketMQ {@code MessageListenerOrderly}）。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQConsumer}（{@code messageModel = ORDERLY}）注解即可注册为顺序消费者。
 * 框架保证同一 {@code shardingKey} 的消息在单线程内串行消费。
 *
 * <p>返回值含义（唯一消费结果表达方式）：
 * <ul>
 *   <li>{@link OrderlyAction#SUCCESS} - 消费成功，自动 ACK，下一条继续</li>
 *   <li>{@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT} - 暂停当前 shard 一小段时间（默认 10ms），
 *       消息留在 PEL 等待重新消费同一消息</li>
 * </ul>
 *
 * <p>抛出 {@link RuntimeException} 等价于返回 {@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT}。
 * 框架不提供手动 ACK/nack/defer 调用，避免双模式冲突。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageOrderlyConsumer<T> extends StreamMessageConsumer<T> {

    /**
     * 处理单条顺序消息。
     *
     * @param message 消息载体
     * @param context 顺序消费上下文
     * @return 处理结果动作，返回 null 视为 {@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT}
     * @throws Exception 业务异常，框架将其视为 {@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT}
     */
    OrderlyAction onMessage(Message<T> message, ConsumeOrderlyContext context) throws Exception;
}
