package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.OrderlyAction;
import io.github.streammq.core.message.Message;

/**
 * 顺序消费回调接口。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQConsumer}（{@code messageModel = ORDERLY}）注解即可注册为顺序消费者。
 * 框架保证同一 {@code shardingKey} 的消息在单线程内串行消费。
 *
 * <p>返回值含义：
 * <ul>
 *   <li>{@link OrderlyAction#SUCCESS} - 消费成功，自动 ACK，下一条继续</li>
 *   <li>{@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT} - 暂停当前 shard 一小段时间（默认 10ms），然后重新消费同一消息</li>
 * </ul>
 *
 * <p>在 {@link AcknowledgeMode#MANUAL} 模式下，{@code onMessage} 的返回值被忽略，
 * 由 Consumer 通过 {@link ConsumeContext#acknowledge()} 显式控制 ACK。
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
     * @return 处理结果动作
     * @throws Exception 业务异常，框架将其视为 {@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT}
     */
    OrderlyAction onMessage(Message<T> message, ConsumeOrderlyContext context) throws Exception;

    @Override
    default void consumeMessage(Message<T> message, ConsumeContext context) throws Exception {
        ConsumeOrderlyContext orderlyContext = (ConsumeOrderlyContext) context;
        OrderlyAction action = onMessage(message, orderlyContext);
        // AUTO 模式下：SUCCESS 标记 context 已 ACK；SUSPEND 不做标记（消息留在 PEL）
        // MANUAL 模式下：onMessage 返回值被忽略，由 context.acknowledge() 控制
        if (action == OrderlyAction.SUCCESS) {
            context.markAcked();
        }
    }
}
