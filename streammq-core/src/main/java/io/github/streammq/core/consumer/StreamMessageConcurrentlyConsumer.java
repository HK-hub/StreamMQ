package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;

/**
 * 自动 ACK 消费回调接口（并发消费）。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQConsumer} 注解即可注册为并发消费者。
 * Consumer 返回 {@link ConsumeAction} 控制后续流程：
 * <ul>
 *   <li>{@link ConsumeAction#SUCCESS} - 自动 ACK</li>
 *   <li>{@link ConsumeAction#RECONSUME_LATER} - 进入重试</li>
 * </ul>
 *
 * <p>抛出 {@link RuntimeException} 等价于返回 {@link ConsumeAction#RECONSUME_LATER}。
 *
 * <p>在 {@link AcknowledgeMode#MANUAL} 模式下，{@code onMessage} 的返回值被忽略，
 * 由 Consumer 通过 {@link ConsumeContext#acknowledge()} 显式控制 ACK。
 *
 * <p>命名说明：对齐 RocketMQ 的 {@code MessageListenerConcurrently}，
 * 实现 {@code onMessage} 的类是"消费者"（Consumer），负责业务处理；
 * 而 {@link StreamMQListener} 是底层"监听器"，
 * 负责从 Redis Stream 拉取消息后交给本接口的实现类处理。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageConcurrentlyConsumer<T> extends StreamMessageConsumer<T> {

    /**
     * 处理单条消息。
     *
     * @param message 消息载体
     * @param context 消费上下文
     * @return 处理结果动作
     * @throws Exception 业务异常，框架将其视为 {@link ConsumeAction#RECONSUME_LATER}
     */
    ConsumeAction onMessage(Message<T> message, ConsumeContext context) throws Exception;

    @Override
    default void consumeMessage(Message<T> message, ConsumeContext context) throws Exception {
        ConsumeAction action = onMessage(message, context);
        // AUTO 模式下：SUCCESS 标记 context 已 ACK；RECONSUME_LATER 不做标记（框架会处理重试）
        // MANUAL 模式下：onMessage 返回值被忽略，由 context.acknowledge() 控制；这里 markAcked 也不会重复 ACK
        if (action == ConsumeAction.SUCCESS) {
            context.markAcked();
        }
    }
}
