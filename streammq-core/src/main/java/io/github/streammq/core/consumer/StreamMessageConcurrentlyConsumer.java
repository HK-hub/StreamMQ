package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;

/**
 * 并发消费回调接口（返回值驱动 ACK / 重试，对齐 RocketMQ {@code MessageListenerConcurrently}）。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQConsumer} 注解即可注册为并发消费者。 Consumer 返回 {@link ConsumeAction}
 * 控制后续流程（唯一消费结果表达方式）：
 *
 * <ul>
 *   <li>{@link ConsumeAction#SUCCESS} - 自动 ACK
 *   <li>{@link ConsumeAction#RECONSUME_LATER} - 按 {@code RetryPolicy} 重试
 *   <li>{@code ConsumeAction.defer(Duration)} - 按指定延迟重试
 * </ul>
 *
 * <p>抛出 {@link RuntimeException} 等价于返回 {@link ConsumeAction#RECONSUME_LATER}。 框架不提供手动
 * ACK/nack/defer 调用，避免双模式冲突。
 *
 * <p>命名说明：实现 {@code onMessage} 的类是"消费者"（Consumer），负责业务处理； 而 {@link StreamMQListener} 是底层"监听器"，负责从
 * Redis Stream 拉取消息后交给本接口处理。
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
   * @param context 消费上下文（仅提供元数据，不提供手动 ACK）
   * @return 处理结果动作（{@link ConsumeAction#SUCCESS} / {@link ConsumeAction#RECONSUME_LATER} / {@code
   *     ConsumeAction.defer(Duration)}），返回 null 视为 RECONSUME_LATER
   * @throws Exception 业务异常，框架将其视为 {@link ConsumeAction#RECONSUME_LATER}
   */
  ConsumeAction onMessage(Message<T> message, ConsumeContext context) throws Exception;
}
