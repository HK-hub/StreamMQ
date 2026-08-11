package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;

/**
 * 顺序消费回调接口（返回值驱动 ACK / 挂起，对齐 RocketMQ {@code MessageListenerOrderly}）。
 *
 * <p>实现此接口并在类上标注 {@link StreamMQConsumer}（{@code messageModel = ORDERLY}）注解即可注册为顺序消费者。 框架保证同一
 * {@code shardingKey} 的消息在单线程内串行消费。
 *
 * <p>返回值含义（唯一消费结果表达方式，与并发消费共用 {@link ConsumeAction}）：
 *
 * <ul>
 *   <li>{@link ConsumeAction#SUCCESS} - 消费成功，自动 ACK，下一条继续
 *   <li>{@link ConsumeAction#RECONSUME_LATER} - 暂停当前 shard 一小段时间（默认 10ms）， 消息留在 PEL
 *       等待重新消费同一消息（与并发消费的 RECONSUME_LATER 行为不同， 顺序消费不会将消息重新入队重试，而是暂停 shard 后原地重试）
 * </ul>
 *
 * <p>抛出 {@link RuntimeException} 等价于返回 {@link ConsumeAction#RECONSUME_LATER}。 框架不提供手动
 * ACK/nack/defer 调用，避免双模式冲突。
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
   * @return 处理结果动作，返回 null 视为 {@link ConsumeAction#RECONSUME_LATER}
   * @throws Exception 业务异常，框架将其视为 {@link ConsumeAction#RECONSUME_LATER}
   */
  ConsumeAction onMessage(Message<T> message, ConsumeOrderlyContext context) throws Exception;
}
