package io.github.streammq.tracing.model;

/**
 * 追踪事件类型，表示消息生命周期中的一个关键事件分类。
 *
 * <p>由 {@link TraceEvent} 引用，用于消息链路可视化与问题排查。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public enum TraceEventType {
  /** 消息发送事件（生产者投递到 Topic） */
  SEND,
  /** 消息投递事件（Broker 投递给消费者） */
  DELIVER,
  /** 消息消费事件（消费者处理） */
  CONSUME,
  /** 重试事件（消费失败后重新投递） */
  RETRY,
  /** 死信队列事件（超过最大重试次数进入 DLQ） */
  DLQ,
  /** 延时事件（延时消息调度触发） */
  DELAY
}
