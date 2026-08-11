package io.github.streammq.core.enums;

/**
 * 消息模型：并发消息 vs 顺序消息。
 *
 * <p>决定 {@code @StreamMQListener} 的消费线程模型与 shard 路由策略：
 *
 * <ul>
 *   <li>{@link #CONCURRENT} - 默认，多线程并发消费，不保证顺序
 *   <li>{@link #ORDERLY} - 顺序消费，按 shardingKey 路由到固定 shard，shard 内单线程串行
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum MessageModel {

  /** 并发消息：多线程并发消费，最大化吞吐，不保证消息顺序。 适用于绝大多数业务场景。 */
  CONCURRENT,

  /** 顺序消息：同一 shardingKey 的消息路由到同一 shard，shard 内单线程串行消费。 适用于订单状态机、流程引擎等需要严格顺序的场景。 */
  ORDERLY
}
