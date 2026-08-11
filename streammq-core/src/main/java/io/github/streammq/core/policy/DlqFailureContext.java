package io.github.streammq.core.policy;

import java.util.Map;

/**
 * DLQ 消费失败上下文（只读），由框架在调用 {@link DlqFailureStrategy#decide} 时构造并注入。 提供当前死信消息的元数据，供策略决策参考。
 *
 * <p>关键信息：
 *
 * <ul>
 *   <li>{@link #dlqAttempts()} - 本次 DLQ 消费已重试的次数（初次失败为 0）
 *   <li>{@link #maxDlqRetryAttempts()} - 配置的最大 DLQ 重试次数
 *   <li>{@link #dlqReason()} - 消息进入 DLQ 的原因（如 "maxRetry"）
 *   <li>{@link #originalTopic()} - 消息的原始 topic
 *   <li>{@link #originalMessageId()} - 原始 Stream Entry ID
 *   <li>{@link #lastFailureCause()} - 最近一次消费失败原因（可为 null）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface DlqFailureContext {

  /** 当前消息的 DLQ 消费失败次数（初次为 0，每次重试后递增） */
  int dlqAttempts();

  /** 配置的最大 DLQ 重试次数 */
  int maxDlqRetryAttempts();

  /** 消息进入 DLQ 的原因 */
  String dlqReason();

  /** 消息的原始 topic */
  String originalTopic();

  /** 原始 Stream Entry ID（进入 DLQ 前的 ID） */
  String originalMessageId();

  /** 最近一次消费失败原因（消费者抛出的异常）；无异常时可为 null */
  Throwable lastFailureCause();

  /** 消息在 DLQ Stream 中的原始字段（供高级策略检查消息内容后决策） */
  Map<String, String> dlqFields();

  /** DLQ 重试延迟配置（毫秒） */
  long dlqRetryDelayMs();
}
