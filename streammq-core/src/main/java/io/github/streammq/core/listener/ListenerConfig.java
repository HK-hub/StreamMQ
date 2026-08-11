package io.github.streammq.core.listener;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * 监听器配置，替代弱类型 {@code Map<String, Object>} 的强类型值对象。
 *
 * <p>用于 {@link StreamMQListenerFactory#createListener(ListenerConfig)} 创建底层 {@link
 * StreamMQListener} 实例。Listener 负责从 Redis Stream 拉取消息， 然后交给业务层 {@link
 * StreamMessageConcurrentlyConsumer} 处理。
 *
 * <p>使用 Builder 模式构造：
 *
 * <pre>{@code
 * ListenerConfig config = ListenerConfig.builder()
 *     .topic("my-topic")
 *     .consumerGroup("my-group")
 *     .consumerName("consumer-1")
 *     .namespace("ns")
 *     .build();
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@Builder
public class ListenerConfig {

  /** 主题（必填） */
  @NonNull private final String topic;

  /** 消费者组名（必填） */
  @NonNull private final String consumerGroup;

  /** 消费者实例名（可选，默认自动生成 UUID 后缀） */
  private final String consumerName;

  /** 命名空间（可选，默认空字符串） */
  @Builder.Default private final String namespace = "";

  /** 每次拉取批量大小（可选，默认 {@link StreamMQConstants#DEFAULT_CONSUME_BATCH_SIZE}） */
  @Builder.Default private final int pullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;

  /** 阻塞拉取超时毫秒（可选，默认 {@link StreamMQConstants#DEFAULT_PULL_BLOCK_TIMEOUT_MS}） */
  @Builder.Default
  private final long pullBlockTimeoutMillis = StreamMQConstants.DEFAULT_PULL_BLOCK_TIMEOUT_MS;

  /** 拉取间隔毫秒（可选，默认 0 表示不等待） */
  @Builder.Default
  private final long pullIntervalMillis = StreamMQConstants.DEFAULT_PULL_INTERVAL_MS;

  /** 序列化器类（可选，为 null 表示使用全局配置） */
  private final Class<? extends MessageSerializer> serializer;

  /**
   * 每个消费者专属消息转换器实例（可选，为 null 表示使用工厂全局转换器）。
   *
   * <p>由容器在注册时按注解 {@code messageConverter} / {@code serializer} 实例化， 传入工厂创建的 Listener 使用此转换器解码消息，实现
   * per-consumer 序列化策略。
   *
   * @return 消息转换器实例，可为 null
   */
  private final MessageConverter converter;

  /**
   * DLQ 模式标志（可选，默认 false）。 设置为 true 时，监听器从 DLQ Stream 消费死信消息， Stream Key 使用 {@code
   * streammq:{ns}:dlq:{consumerGroup}}（对齐 RocketMQ %DLQ%{group}）。
   *
   * @return true 表示 DLQ 模式
   */
  @Builder.Default private final boolean dlqMode = false;

  /**
   * Retry 模式标志（可选，默认 false）。 设置为 true 时，监听器从 retry Stream 消费重试消息， Stream Key 使用 {@code
   * streammq:{ns}:retry:msg:{topic}:{consumerGroup}}（对齐 RocketMQ %RETRY%{group}%）。
   *
   * @return true 表示 retry 模式
   */
  @Builder.Default private final boolean retryMode = false;

  /**
   * 目标 body 类型（跨平台反序列化回退类型）。
   *
   * <p>当 Stream Entry 中不含 {@code bodyType} 字段（发送方非 StreamMQ SDK，如 Go/Python 直接写 Redis Stream）， 或
   * {@code bodyType} 指向的类在消费端不可加载时，使用此类型作为反序列化目标类型。
   *
   * <p>通常由容器在注册 Consumer 时通过 {@code io.github.streammq.core.util.BodyTypeResolver} 解析 {@code
   * StreamMessageConcurrentlyConsumer<T>} 的泛型 T 自动填入，无需用户手动配置。
   *
   * <p>若此字段为 null 且 Stream Entry 缺失 {@code bodyType}，最终回退为 {@code String.class}，
   * 由消费者自行将字符串反序列化为目标类型（跨语言场景的推荐用法）。
   *
   * @return 目标 body 类型，可为 null
   */
  private final Class<?> targetBodyType;

  /**
   * 广播消费模式标志（可选，默认 false）。 设置为 true 时，每个消费者实例使用独立的消费者组名， 使得同一 Topic 的每条消息被所有消费者实例各自处理一次。
   *
   * @return true 表示广播模式
   */
  @Builder.Default private final boolean broadcast = false;

  @lombok.Builder.Default
  private final int maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

  @lombok.Builder.Default
  private final long consumeTimeoutMillis = StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;

  @lombok.Builder.Default private final int consumeThreadMin = 1;

  @lombok.Builder.Default
  private final int consumeThreadMax = StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX;

  @lombok.Builder.Default
  private final long suspendCurrentQueueTimeMillis =
      StreamMQConstants.DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MS;

  @lombok.Builder.Default private final int shardCount = StreamMQConstants.DEFAULT_SHARD_COUNT;

  @lombok.Builder.Default private final int streamMaxLen = StreamMQConstants.DEFAULT_STREAM_MAX_LEN;

  @lombok.Builder.Default private final boolean enableMsgTrace = false;

  /**
   * Builder 自定义验证。
   *
   * <p>验证规则：
   *
   * <ul>
   *   <li>topic 不能为空
   *   <li>consumerGroup 不能为空
   *   <li>pullBatchSize 必须 > 0
   *   <li>consumeThreadMin 必须 >= 1
   *   <li>consumeThreadMax 必须 >= consumeThreadMin
   *   <li>maxReconsumeTimes 必须 >= 0
   *   <li>shardCount 必须 >= 1（顺序消费时）
   * </ul>
   */
  @lombok.Builder
  public ListenerConfig(
      String topic,
      String consumerGroup,
      String consumerName,
      String namespace,
      int pullBatchSize,
      long pullBlockTimeoutMillis,
      long pullIntervalMillis,
      Class<? extends MessageSerializer> serializer,
      MessageConverter converter,
      boolean dlqMode,
      boolean retryMode,
      Class<?> targetBodyType,
      boolean broadcast,
      int maxReconsumeTimes,
      long consumeTimeoutMillis,
      int consumeThreadMin,
      int consumeThreadMax,
      long suspendCurrentQueueTimeMillis,
      int shardCount,
      int streamMaxLen,
      boolean enableMsgTrace) {
    this.topic = Objects.requireNonNull(topic, "topic must not be null");
    this.consumerGroup = Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
    if (topic.trim().isEmpty()) {
      throw new IllegalArgumentException("topic must not be empty");
    }
    if (consumerGroup.trim().isEmpty()) {
      throw new IllegalArgumentException("consumerGroup must not be empty");
    }
    if (pullBatchSize <= 0) {
      throw new IllegalArgumentException("pullBatchSize must be > 0, got: " + pullBatchSize);
    }
    if (consumeThreadMin < 1) {
      throw new IllegalArgumentException("consumeThreadMin must be >= 1, got: " + consumeThreadMin);
    }
    if (consumeThreadMax < consumeThreadMin) {
      throw new IllegalArgumentException(
          "consumeThreadMax must be >= consumeThreadMin, got: max="
              + consumeThreadMax
              + ", min="
              + consumeThreadMin);
    }
    if (maxReconsumeTimes < 0) {
      throw new IllegalArgumentException(
          "maxReconsumeTimes must be >= 0, got: " + maxReconsumeTimes);
    }
    if (shardCount < 1) {
      throw new IllegalArgumentException("shardCount must be >= 1, got: " + shardCount);
    }
    if (consumeTimeoutMillis < 0) {
      throw new IllegalArgumentException(
          "consumeTimeoutMillis must be >= 0, got: " + consumeTimeoutMillis);
    }
    if (pullBlockTimeoutMillis < 0) {
      throw new IllegalArgumentException(
          "pullBlockTimeoutMillis must be >= 0, got: " + pullBlockTimeoutMillis);
    }
    if (pullIntervalMillis < 0) {
      throw new IllegalArgumentException(
          "pullIntervalMillis must be >= 0, got: " + pullIntervalMillis);
    }
    if (suspendCurrentQueueTimeMillis < 0) {
      throw new IllegalArgumentException(
          "suspendCurrentQueueTimeMillis must be >= 0, got: " + suspendCurrentQueueTimeMillis);
    }

    this.consumerName = consumerName;
    this.namespace = Objects.isNull(namespace) ? "" : namespace;
    this.pullBatchSize = pullBatchSize;
    this.pullBlockTimeoutMillis = pullBlockTimeoutMillis;
    this.pullIntervalMillis = pullIntervalMillis;
    this.serializer = serializer;
    this.converter = converter;
    this.dlqMode = dlqMode;
    this.retryMode = retryMode;
    this.targetBodyType = targetBodyType;
    this.broadcast = broadcast;
    this.maxReconsumeTimes = maxReconsumeTimes;
    this.consumeTimeoutMillis = consumeTimeoutMillis;
    this.consumeThreadMin = consumeThreadMin;
    this.consumeThreadMax = consumeThreadMax;
    this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    this.shardCount = shardCount;
    this.streamMaxLen = streamMaxLen;
    this.enableMsgTrace = enableMsgTrace;
  }
}
