package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.*;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;

/**
 * 重试与死信队列(DLQ)全流程 Redis 联动集成测试。
 *
 * <p>覆盖 Listener 失败 -> retry ZSet -> RetryScheduler 转投 -> DLQ Stream 全链路, 以及 DLQ 路由成功/失败对 PEL 的影响。
 */
@DisplayName("重试与死信队列集成测试")
class RetryAndDlqIT extends AbstractRedisIT {

  /**
   * 测试用快速重试策略,可配置固定延迟与最大重试次数。 当 {@code reconsumeTimes >= maxRetries} 时返回 {@code null},触发 DLQ 路由。
   */
  static class FastRetryPolicy implements RetryPolicy {
    private final long delayMs;
    private final int maxRetries;

    FastRetryPolicy(long delayMs, int maxRetries) {
      this.delayMs = delayMs;
      this.maxRetries = maxRetries;
    }

    @Override
    public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
      if (reconsumeTimes >= maxRetries) {
        return null;
      }
      return Duration.ofMillis(delayMs);
    }

    @Override
    public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
      return reconsumeTimes >= maxRetries;
    }
  }

  /** 通过动态代理构造 {@link StreamMQConsumer} 注解实例。 */
  @SuppressWarnings("unchecked")
  private static StreamMQConsumer mkAnnotation(String topic, String group, int maxReconsumeTimes) {
    return (StreamMQConsumer)
        Proxy.newProxyInstance(
            StreamMQConsumer.class.getClassLoader(),
            new Class<?>[] {StreamMQConsumer.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "topic" -> topic;
                  case "consumerGroup" -> group;
                  case "consumeMode" -> ConsumeMode.CLUSTERING;
                  case "messageModel" -> MessageModel.CONCURRENT;
                  case "maxReconsumeTimes" -> maxReconsumeTimes;
                  case "consumeThreadMin" -> 1;
                  case "consumeThreadMax" -> 64;
                  case "consumeTimeout" -> 30000L;
                  case "selectorExpression" -> "*";
                  case "serializer" -> MessageSerializer.class;
                  case "namespace" -> "";
                  case "enable" -> true;
                  case "selectorType" -> SelectorType.TAG;
                  case "pullBatchSize" -> 32;
                  case "retryPolicy" -> RetryPolicy.class;
                  case "enableMsgTrace" -> false;
                  case "streamMaxLen" -> 0;
                  case "messageConverter" -> MessageConverter.class;
                  case "rebalanceStrategy" -> RebalanceStrategy.class;
                  case "pullInterval" -> 0L;
                  case "suspendCurrentQueueTimeMillis" -> 1000L;
                  case "shardCount" -> 4;
                  case "consumerName" -> "";
                  case "annotationType" -> StreamMQConsumer.class;
                  case "hashCode" -> (topic + group).hashCode();
                  case "equals" -> args != null && args.length > 0 && proxy == args[0];
                  case "toString" ->
                      "@StreamMQConsumer(topic=" + topic + ", consumerGroup=" + group + ")";
                  default -> defaultAnnotationValue(method.getReturnType());
                });
  }

  /** 根据返回类型返回注解属性的默认值，避免新增注解属性时测试代理崩溃。 */
  private static Object defaultAnnotationValue(Class<?> returnType) {
    if (returnType == String.class) return "";
    if (returnType == int.class) return 0;
    if (returnType == long.class) return 0L;
    if (returnType == boolean.class) return false;
    if (returnType == Class.class) return null;
    if (returnType.isEnum()) return returnType.getEnumConstants()[0];
    return null;
  }

  @Test
  @DisplayName("消息消费失败:Listener 抛异常后消息进入 retry ZSet 且原消息已 ACK")
  void consumeFailure_messageEntersRetryZSet() {
    String topic = "retry-fail-topic";
    String group = "retry-fail-group";

    RetryPolicy fastPolicy = new FastRetryPolicy(100, 100);
    RedissonStreamListenerFactory consumerFactory =
        new RedissonStreamListenerFactory(redisson, converter);
    DefaultStreamMQListenerContainer container =
        new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, converter, fastPolicy, namespace);

    StreamMessageConcurrentlyConsumer<String> listener =
        (msg, ctx) -> {
          throw new RuntimeException("intentional failure");
        };
    container.registerConsumer(listener, mkAnnotation(topic, group, 16));
    createConsumerGroup(topic, group);
    container.start();

    try {
      RedissonStreamProducer producer =
          new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
      producer.syncSend(MessageBuilder.<String>withTopic(topic).body("retry-test").build());
      producer.close();

      String retryKey = StreamMQKeys.retryZSet(namespace, topic, group);
      await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
                assertThat(zset.size()).isEqualTo(1);
              });

      // 原消息应已 ACK(从 PEL 移除)
      RStream<String, String> stream =
          redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
      assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100))
          .isEmpty();
    } finally {
      container.stop();
    }
  }

  @Test
  @DisplayName("重试投递:retry ZSet 中的消息被重新投递到目标 Stream 并被消费")
  void retryRedelivery_messageReDeliveredAndConsumed() {
    String topic = "retry-redeliver-topic";
    String group = "retry-redeliver-group";

    RetryPolicy fastPolicy = new FastRetryPolicy(100, 100);
    RedissonStreamListenerFactory consumerFactory =
        new RedissonStreamListenerFactory(redisson, converter);
    DefaultStreamMQListenerContainer container =
        new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, converter, fastPolicy, namespace);

    AtomicInteger attempt = new AtomicInteger(0);
    StreamMessageConcurrentlyConsumer<String> listener =
        (msg, ctx) -> {
          if (attempt.incrementAndGet() == 1) {
            throw new RuntimeException("first attempt fails");
          }
          return ConsumeAction.SUCCESS;
        };
    container.registerConsumer(listener, mkAnnotation(topic, group, 16));

    RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10);
    container.registerRetryTargets(scheduler);
    createConsumerGroup(topic, group);
    scheduler.start();
    container.start();

    try {
      RedissonStreamProducer producer =
          new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
      producer.syncSend(MessageBuilder.<String>withTopic(topic).body("redeliver-body").build());
      producer.close();

      // 等待第二次消费成功(第一次失败 + 重试投递 + 第二次成功)
      await().atMost(15, TimeUnit.SECONDS).until(() -> attempt.get() >= 2);
      assertThat(attempt.get()).isEqualTo(2);

      // 最终成功后 PEL 应为空
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .isEmpty();
              });
    } finally {
      container.stop();
      scheduler.stop();
    }
  }

  @Test
  @DisplayName("达到最大重试次数后消息进入 DLQ Stream")
  void maxRetry_messageEntersDlq() {
    String topic = "retry-max-topic";
    String group = "retry-max-group";

    RetryPolicy fastPolicy = new FastRetryPolicy(100, 100);
    RedissonStreamListenerFactory consumerFactory =
        new RedissonStreamListenerFactory(redisson, converter);
    DefaultStreamMQListenerContainer container =
        new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, converter, fastPolicy, namespace);

    StreamMessageConcurrentlyConsumer<String> listener =
        (msg, ctx) -> {
          throw new RuntimeException("always fails");
        };
    // maxReconsumeTimes=2:RetryScheduler 在 retryCount>=2 时路由到 DLQ
    container.registerConsumer(listener, mkAnnotation(topic, group, 2));

    RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10);
    container.registerRetryTargets(scheduler);
    createConsumerGroup(topic, group);
    scheduler.start();
    container.start();

    try {
      RedissonStreamProducer producer =
          new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
      producer.syncSend(MessageBuilder.<String>withTopic(topic).body("dlq-bound").build());
      producer.close();

      String dlqKey = StreamMQKeys.dlqStream(namespace, group);
      await()
          .atMost(20, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                RStream<String, String> dlqStream = redisson.getStream(dlqKey);
                assertThat(dlqStream.size()).isEqualTo(1L);
              });
    } finally {
      container.stop();
      scheduler.stop();
    }
  }

  @Test
  @DisplayName("DLQ 路由成功后 ACK:消息从 PEL 移除")
  void dlqRoutingSuccess_pelEmpty() {
    String topic = "dlq-success-topic";
    String group = "dlq-success-group";

    // RetryPolicy 返回 null:container 直接路由到 DLQ
    RetryPolicy noRetryPolicy = new FastRetryPolicy(100, 0);
    RedissonStreamListenerFactory consumerFactory =
        new RedissonStreamListenerFactory(redisson, converter);
    DefaultStreamMQListenerContainer container =
        new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, converter, noRetryPolicy, namespace);

    StreamMessageConcurrentlyConsumer<String> listener =
        (msg, ctx) -> {
          throw new RuntimeException("trigger DLQ");
        };
    container.registerConsumer(listener, mkAnnotation(topic, group, 0));
    createConsumerGroup(topic, group);
    container.start();

    try {
      RedissonStreamProducer producer =
          new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
      producer.syncSend(MessageBuilder.<String>withTopic(topic).body("to-dlq").build());
      producer.close();

      String dlqKey = StreamMQKeys.dlqStream(namespace, group);
      await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                RStream<String, String> dlqStream = redisson.getStream(dlqKey);
                assertThat(dlqStream.size()).isEqualTo(1L);
              });

      // DLQ 路由成功后 ACK,PEL 应为空
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .isEmpty();
              });
    } finally {
      container.stop();
    }
  }

  @Test
  @DisplayName("DLQ 路由失败保留 PEL:模拟 DLQ 写入失败,消息仍在 PEL 中")
  @SuppressWarnings("unchecked")
  void dlqRoutingFailure_messageStaysInPel() {
    String topic = "dlq-fail-topic";
    String group = "dlq-fail-group";
    String dlqKey = StreamMQKeys.dlqStream(namespace, group);

    // 创建 spy 客户端:DLQ stream add 时抛异常
    RedissonClient spyClient = Mockito.spy(redisson);
    @SuppressWarnings("unchecked")
    RStream<String, String> failingStream = Mockito.mock(RStream.class);
    Mockito.doThrow(new RuntimeException("simulated DLQ write failure"))
        .when(failingStream)
        .add(Mockito.any(StreamAddArgs.class));
    Mockito.doReturn(failingStream).when(spyClient).getStream(dlqKey);

    RetryPolicy noRetryPolicy = new FastRetryPolicy(100, 0);
    RedissonStreamListenerFactory consumerFactory =
        new RedissonStreamListenerFactory(redisson, converter);
    DefaultStreamMQListenerContainer container =
        new DefaultStreamMQListenerContainer(
            spyClient, consumerFactory, converter, noRetryPolicy, namespace);

    StreamMessageConcurrentlyConsumer<String> listener =
        (msg, ctx) -> {
          throw new RuntimeException("trigger DLQ fail");
        };
    container.registerConsumer(listener, mkAnnotation(topic, group, 0));
    createConsumerGroup(topic, group);
    container.start();

    try {
      RedissonStreamProducer producer =
          new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
      producer.syncSend(MessageBuilder.<String>withTopic(topic).body("dlq-fail").build());
      producer.close();

      // DLQ 写入失败,消息应留在 PEL 中(未 ACK)
      await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                List<?> pending =
                    stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100);
                assertThat(pending).hasSize(1);
              });
    } finally {
      container.stop();
    }
  }

  @Test
  @DisplayName("retry payload Hash 包含 retryCount 与 targetTopic 元数据")
  void retryPayload_containsMetadata() {
    String topic = "retry-payload-topic";
    String group = "retry-payload-group";

    RetryPolicy fastPolicy = new FastRetryPolicy(100, 100);
    RedissonStreamListenerFactory consumerFactory =
        new RedissonStreamListenerFactory(redisson, converter);
    DefaultStreamMQListenerContainer container =
        new DefaultStreamMQListenerContainer(
            redisson, consumerFactory, converter, fastPolicy, namespace);

    StreamMessageConcurrentlyConsumer<String> listener =
        (msg, ctx) -> {
          throw new RuntimeException("fail");
        };
    container.registerConsumer(listener, mkAnnotation(topic, group, 16));
    createConsumerGroup(topic, group);
    container.start();

    try {
      RedissonStreamProducer producer =
          new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
      producer.syncSend(MessageBuilder.<String>withTopic(topic).body("payload-test").build());
      producer.close();

      // 等待 retry ZSet 有消息
      String retryKey = StreamMQKeys.retryZSet(namespace, topic, group);
      await()
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () -> {
                RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
                return zset.size() == 1;
              });

      // 读取 payload Hash,验证元数据
      RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
      String msgId = zset.iterator().next();
      String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
      RMap<String, String> payload = redisson.getMap(payloadKey);
      assertThat(payload).containsKey(RetryScheduler.FIELD_RETRY_COUNT);
      assertThat(payload).containsEntry(RetryScheduler.FIELD_TARGET_TOPIC, topic);
      assertThat(payload.get(RetryScheduler.FIELD_RETRY_COUNT)).isEqualTo("0");
    } finally {
      container.stop();
    }
  }
}
