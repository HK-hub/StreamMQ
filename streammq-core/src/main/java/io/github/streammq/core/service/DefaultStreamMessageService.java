package io.github.streammq.core.service;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.*;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;

/**
 * {@link StreamMessageService} 的默认实现，封装 {@link StreamMessageTemplate} 提供更简洁的 API。
 *
 * <p>用户无需手动构造 {@link Message} 对象，只需传入 body 和 topic 即可发送。 类似 RocketMQ 的 DefaultMQPushProducer 封装层。
 *
 * <p><b>泛型设计</b>：所有方法均使用方法级泛型 {@code <T>}，支持同一 Service 实例 发送不同 body 类型的消息，无需泛型强转。
 *
 * <p>遵循「依赖接口而非实现」原则，业务代码应注入 {@link StreamMessageService}， 由 Spring 自动装配本默认实现。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 * @see StreamMessageService
 */
@RequiredArgsConstructor
public class DefaultStreamMessageService implements StreamMessageService {

  private final StreamMessageTemplate template;

  // ===================== 同步发送（Message 对象） =====================

  @Override
  public <T> SendResult send(Message<T> message) {
    return template.syncSend(message);
  }

  @Override
  public <T> SendResult send(Message<T> message, long timeoutMillis) {
    return template.syncSend(message, timeoutMillis);
  }

  @Override
  public <T> SendResult send(Message<T> message, long timeoutMillis, int retryTimes) {
    return template.syncSend(message, timeoutMillis, retryTimes);
  }

  // ===================== 同步发送（topic + body） =====================

  @Override
  public <T> SendResult send(String topic, T body) {
    return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).build());
  }

  @Override
  public <T> SendResult send(String topic, T body, String tag) {
    return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
  }

  @Override
  public <T> SendResult send(String topic, T body, String tag, String keys) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).keys(keys).build());
  }

  @Override
  public <T> SendResult send(String topic, T body, String tag, String keys, String shardingKey) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic)
            .body(body)
            .tag(tag)
            .keys(keys)
            .shardingKey(shardingKey)
            .build());
  }

  // ===================== 同步发送（带超时） =====================

  @Override
  public <T> SendResult send(String topic, T body, long timeoutMillis) {
    return template.syncSend(MessageBuilder.<T>withTopic(topic).body(body).build(), timeoutMillis);
  }

  @Override
  public <T> SendResult send(String topic, T body, String tag, long timeoutMillis) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), timeoutMillis);
  }

  @Override
  public <T> SendResult send(String topic, T body, String tag, String keys, long timeoutMillis) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).keys(keys).build(), timeoutMillis);
  }

  @Override
  public <T> SendResult send(
      String topic, T body, String tag, String keys, String shardingKey, long timeoutMillis) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic)
            .body(body)
            .tag(tag)
            .keys(keys)
            .shardingKey(shardingKey)
            .build(),
        timeoutMillis);
  }

  // ===================== 同步发送（带超时和重试） =====================

  @Override
  public <T> SendResult send(String topic, T body, long timeoutMillis, int retryTimes) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).build(), timeoutMillis, retryTimes);
  }

  @Override
  public <T> SendResult send(String topic, T body, String tag, long timeoutMillis, int retryTimes) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), timeoutMillis, retryTimes);
  }

  @Override
  public <T> SendResult send(
      String topic, T body, String tag, String keys, long timeoutMillis, int retryTimes) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).keys(keys).build(),
        timeoutMillis,
        retryTimes);
  }

  @Override
  public <T> SendResult send(
      String topic,
      T body,
      String tag,
      String keys,
      String shardingKey,
      long timeoutMillis,
      int retryTimes) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic)
            .body(body)
            .tag(tag)
            .keys(keys)
            .shardingKey(shardingKey)
            .build(),
        timeoutMillis,
        retryTimes);
  }

  // ===================== 同步发送（MessageMetadataBuilder 模式） =====================

  @Override
  public <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.syncSend(builder.build());
  }

  @Override
  public <T> SendResult send(
      String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.syncSend(builder.build(), timeoutMillis);
  }

  @Override
  public <T> SendResult send(
      String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis, int retryTimes) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.syncSend(builder.build(), timeoutMillis, retryTimes);
  }

  // ===================== 异步发送（返回 CompletableFuture） =====================

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(Message<T> message) {
    return template.asyncSend(message);
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(String topic, T body) {
    return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build());
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, String tag) {
    return template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(
      String topic, T body, String tag, String keys) {
    return template.asyncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).keys(keys).build());
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(
      String topic, T body, String tag, String keys, String shardingKey) {
    return template.asyncSend(
        MessageBuilder.<T>withTopic(topic)
            .body(body)
            .tag(tag)
            .keys(keys)
            .shardingKey(shardingKey)
            .build());
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(String topic, T body, long timeoutMillis) {
    return template
        .asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build())
        .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(
      String topic, T body, MessageMetadataBuilder metadata) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.asyncSend(builder.build());
  }

  @Override
  public <T> CompletableFuture<SendResult> asyncSend(
      String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.asyncSend(builder.build()).orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  // ===================== 异步发送（回调通知） =====================

  @Override
  public <T> void asyncSend(String topic, T body, SendCallback callback) {
    template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).build(), callback);
  }

  @Override
  public <T> void asyncSend(String topic, T body, String tag, SendCallback callback) {
    template.asyncSend(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), callback);
  }

  @Override
  public <T> void asyncSend(String topic, T body, SendCallback callback, long timeoutMillis) {
    template.asyncSend(
        MessageBuilder.<T>withTopic(topic).body(body).build(), callback, timeoutMillis);
  }

  @Override
  public <T> void asyncSend(
      String topic, T body, MessageMetadataBuilder metadata, SendCallback callback) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    template.asyncSend(builder.build(), callback);
  }

  @Override
  public <T> void asyncSend(
      String topic,
      T body,
      MessageMetadataBuilder metadata,
      SendCallback callback,
      long timeoutMillis) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    template.asyncSend(builder.build(), callback, timeoutMillis);
  }

  // ===================== 单向发送 =====================

  @Override
  public <T> void sendOneway(Message<T> message) {
    template.sendOneway(message);
  }

  @Override
  public <T> void sendOneway(String topic, T body) {
    template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body).build());
  }

  @Override
  public <T> void sendOneway(String topic, T body, String tag) {
    template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build());
  }

  @Override
  public <T> void sendOneway(String topic, T body, String tag, String keys) {
    template.sendOneway(MessageBuilder.<T>withTopic(topic).body(body).tag(tag).keys(keys).build());
  }

  @Override
  public <T> void sendOneway(String topic, T body, String tag, String keys, String shardingKey) {
    template.sendOneway(
        MessageBuilder.<T>withTopic(topic)
            .body(body)
            .tag(tag)
            .keys(keys)
            .shardingKey(shardingKey)
            .build());
  }

  @Override
  public <T> void sendOneway(String topic, T body, MessageMetadataBuilder metadata) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    template.sendOneway(builder.build());
  }

  // ===================== 批量发送 =====================

  @Override
  public <T> List<SendResult> sendBatch(BatchMessage<T> batch) {
    return template.syncSendBatch(batch);
  }

  @Override
  public <T> List<SendResult> sendBatch(String topic, List<T> bodies) {
    Objects.requireNonNull(bodies, "bodies");
    if (bodies.isEmpty()) {
      throw new IllegalArgumentException("bodies list is empty");
    }
    List<Message<T>> messages =
        bodies.stream().map(body -> MessageBuilder.<T>withTopic(topic).body(body).build()).toList();
    BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
    return template.syncSendBatch(batch);
  }

  @Override
  public <T> List<SendResult> sendBatch(String topic, String tag, List<T> bodies) {
    Objects.requireNonNull(bodies, "bodies");
    if (bodies.isEmpty()) {
      throw new IllegalArgumentException("bodies list is empty");
    }
    List<Message<T>> messages =
        bodies.stream()
            .map(body -> MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build())
            .toList();
    BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
    return template.syncSendBatch(batch);
  }

  @Override
  public <T> List<SendResult> sendBatch(
      String topic, List<T> bodies, MessageMetadataBuilder metadata) {
    Objects.requireNonNull(bodies, "bodies");
    if (bodies.isEmpty()) {
      throw new IllegalArgumentException("bodies list is empty");
    }
    List<Message<T>> messages =
        bodies.stream()
            .map(
                body -> {
                  MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
                  if (Objects.nonNull(metadata)) {
                    metadata.applyTo(builder);
                  }
                  return builder.build();
                })
            .toList();
    BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
    return template.syncSendBatch(batch);
  }

  @Override
  public <T> List<SendResult> sendBatch(List<Message<T>> messages) {
    Objects.requireNonNull(messages, "messages");
    if (messages.isEmpty()) {
      throw new IllegalArgumentException("messages list is empty");
    }
    String topic = messages.get(0).getTopic();
    BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(messages).build();
    return template.syncSendBatch(batch);
  }

  @Override
  public <T> List<SendResult> sendBatch(
      List<Message<T>> messages, long timeoutMillis, int retryTimes) {
    // 当前 template.syncSendBatch 不支持超时/重试参数，这里预留 API，
    // 实际通过 template 的默认行为发送；未来 template 扩展后可直接传递参数。
    return sendBatch(messages);
  }

  @Override
  @SafeVarargs
  public final <T> List<SendResult> sendBatch(Message<T>... messages) {
    Objects.requireNonNull(messages, "messages");
    if (messages.length == 0) {
      throw new IllegalArgumentException("messages array is empty");
    }
    return sendBatch(List.of(messages));
  }

  @Override
  @SafeVarargs
  public final <T> List<SendResult> sendBatch(String topic, Message<T>... messages) {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(messages, "messages");
    if (messages.length == 0) {
      throw new IllegalArgumentException("messages array is empty");
    }
    List<Message<T>> normalized = new ArrayList<>(messages.length);
    for (Message<T> msg : messages) {
      normalized.add(normalizeTopic(msg, topic));
    }
    BatchMessage<T> batch = BatchMessage.<T>withTopic(topic).addAll(normalized).build();
    return template.syncSendBatch(batch);
  }

  @Override
  @SafeVarargs
  public final <T> List<SendResult> sendBatch(
      String topic, long timeoutMillis, int retryTimes, Message<T>... messages) {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(messages, "messages");
    if (messages.length == 0) {
      throw new IllegalArgumentException("messages array is empty");
    }
    // 当前 template.syncSendBatch 不支持超时/重试参数，预留 API
    return sendBatch(topic, messages);
  }

  /**
   * 规范化消息的 Topic（如果不一致则重新构造，不修改原始消息）。
   *
   * @param message 原始消息
   * @param topic 目标 Topic
   * @param <T> body 类型
   * @return Topic 已规范化的消息
   */
  private <T> Message<T> normalizeTopic(Message<T> message, String topic) {
    Objects.requireNonNull(message, "message");
    if (topic.equals(message.getTopic())) {
      return message;
    }
    // 使用 MessageBuilder 重新构造消息，覆盖 Topic，保留其他所有字段
    MessageBuilder<T> builder =
        MessageBuilder.<T>withTopic(topic)
            .body(message.getBody())
            .tag(message.getTag())
            .keys(message.getKeys())
            .shardingKey(message.getShardingKey());
    if (Objects.nonNull(message.getDelayLevel())) {
      builder.delayLevel(message.getDelayLevel());
    }
    if (Objects.nonNull(message.getDelayTimeMillis())) {
      builder.delayTimeMillis(message.getDelayTimeMillis());
    }
    if (message.getBornTimestamp() > 0) {
      builder.bornTimestamp(message.getBornTimestamp());
    }
    if (Objects.nonNull(message.getBornHost())) {
      builder.bornHost(message.getBornHost());
    }
    if (Objects.nonNull(message.getTransactionId())) {
      builder.transactionId(message.getTransactionId());
    }
    Map<String, String> props = message.getProperties();
    if (!props.isEmpty()) {
      builder.properties(props);
    }
    Map<String, String> userProps = message.getUserProperties();
    for (Map.Entry<String, String> entry : userProps.entrySet()) {
      builder.withUserProperty(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  // ===================== 延时消息 =====================

  @Override
  public <T> SendResult sendDelay(String topic, T body, DelayLevel delayLevel) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).delayLevel(delayLevel).build());
  }

  @Override
  public <T> SendResult sendDelay(String topic, T body, long delayTimeMillis) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).delayTimeMillis(delayTimeMillis).build());
  }

  @Override
  public <T> SendResult sendDelay(String topic, T body, String tag, DelayLevel delayLevel) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).delayLevel(delayLevel).build());
  }

  @Override
  public <T> SendResult sendDelay(String topic, T body, String tag, long delayTimeMillis) {
    return template.syncSend(
        MessageBuilder.<T>withTopic(topic)
            .body(body)
            .tag(tag)
            .delayTimeMillis(delayTimeMillis)
            .build());
  }

  @Override
  public <T> SendResult sendDelay(String topic, T body, MessageMetadataBuilder metadata) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.syncSend(builder.build());
  }

  @Override
  public <T> SendResult sendDelay(
      String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.syncSend(builder.build(), timeoutMillis);
  }

  // ===================== 事务消息 =====================

  @Override
  public <T> SendResult sendTransaction(String topic, T body, TransactionCallback<T> callback) {
    return template.executeInTransaction(
        MessageBuilder.<T>withTopic(topic).body(body).build(), callback);
  }

  @Override
  public <T> SendResult sendTransaction(
      String topic, T body, String tag, TransactionCallback<T> callback) {
    return template.executeInTransaction(
        MessageBuilder.<T>withTopic(topic).body(body).tag(tag).build(), callback);
  }

  @Override
  public <T> SendResult sendTransaction(
      String topic, T body, MessageMetadataBuilder metadata, TransactionCallback<T> callback) {
    MessageBuilder<T> builder = MessageBuilder.<T>withTopic(topic).body(body);
    if (Objects.nonNull(metadata)) {
      metadata.applyTo(builder);
    }
    return template.executeInTransaction(builder.build(), callback);
  }
}
