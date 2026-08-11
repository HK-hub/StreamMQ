package io.github.streammq.core.service;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;

/**
 * 基础同步发送服务接口。
 *
 * <p>提供核心的同步消息发送能力，支持多种参数组合。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface BasicMessageService {

  <T> SendResult send(Message<T> message);

  <T> SendResult send(Message<T> message, long timeoutMillis);

  <T> SendResult send(Message<T> message, long timeoutMillis, int retryTimes);

  <T> SendResult send(String topic, T body);

  <T> SendResult send(String topic, T body, String tag);

  <T> SendResult send(String topic, T body, String tag, String keys);

  <T> SendResult send(String topic, T body, String tag, String keys, String shardingKey);

  <T> SendResult send(String topic, T body, long timeoutMillis);

  <T> SendResult send(String topic, T body, String tag, long timeoutMillis);

  <T> SendResult send(String topic, T body, String tag, String keys, long timeoutMillis);

  <T> SendResult send(
      String topic, T body, String tag, String keys, String shardingKey, long timeoutMillis);

  <T> SendResult send(String topic, T body, long timeoutMillis, int retryTimes);

  <T> SendResult send(String topic, T body, String tag, long timeoutMillis, int retryTimes);

  <T> SendResult send(
      String topic, T body, String tag, String keys, long timeoutMillis, int retryTimes);

  <T> SendResult send(
      String topic,
      T body,
      String tag,
      String keys,
      String shardingKey,
      long timeoutMillis,
      int retryTimes);

  <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata);

  <T> SendResult send(String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis);

  <T> SendResult send(
      String topic, T body, MessageMetadataBuilder metadata, long timeoutMillis, int retryTimes);
}
