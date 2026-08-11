package io.github.streammq.core.service;

import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.transaction.TransactionCallback;

/**
 * 事务消息发送服务接口。
 *
 * <p>提供事务消息发送能力，支持半消息 + 本地事务 + 回查机制。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface TransactionMessageService {

  <T> SendResult sendTransaction(String topic, T body, TransactionCallback<T> callback);

  <T> SendResult sendTransaction(String topic, T body, String tag, TransactionCallback<T> callback);

  <T> SendResult sendTransaction(
      String topic, T body, MessageMetadataBuilder metadata, TransactionCallback<T> callback);
}
