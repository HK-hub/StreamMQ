/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.producer;

import io.github.streammq.core.exception.ProducerTimeoutException;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * StreamMQ 生产者接口（底层抽象）。
 *
 * <p>提供原始的发送 API，{@link StreamMessageTemplate} 在此基础上做业务友好封装。 实现类位于 {@code
 * streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageProducer {

    /**
     * 同步发送单条消息。
     *
     * @param message 消息
     * @return 发送结果
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    SendResult syncSend(Message<?> message);

    /**
     * 同步发送单条消息（带超时）。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数
     * @return 发送结果
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 超时
     */
    SendResult syncSend(Message<?> message, long timeoutMillis);

    /**
     * 异步发送单条消息。
     *
     * @param message 消息
     * @return 异步结果
     */
    CompletableFuture<SendResult> asyncSend(Message<?> message);

    /**
     * 异步发送单条消息（带超时）。
     *
     * <p>默认实现委托 {@link #asyncSend(Message)} 并施加真实超时控制：超时后返回的 Future 以 {@link
     * ProducerTimeoutException} 异常完成（不阻塞调用线程）；其余失败原样透传。适配层可覆盖以获得更精确的底层超时语义。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数（必须 &gt; 0）
     * @return 带超时约束的异步结果；超时以 {@link ProducerTimeoutException} 异常完成
     * @throws IllegalArgumentException 如果 {@code timeoutMillis <= 0}
     */
    default CompletableFuture<SendResult> asyncSend(Message<?> message, long timeoutMillis) {
        Objects.requireNonNull(message, "message");
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive: " + timeoutMillis);
        }
        CompletableFuture<SendResult> deadlineFuture = new CompletableFuture<>();
        deadlineFuture.orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        asyncSend(message)
                .whenComplete(
                        (result, error) -> {
                            if (Objects.isNull(error)) {
                                deadlineFuture.complete(result);
                            } else {
                                deadlineFuture.completeExceptionally(error);
                            }
                        });
        return deadlineFuture.handle(
                (result, error) -> {
                    if (Objects.isNull(error)) {
                        return result;
                    }
                    Throwable cause =
                            error instanceof CompletionException
                                            && Objects.nonNull(error.getCause())
                                    ? error.getCause()
                                    : error;
                    if (cause instanceof TimeoutException timeout) {
                        throw new ProducerTimeoutException(
                                "asyncSend timed out after "
                                        + timeoutMillis
                                        + "ms for topic "
                                        + message.getTopic(),
                                message.getTopic(),
                                timeoutMillis,
                                timeout);
                    }
                    if (cause instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new StreamMQException("async send failed", cause);
                });
    }

    /**
     * 单向发送：不等待响应，不抛异常，性能最高。
     *
     * @param message 消息
     */
    void sendOneway(Message<?> message);

    /**
     * 批量发送（基于 RBatch / Pipeline）。
     *
     * @param messages 消息列表（必须同 Topic）
     * @return 每条消息的发送结果
     * @throws IllegalArgumentException 如果消息列表为空或 Topic 不一致
     */
    List<SendResult> syncSendBatch(List<? extends Message<?>> messages);

    /**
     * 批量发送（基于 RBatch / Pipeline），支持指定超时。
     *
     * <p>默认实现按「整体截止时间 + 逐条剩余预算」顺序发送：每条消息委托 {@link #syncSend(Message, long)}
     * 并传入从总超时中扣除已耗时间的剩余预算，预算耗尽时抛出 {@link ProducerTimeoutException}。 适配层可覆盖以提供真正的 Pipeline 批量语义。
     *
     * @param messages 消息列表（必须同 Topic）
     * @param timeoutMillis 整批超时毫秒数（必须 &gt; 0）
     * @return 每条消息的发送结果
     * @throws IllegalArgumentException 如果消息列表为空、Topic 不一致或超时非法
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 超过整批截止时间
     */
    default List<SendResult> syncSendBatch(
            List<? extends Message<?>> messages, long timeoutMillis) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("messages list is empty");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive: " + timeoutMillis);
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        List<SendResult> results = new ArrayList<>(messages.size());
        for (Message<?> message : messages) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            if (remainingMillis <= 0) {
                throw new ProducerTimeoutException(
                        "syncSendBatch timed out after "
                                + timeoutMillis
                                + "ms for topic "
                                + message.getTopic(),
                        message.getTopic(),
                        timeoutMillis);
            }
            results.add(syncSend(message, remainingMillis));
        }
        return results;
    }

    /** 关闭生产者，释放资源。 */
    void close();
}
