/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.template;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.filter.ProducerFilter;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendOptions;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.transaction.TransactionExecutor;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamMQ 消息模板（核心生产者 API），对齐 RocketMQ {@code RocketMQTemplate} 体验。
 *
 * <p><b>API 收敛（0.1.0）：</b>每个发送模式只保留一个以 {@link SendOptions} 为参数的规范形；此前的伸缩重载（timeout-only /
 * timeout+retry / callback+timeout / batch+timeout+retry）已全部移除，统一由 {@link SendOptions} 表达。零参便捷形式以
 * {@code default} 方法提供，不增加实现方负担。
 *
 * <p>核心发送语义：
 *
 * <ul>
 *   <li>{@link #syncSend(Message, SendOptions)} - 同步发送，等待 Redis 返回
 *   <li>{@link #asyncSend(Message, SendOptions)} - 异步发送，返回 {@link CompletableFuture}； 回调形式为派生默认方法
 *   <li>{@link #sendOneway(Message)} - 单向发送，不等待响应，性能最高
 *   <li>{@link #syncSendBatch(BatchMessage, SendOptions)} - 批量发送，基于 RBatch Pipeline
 *   <li>{@link #executeInTransaction} - 事务消息，半消息 + 本地事务（继承自 {@link TransactionExecutor}）
 * </ul>
 *
 * <p><b>可靠性保证模型：</b>
 *
 * <ul>
 *   <li>{@code syncSend}：等待 Redis XADD 命令返回，确认消息已写入 Stream。 持久化级别取决于 Redis AOF 配置（{@code
 *       appendfsync everysec} 默认每秒刷盘）
 *   <li>{@code asyncSend}：通过 {@link CompletableFuture} 异步获取 XADD 结果，语义与 syncSend 相同
 *   <li>{@code sendOneway}：Fire-and-forget，不等待 Redis 响应，不保证消息一定写入
 *   <li>{@code syncSendBatch}：基于 Pipeline 批量发送，Pipeline 本身失败会抛异常，单条失败独立标识
 *   <li>{@code executeInTransaction}：完整半消息流程通过 TransactionScanner 回查兜底
 * </ul>
 *
 * <p><b>泛型设计</b>：泛型参数 {@code <T>} 声明在方法级别而非类级别。一个 Template 单例 可发送不同 body 类型的消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageTemplate extends TransactionExecutor {

    /** 默认发送超时（毫秒） */
    long DEFAULT_SEND_TIMEOUT_MILLIS = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** 默认同步发送重试次数 */
    int DEFAULT_SYNC_RETRY_TIMES = StreamMQConstants.DEFAULT_SYNC_RETRY_TIMES;

    /**
     * 同步发送（规范形）。
     *
     * @param message 消息
     * @param options 发送选项（超时、重试等），null 时按 {@link SendOptions#defaults()} 处理
     * @param <T> body 类型
     * @return 发送结果
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    <T> SendResult syncSend(Message<T> message, SendOptions options);

    /**
     * 同步发送（默认超时与重试次数）。便捷默认方法。
     *
     * @param message 消息
     * @param <T> body 类型
     * @return 发送结果
     */
    default <T> SendResult syncSend(Message<T> message) {
        return syncSend(message, SendOptions.defaults());
    }

    /**
     * 异步发送（规范形），返回 {@link CompletableFuture}。
     *
     * @param message 消息
     * @param options 发送选项，null 时按 {@link SendOptions#defaults()} 处理
     * @param <T> body 类型
     * @return 异步结果
     */
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options);

    /** 异步发送（默认参数）。便捷默认方法。 */
    default <T> CompletableFuture<SendResult> asyncSend(Message<T> message) {
        return asyncSend(message, SendOptions.defaults());
    }

    /**
     * 异步发送（回调通知）。由 {@link #asyncSend(Message, SendOptions)} 派生的默认方法， 回调在完成线程上触发；{@code onSuccess}
     * 收到与 Future 相同的 {@link SendResult}， 异常被包装为 {@link
     * io.github.streammq.core.exception.StreamMQException} 后交给 {@code onException}。
     *
     * <p>用户回调抛出的异常会被捕获并记录 WARN 日志（含消息 ID），不会向上传播中断完成线程。
     *
     * @param message 消息
     * @param options 发送选项，null 时按默认值处理
     * @param callback 回调
     * @param <T> body 类型
     */
    default <T> void asyncSend(Message<T> message, SendOptions options, SendCallback callback) {
        asyncSend(message, options)
                .whenComplete(
                        (result, ex) -> {
                            try {
                                if (Objects.isNull(ex)) {
                                    callback.onSuccess(result);
                                } else {
                                    callback.onException(
                                            ex instanceof RuntimeException re
                                                    ? re
                                                    : new io.github.streammq.core.exception
                                                            .StreamMQException(
                                                            "async send failed", ex));
                                }
                            } catch (Throwable dispatchError) {
                                LoggerFactory.getLogger(StreamMessageTemplate.class)
                                        .warn(
                                                "async send callback threw exception: topic={},"
                                                        + " messageId={}",
                                                message.getTopic(),
                                                Objects.nonNull(result)
                                                        ? result.getMessageId()
                                                        : "unknown",
                                                dispatchError);
                            }
                        });
    }

    /**
     * 异步发送（回调通知，默认参数）。由 {@link #asyncSend(Message, SendOptions, SendCallback)} 派生的便捷默认方法，对齐
     * RocketMQ {@code asyncSend(msg, callback)} 习惯。
     *
     * @param message 消息
     * @param callback 回调
     * @param <T> body 类型
     */
    default <T> void asyncSend(Message<T> message, SendCallback callback) {
        asyncSend(message, SendOptions.defaults(), callback);
    }

    /**
     * 单向发送：不等待响应，不抛异常（fire-and-forget）。
     *
     * @param message 消息
     * @param <T> body 类型
     */
    <T> void sendOneway(Message<T> message);

    /**
     * 批量发送（规范形）：Pipeline 一次性投递，结果与输入顺序一一对应并携带真实 Entry ID； Pipeline 本身异常时抛出 {@link
     * io.github.streammq.core.exception.StreamMQException}。 重试语义为 at-least-once（可能重复投递，消费端需幂等）。
     *
     * @param batch 批量消息
     * @param options 发送选项（超时、整体重试次数），null 时按默认值处理
     * @param <T> body 类型
     * @return 每条消息的发送结果（与输入顺序一致）
     * @throws IllegalArgumentException 如果 batch 为空
     */
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch, SendOptions options);

    /** 批量发送（默认参数）。便捷默认方法。 */
    default <T> List<SendResult> syncSendBatch(BatchMessage<T> batch) {
        return syncSendBatch(batch, SendOptions.defaults());
    }

    /**
     * 返回消息转换器。
     *
     * @return 消息转换器
     */
    MessageConverter getMessageConverter();

    /**
     * 返回生产者拦截器链（不可修改）。
     *
     * @return 拦截器列表
     */
    List<ProducerInterceptor> getProducerInterceptors();

    /**
     * 设置生产者拦截器链（覆盖现有）。
     *
     * @param interceptors 拦截器列表
     */
    void setProducerInterceptors(List<ProducerInterceptor> interceptors);

    /**
     * 添加单个生产者拦截器。
     *
     * @param interceptor 拦截器
     */
    void addProducerInterceptor(ProducerInterceptor interceptor);

    /**
     * 添加单个生产者过滤器（发送前过滤）。
     *
     * @param filter 过滤器
     */
    void addProducerFilter(ProducerFilter filter);
}
