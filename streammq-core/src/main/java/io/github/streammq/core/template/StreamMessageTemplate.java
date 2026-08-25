/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.template;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendOptions;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.transaction.TransactionExecutor;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * StreamMQ 消息模板（核心生产者 API），对齐 RocketMQ {@code RocketMQTemplate} 体验。
 *
 * <p>核心发送语义：
 *
 * <ul>
 *   <li>{@link #syncSend} - 同步发送，等待 Redis 返回
 *   <li>{@link #asyncSend} - 异步发送，返回 {@link CompletableFuture} 或回调 {@link SendCallback}
 *   <li>{@link #sendOneway} - 单向发送，不等待响应，性能最高
 *   <li>{@link #syncSendBatch} - 批量发送，基于 RBatch Pipeline
 *   <li>{@link #executeInTransaction} - 事务消息，半消息 + 本地事务
 * </ul>
 *
 * <p>拦截器链：所有 {@code syncSend} / {@code asyncSend} 调用前后均经过 {@link ProducerInterceptor} 链。
 *
 * <p><b>可靠性保证模型（05-1.3）：</b>
 *
 * <ul>
 *   <li>{@code syncSend}：等待 Redis XADD 命令返回，确认消息已写入 Stream 缓冲区。 持久化级别取决于 Redis AOF 配置（{@code
 *       appendfsync everysec} 默认每秒刷盘）
 *   <li>{@code asyncSend}：通过 {@link CompletableFuture} 异步获取 XADD 结果，语义与 syncSend 相同
 *   <li>{@code sendOneway}：Fire-and-forget，不等待 Redis 响应，不保证消息一定写入
 *   <li>{@code syncSendBatch}：基于 Pipeline 批量发送，Pipeline 本身失败会抛异常，单条失败独立标识
 *   <li>{@code executeInTransaction}：简化模式在当前线程执行本地事务，完整模式通过 TransactionScanner 回查
 * </ul>
 *
 * <p><b>不支持 Redis WAIT 命令</b>：当前实现不集成 WAIT 命令等待从节点确认。 如需更强持久化保证，请配置 Redis {@code appendfsync
 * always} 或自行集成 WAIT。
 *
 * <p><b>泛型设计</b>：泛型参数 {@code <T>} 声明在方法级别而非类级别。一个 Template 单例 可发送不同 body 类型的消息，无需为每种 body 类型创建独立的
 * Template 实例， 也避免了调用方繁琐的泛型强转。
 *
 * <p>实现类位于 {@code streammq-redisson-adapter} 模块（{@code DefaultStreamMessageTemplate}）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageTemplate extends TransactionExecutor {

    /** 默认发送超时（毫秒） */
    long DEFAULT_SEND_TIMEOUT_MILLIS = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** 默认同步发送重试次数 */
    int DEFAULT_SYNC_RETRY_TIMES = StreamMQConstants.DEFAULT_SYNC_RETRY_TIMES;

    /** 默认异步发送重试次数（不重试） */
    int DEFAULT_ASYNC_RETRY_TIMES = StreamMQConstants.DEFAULT_ASYNC_RETRY_TIMES;

    /**
     * 同步发送（默认超时、默认重试次数）。
     *
     * <p><b>消息大小限制（02-2.2）：</b>Redis Stream 单条消息最大 512MB， 但推荐不超过 1MB。超大消息会增加网络传输和内存压力。 序列化后的消息大小取决于
     * {@link io.github.streammq.core.serializer.MessageSerializer} 实现。
     *
     * @param message 消息
     * @param <T> body 类型
     * @return 发送结果
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    <T> SendResult syncSend(Message<T> message);

    /**
     * 同步发送（指定超时）。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数
     * @param <T> body 类型
     * @return 发送结果
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 超时
     */
    <T> SendResult syncSend(Message<T> message, long timeoutMillis);

    /**
     * 同步发送（指定超时与重试次数）。
     *
     * @param message 消息
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数（0 表示不重试）
     * @param <T> body 类型
     * @return 发送结果
     * @throws io.github.streammq.core.exception.ProducerTimeoutException 重试后仍超时
     */
    <T> SendResult syncSend(Message<T> message, long timeoutMillis, int retryTimes);

    /**
     * 异步发送（返回 {@link CompletableFuture}）。
     *
     * @param message 消息
     * @param <T> body 类型
     * @return 异步结果
     */
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message);

    /**
     * 异步发送（回调通知）。
     *
     * @param message 消息
     * @param callback 回调
     * @param <T> body 类型
     */
    <T> void asyncSend(Message<T> message, SendCallback callback);

    /**
     * 异步发送（回调通知 + 指定超时）。
     *
     * @param message 消息
     * @param callback 回调
     * @param timeoutMillis 超时毫秒数
     * @param <T> body 类型
     */
    <T> void asyncSend(Message<T> message, SendCallback callback, long timeoutMillis);

    /**
     * 单向发送：不等待响应，不抛异常。
     *
     * @param message 消息
     * @param <T> body 类型
     */
    <T> void sendOneway(Message<T> message);

    /**
     * 同步发送（通过 {@link SendOptions} 指定超时与重试参数）。
     *
     * <p>这是推荐的发送方式，通过 {@link SendOptions} 统一管理发送参数， 避免多个重载方法导致的 API 膨胀。
     *
     * @param message 消息
     * @param options 发送选项（超时、重试等），不能为 null
     * @param <T> body 类型
     * @return 发送结果
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    <T> SendResult syncSend(Message<T> message, SendOptions options);

    /**
     * 异步发送（通过 {@link SendOptions} 指定发送参数，返回 {@link CompletableFuture}）。
     *
     * @param message 消息
     * @param options 发送选项，不能为 null
     * @param <T> body 类型
     * @return 异步结果
     */
    <T> CompletableFuture<SendResult> asyncSend(Message<T> message, SendOptions options);

    /**
     * 异步发送（通过 {@link SendOptions} 指定发送参数，回调通知）。
     *
     * @param message 消息
     * @param options 发送选项，不能为 null
     * @param callback 回调
     * @param <T> body 类型
     */
    <T> void asyncSend(Message<T> message, SendOptions options, SendCallback callback);

    /**
     * 批量发送。
     *
     * <p>语义：所有消息通过 Pipeline 一次性发送到 Redis，单条失败不会导致整个批次异常。 返回的 {@link SendResult}
     * 列表与输入消息一一对应，每条结果携带该消息在 Redis Stream 中的<b>真实 Entry ID</b>， 每条结果的状态独立标识成功/失败。 如果 Pipeline
     * 本身异常（如网络中断），则抛出 {@link io.github.streammq.core.exception.StreamMQException}。
     *
     * @param batch 批量消息
     * @param <T> body 类型
     * @return 每条消息的发送结果（与输入顺序一致）
     * @throws IllegalArgumentException 如果 batch 为空
     * @throws io.github.streammq.core.exception.StreamMQException 如果 Pipeline 本身异常
     */
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);

    /**
     * 批量发送，支持指定超时与重试次数。
     *
     * <p>语义与 {@link #syncSendBatch(BatchMessage)} 一致，另支持 Pipeline 超时与整体重试。 重试语义为
     * at-least-once：失败后可能已部分写入 Redis，重试可能造成重复投递，由消费端幂等兜底。
     *
     * @param batch 批量消息
     * @param timeoutMillis 超时毫秒数（&lt;=0 使用默认超时）
     * @param retryTimes 重试次数（&lt;0 按 0 处理）
     * @param <T> body 类型
     * @return 每条消息的发送结果（与输入顺序一致）
     * @throws IllegalArgumentException 如果 batch 为空
     * @throws io.github.streammq.core.exception.StreamMQException 如果 Pipeline 本身异常
     */
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch, long timeoutMillis, int retryTimes);

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
    void addProducerFilter(io.github.streammq.core.filter.ProducerFilter filter);
}
