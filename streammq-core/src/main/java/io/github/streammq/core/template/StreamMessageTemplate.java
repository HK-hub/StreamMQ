package io.github.streammq.core.template;

import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.transaction.TransactionExecutor;
import io.github.streammq.core.transaction.TransactionCallback;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * StreamMQ 消息模板（核心生产者 API），对齐 RocketMQ {@code RocketMQTemplate} 体验。
 *
 * <p>核心发送语义：
 * <ul>
 *   <li>{@link #syncSend} - 同步发送，等待 Redis 返回</li>
 *   <li>{@link #asyncSend} - 异步发送，返回 {@link CompletableFuture} 或回调 {@link SendCallback}</li>
 *   <li>{@link #sendOneway} - 单向发送，不等待响应，性能最高</li>
 *   <li>{@link #syncSendBatch} - 批量发送，基于 RBatch Pipeline</li>
 *   <li>{@link #executeInTransaction} - 事务消息，半消息 + 本地事务</li>
 * </ul>
 *
 * <p>拦截器链：所有 {@code syncSend} / {@code asyncSend} 调用前后均经过 {@link ProducerInterceptor} 链。
 *
 * <p><b>泛型设计</b>：泛型参数 {@code <T>} 声明在方法级别而非类级别。一个 Template 单例
 * 可发送不同 body 类型的消息，无需为每种 body 类型创建独立的 Template 实例，
 * 也避免了调用方繁琐的泛型强转。
 *
 * <p>实现类位于 {@code streammq-redisson-adapter} 模块（{@code DefaultStreamMessageTemplate}）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageTemplate extends TransactionExecutor {

    /** 默认发送超时（毫秒） */
    long DEFAULT_SEND_TIMEOUT_MILLIS = 3000L;

    /** 默认同步发送重试次数 */
    int DEFAULT_SYNC_RETRY_TIMES = 2;

    /** 默认异步发送重试次数（不重试） */
    int DEFAULT_ASYNC_RETRY_TIMES = 0;

    /**
     * 同步发送（默认超时、默认重试次数）。
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
     * 批量发送。
     *
     * @param batch 批量消息
     * @param <T> body 类型
     * @return 每条消息的发送结果
     * @throws IllegalArgumentException 如果 batch 为空
     */
    <T> List<SendResult> syncSendBatch(BatchMessage<T> batch);

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
