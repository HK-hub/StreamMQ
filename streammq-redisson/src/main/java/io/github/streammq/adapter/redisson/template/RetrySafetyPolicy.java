/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.template;

import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.exception.StreamMQClientException;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import java.util.Objects;
import java.util.UUID;

/**
 * 发送重试安全判定与失败结果构造（发送管线专用策略）。
 *
 * <p>原为 {@link DefaultStreamMessageTemplate} 的私有静态方法，被同步发送与批量发送两条路径共用， 提取为独立策略类以缩小模板类的私有面。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
final class RetrySafetyPolicy {

    private RetrySafetyPolicy() {}

    /**
     * 判断该异常是否可以安全重试：仅限发送尚未开始（消息确定未落库）的失败。
     *
     * <p>可重试：{@link SerializationException}（序列化失败，未触达 Redis）、{@link
     * StreamMQClientException}（客户端校验/本地错误）。 不可重试：{@link
     * io.github.streammq.core.exception.ProducerTimeoutException} / {@link
     * io.github.streammq.core.exception.StreamMQBrokerException} —— XADD 可能已成功，重试将产生重复消息。
     *
     * @param ex 发送异常
     * @return true 表示可安全重试
     */
    static boolean isSafeToRetry(StreamMQException ex) {
        return ex instanceof SerializationException || ex instanceof StreamMQClientException;
    }

    /**
     * 构造失败结果。
     *
     * @param message 消息
     * @param error 异常
     * @return 失败 SendResult
     */
    static SendResult buildFailedResult(Message<?> message, StreamMQException error) {
        // 使用 UUID 后缀确保失败结果在并发场景下也不会产生 MessageId 碰撞
        String failureId = System.currentTimeMillis() + "-" + UUID.randomUUID();
        return new SendResult(
                new MessageId(failureId),
                message.getTopic(),
                message.getTag(),
                SendStatus.SEND_FAILED,
                message.getBornTimestamp(),
                null,
                Objects.nonNull(error) ? error.getMessage() : "unknown error");
    }
}
