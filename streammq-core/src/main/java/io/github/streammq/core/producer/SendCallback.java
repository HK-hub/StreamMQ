/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.producer;

import io.github.streammq.core.message.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步发送回调。
 *
 * <p>用于 {@code StreamMQTemplate.asyncSend(message, callback)} 形式的异步发送。 框架在发送完成（成功或失败）后调用对应方法。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * template.asyncSend(msg, new SendCallback() {
 *     @Override
 *     public void onSuccess(SendResult result) {
 *         log.info("Send success: {}", result);
 *     }
 *
 *     @Override
 *     public void onException(Throwable ex) {
 *         log.error("Send failed", ex);
 *     }
 * });
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@FunctionalInterface
public interface SendCallback {

    /**
     * 发送成功回调。
     *
     * @param result 发送结果
     */
    void onSuccess(SendResult result);

    /**
     * 发送失败回调。
     *
     * <p>默认实现记录 WARN 日志（含异常堆栈），业务方可按需覆盖以实现告警/重试等逻辑。
     *
     * @param ex 异常
     */
    default void onException(Throwable ex) {
        logger().warn("async send failed (default SendCallback.onException)", ex);
    }

    /**
     * 默认回调日志器（private static，非 API 成员）。
     *
     * <p>0.1.2 起不再以 {@code public static final Logger log} 形式暴露：接口字段隐式 public static final，
     * 会把实现细节变成公开 API 面（用户可读、可被静态引用、无法随实现演进）。{@link LoggerFactory#getLogger(Class)}
     * 自身有缓存，此处按调用获取不引入额外开销。
     */
    private static Logger logger() {
        return LoggerFactory.getLogger(SendCallback.class);
    }
}
