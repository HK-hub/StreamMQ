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

    /** 发送失败默认日志（空实现改为记录 WARN，保证异常可观测） */
    Logger log = LoggerFactory.getLogger(SendCallback.class);

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
        log.warn("async send failed (default SendCallback.onException)", ex);
    }
}
