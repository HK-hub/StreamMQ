/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.consumer;

import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DlqMessageConsumer} 抽象基类，提供日志、异常处理等公共逻辑。
 *
 * <p>子类只需实现 {@link #onDlqMessage(Message, ConsumeContext)}， 成功时框架自动 ACK，抛出异常时由 {@code
 * DlqFailureStrategy} 决策后续处理。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class AbstractDlqMessageConsumer<T> implements DlqMessageConsumer<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * 处理死信消息（子类实现核心逻辑）。
     *
     * @param message 死信消息
     * @param context 消费上下文
     * @throws Exception 抛出即视为消费失败
     */
    @Override
    public abstract void onDlqMessage(Message<T> message, ConsumeContext context) throws Exception;
}
