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
 * {@link DlqMessageConsumer} 抽象基类：为子类提供一个已按实际类型初始化的 {@code protected Logger}。
 *
 * <p>子类只需实现 {@link #onDlqMessage(Message, ConsumeContext)}， 成功时框架自动 ACK，抛出异常时由 {@code
 * DlqFailureStrategy} 决策后续处理（该决策逻辑不在本基类，由适配层的 {@code RetryAndDlqHandler} 承担）。
 *
 * <p><b>职责边界（0.1.2 起的准确描述）：</b>本基类只提供日志器，不提供异常处理模板方法—— 此前 javadoc 声称"提供日志、异常处理等公共逻辑"与实现不符（类体只有一个
 * logger 字段）。 保留本类而不删除是为了兼容已发布的 {@code extends AbstractDlqMessageConsumer<T>} 写法（samples / 下游用户）。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class AbstractDlqMessageConsumer<T> implements DlqMessageConsumer<T> {

    /** 子类日志器（按子类实际类型初始化，便于日志聚合）。 */
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
