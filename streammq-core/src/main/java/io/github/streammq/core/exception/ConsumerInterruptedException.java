/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.exception;

import lombok.Getter;

/**
 * 消费中断异常：Listener 容器在消费过程中被强制停止（如 JVM 关闭、消费超时）。
 *
 * <p>属于可恢复异常，重启后可继续消费 PEL 中的消息。
 *
 * <p><b>使用方说明（0.1.2）：</b>core 只提供类型，不在自身代码中抛出——消费线程阻塞在 Broker 交互 （XREADGROUP/ACK/XAUTOCLAIM
 * 等）时被中断的场景位于适配层（{@code streammq-redisson} 的容器/监听器 循环），由适配层在 {@code InterruptedException}
 * 处包装为本异常抛出，以便业务层区分"消费者被停止"与普通运行时错误。 业务层若需要感知容器停止， 捕获本异常即可；其 {@code getTopic()} / {@code
 * getConsumerGroup()} 标明受影响的注册。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public class ConsumerInterruptedException extends StreamMQException {

    private static final long serialVersionUID = 1L;

    /** 受影响的 ConsumerGroup */
    private final String consumerGroup;

    /** 受影响的 Topic */
    private final String topic;

    public ConsumerInterruptedException(String message, String topic, String consumerGroup) {
        super(message);
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public ConsumerInterruptedException(
            String message, String topic, String consumerGroup, Throwable cause) {
        super(message, cause);
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }
}
