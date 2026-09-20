/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.transaction;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单事务消息消费者示例。
 *
 * <p>事务消息「半消息 + 本地事务提交」完成后才投递到 Topic，本消费者负责接收并处理这些已提交的订单消息， 使事务示例默认运行（{@code mvn
 * spring-boot:run}）即可观察「发送 → 提交 → 消费」完整闭环。
 *
 * <p>开发建议（幂等）：事务消息与普通消息一样是 at-least-once 投递，重试/回查可能造成重复投递， 业务处理需按 {@code keys}（订单号）幂等。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = SampleConstants.CONSUMER_GROUP)
public class OrderTransactionConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderTransactionConsumer.class);

    /** 已接收消息数（供集成测试断言示例自带消费端确实收到事务消息） */
    private final AtomicInteger receivedCount = new AtomicInteger(0);

    /** 最近一条消息的 body（供集成测试断言内容） */
    private final AtomicReference<String> lastBody = new AtomicReference<>();

    /**
     * 处理事务消息（本地事务已提交后投递）。
     *
     * @param message 消息载体
     * @param context 消费上下文
     * @return 消费成功动作
     */
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        lastBody.set(message.getBody());
        int count = receivedCount.incrementAndGet();
        log.info(
                "Received transaction message: keys={}, tag={}, body={}, bizType={},"
                        + " consumedCount={}",
                message.getKeys(),
                message.getTag(),
                message.getBody(),
                message.getUserProperties().get(SampleConstants.PROP_BIZ_TYPE),
                count);
        // 实际业务在这里处理订单：例如写订单库/发通知；失败请返回 RECONSUME_LATER，由框架重试并在
        // 耗尽 max-reconsume-times 后路由到 DLQ（streammq:{ns}:dlq:{consumerGroup}）
        return ConsumeAction.SUCCESS;
    }

    /**
     * 获取已接收消息数。
     *
     * @return 已接收消息数
     */
    public int getReceivedCount() {
        return receivedCount.get();
    }

    /**
     * 获取最近一条消息的 body。
     *
     * @return 最近一条消息 body，未收到消息时为 null
     */
    public String getLastBody() {
        return lastBody.get();
    }
}
