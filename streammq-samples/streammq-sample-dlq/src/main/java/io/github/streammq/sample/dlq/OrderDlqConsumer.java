/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.dlq;

import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.consumer.AbstractDlqMessageConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.message.Message;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * 订单死信队列消费者示例。
 *
 * <p>演示死信队列消费：
 *
 * <ul>
 *   <li>使用 {@link StreamMQDlqConsumer} 注解注册死信消费者
 *   <li>继承 {@link AbstractDlqMessageConsumer} 实现死信消息处理
 *   <li>配置 DLQ 失败策略（默认 LogAndDrop）
 * </ul>
 *
 * <p>死信消息来源：当 {@code order-consumer-group} 的消息消费失败超过 maxReconsumeTimes 后， 消息会被转移到死信队列 {@code
 * streammq:{ns}:dlq:order-consumer-group}。
 *
 * <p><b>命名空间：</b>本注解<b>不声明</b> {@code namespace}，直接继承全局配置 {@code streammq.namespace}。 注解里的
 * namespace 只能写编译期常量，一旦硬编码就会与生产者 / 业务消费者使用的命名空间（含集成测试覆写的专属命名空间）分离， 导致 DLQ
 * 消费者永远读不到死信、并在另一个命名空间里留下空流与心跳键。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQDlqConsumer(
        consumerGroup = SampleConstants.CONSUMER_GROUP,
        maxDlqRetryAttempts = 3,
        dlqRetryDelayMs = 10000)
public class OrderDlqConsumer extends AbstractDlqMessageConsumer<String> {

    /** 已接收处理的死信消息数（供集成测试断言「重试 → DLQ → DLQ 消费者」闭环） */
    private final AtomicInteger receivedDlqMessageCount = new AtomicInteger(0);

    @Override
    public void onDlqMessage(Message<String> message, ConsumeContext context) throws Exception {
        receivedDlqMessageCount.incrementAndGet();
        log.info(
                "Received DLQ message: topic={}, keys={}, body={}, reconsumeTimes={},"
                        + " consumerGroup={}",
                message.getTopic(),
                message.getKeys(),
                message.getBody(),
                context.reconsumeTimes(),
                context.consumerGroup());

        try {
            processDlqMessage(message);
            log.info("DLQ message processed successfully: keys={}", message.getKeys());
        } catch (Exception e) {
            log.error(
                    "Failed to process DLQ message: keys={}, error={}",
                    message.getKeys(),
                    e.getMessage(),
                    e);
            throw e;
        }
    }

    private void processDlqMessage(Message<String> message) {
        log.debug("Processing DLQ message: body={}", message.getBody());
    }

    /**
     * 获取已接收处理的死信消息数。
     *
     * @return 死信消息数
     */
    public int getReceivedDlqMessageCount() {
        return receivedDlqMessageCount.get();
    }
}
