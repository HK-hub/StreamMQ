package io.github.streammq.sample.dlq;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订单消息消费者示例（模拟消费失败触发死信）。
 *
 * <p>演示死信机制：当消费失败超过 maxReconsumeTimes=3 后，消息进入死信队列。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(
        topic = "order-topic",
        consumerGroup = "order-consumer-group",
        maxReconsumeTimes = 3
)
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final AtomicInteger failCount = new AtomicInteger(0);
    private volatile boolean shouldFail = false;
    private volatile String failOrderId = null;

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) throws Exception {
        log.info("Received order message: keys={}, tag={}, body={}, reconsumeTimes={}",
                message.getKeys(), message.getTag(), message.getBody(), context.reconsumeTimes());

        if (shouldFail && failOrderId != null && failOrderId.equals(message.getKeys())) {
            int count = failCount.incrementAndGet();
            log.error("Simulating order processing failure: orderId={}, failCount={}, maxReconsumeTimes={}",
                    message.getKeys(), count, context.reconsumeTimes());

            if (context.reconsumeTimes() >= 3) {
                log.error("Order message will be moved to DLQ: orderId={}", message.getKeys());
            }

            throw new RuntimeException("Intentional failure for DLQ test: orderId=" + message.getKeys());
        }

        try {
            processOrder(message);
            log.info("Order processed successfully: keys={}", message.getKeys());
            return ConsumeAction.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process order: keys={}, error={}", message.getKeys(), e.getMessage(), e);

            if (context.reconsumeTimes() >= 3) {
                log.error("Order exhausted retries, will be moved to DLQ: keys={}", message.getKeys());
                return ConsumeAction.RECONSUME_LATER;
            }

            return ConsumeAction.RECONSUME_LATER;
        }
    }

    private void processOrder(Message<String> message) {
        log.debug("Processing order: body={}", message.getBody());
    }

    /**
     * 设置指定订单 ID 消费失败（用于测试死信机制）。
     *
     * @param orderId 订单 ID
     */
    public void setFailOrderId(String orderId) {
        this.shouldFail = true;
        this.failOrderId = orderId;
        this.failCount.set(0);
    }

    /**
     * 取消消费失败模拟。
     */
    public void clearFailOrderId() {
        this.shouldFail = false;
        this.failOrderId = null;
        this.failCount.set(0);
    }
}