package io.github.streammq.sample.delay;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 延时消息测试消费者，用于在集成测试中收集收到的消息。
 *
 * <p>使用独立的消费者组（{@code delay-order-consumer-group-it}），
 * 与业务消费者组隔离，确保不影响正常消费逻辑。
 *
 * <p>提供线程安全的消息收集、计数等待与重置能力，配合 Awaitility 进行超时等待。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
@StreamMQConsumer(
        topic = "delay-order-topic",
        consumerGroup = "delay-order-consumer-group-it",
        maxReconsumeTimes = 3
)
public class DelayMessageTestConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(DelayMessageTestConsumer.class);

    private static final List<Message<String>> receivedMessages =
            Collections.synchronizedList(new ArrayList<>());

    private static final AtomicInteger successCount = new AtomicInteger(0);

    private static volatile CountDownLatch latch;

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        receivedMessages.add(message);
        successCount.incrementAndGet();

        log.info("[TestConsumer] 收到延时消息: orderId={}, tag={}, body={}",
                message.getKeys(), message.getTag(), message.getBody());

        if (latch != null) {
            latch.countDown();
        }

        return ConsumeAction.SUCCESS;
    }

    /**
     * 获取所有已接收的消息列表（线程安全副本）。
     *
     * @return 已接收的消息列表
     */
    public static List<Message<String>> getReceivedMessages() {
        synchronized (receivedMessages) {
            return new ArrayList<>(receivedMessages);
        }
    }

    /**
     * 获取已接收消息的数量。
     *
     * @return 消息数量
     */
    public static int getReceivedCount() {
        synchronized (receivedMessages) {
            return receivedMessages.size();
        }
    }

    /**
     * 获取成功消费次数。
     *
     * @return 成功次数
     */
    public static int getSuccessCount() {
        return successCount.get();
    }

    /**
     * 重置所有收集状态，用于测试隔离。
     */
    public static void reset() {
        synchronized (receivedMessages) {
            receivedMessages.clear();
        }
        successCount.set(0);
        latch = null;
    }

    /**
     * 等待指定数量的消息到达。
     *
     * <p>使用 {@link CountDownLatch} 进行同步等待，超时则抛出断言错误。
     *
     * @param expectedCount 期望的消息数量
     * @param timeoutMillis 超时时间（毫秒）
     * @throws InterruptedException 如果等待被中断
     * @throws AssertionError 如果等待超时
     */
    public static void awaitMessages(int expectedCount, long timeoutMillis) throws InterruptedException {
        latch = new CountDownLatch(expectedCount);
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new AssertionError(
                    "等待消息超时，期望 " + expectedCount + " 条，实际收到 " + getReceivedCount() + " 条");
        }
    }
}