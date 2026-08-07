package io.github.streammq.test;

import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试用消息监听器，用于验证消息消费行为。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TestStreamMQListener<T> implements StreamMessageConcurrentlyConsumer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(TestStreamMQListener.class);

    private final List<Message<T>> receivedMessages = new ArrayList<>();
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);
    private final List<Exception> exceptions = new ArrayList<>();
    private volatile CountDownLatch latch;

    private ConsumeAction nextAction = ConsumeAction.SUCCESS;
    private volatile boolean shouldFail = false;
    private volatile int failAfterCount = Integer.MAX_VALUE;

    @Override
    public ConsumeAction onMessage(Message<T> message, ConsumeContext context) throws Exception {
        synchronized (receivedMessages) {
            receivedMessages.add(message);
        }

        LOG.debug("Test listener received message: topic={}, keys={}, body={}",
                message.getTopic(), message.getKeys(), message.getBody());

        if (latch != null) {
            latch.countDown();
        }

        if (shouldFail && successCount.get() >= failAfterCount) {
            Exception ex = new RuntimeException("Intentional test failure");
            synchronized (exceptions) {
                exceptions.add(ex);
            }
            failCount.incrementAndGet();
            throw ex;
        }

        successCount.incrementAndGet();

        return nextAction;
    }

    public List<Message<T>> getReceivedMessages() {
        synchronized (receivedMessages) {
            return new ArrayList<>(receivedMessages);
        }
    }

    public int getReceivedCount() {
        synchronized (receivedMessages) {
            return receivedMessages.size();
        }
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public int getFailCount() {
        return failCount.get();
    }

    public List<Exception> getExceptions() {
        synchronized (exceptions) {
            return new ArrayList<>(exceptions);
        }
    }

    public void reset() {
        synchronized (receivedMessages) {
            receivedMessages.clear();
        }
        successCount.set(0);
        failCount.set(0);
        synchronized (exceptions) {
            exceptions.clear();
        }
        latch = null;
        nextAction = ConsumeAction.SUCCESS;
        shouldFail = false;
        failAfterCount = Integer.MAX_VALUE;
    }

    public void setNextAction(ConsumeAction action) {
        this.nextAction = action;
    }

    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    public void setFailAfterCount(int count) {
        this.failAfterCount = count;
    }

    public void awaitMessages(int expectedCount, long timeoutMillis) throws InterruptedException {
        this.latch = new CountDownLatch(expectedCount);
        // 检查是否已经收到了足够的消息，补偿已收到的消息
        int alreadyReceived = getReceivedCount();
        for (int i = 0; i < Math.min(alreadyReceived, expectedCount); i++) {
            latch.countDown();
        }
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new AssertionError("Timeout waiting for " + expectedCount + " messages, received " + getReceivedCount());
        }
    }

    /**
     * 预设置 latch，用于在发送消息前初始化等待。
     * @param expectedCount 期望的消息数量
     */
    public void prepareAwait(int expectedCount) {
        this.latch = new CountDownLatch(expectedCount);
        // 检查是否已经收到了足够的消息
        if (getReceivedCount() >= expectedCount) {
            this.latch.countDown();
        }
    }

    /**
     * 等待预设的 latch。
     * @param timeoutMillis 超时时间（毫秒）
     * @throws InterruptedException 如果等待被中断
     */
    public void waitForMessages(long timeoutMillis) throws InterruptedException {
        if (latch == null) {
            throw new IllegalStateException("prepareAwait must be called before waitForMessages");
        }
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new AssertionError("Timeout waiting for messages, received " + getReceivedCount());
        }
    }
}