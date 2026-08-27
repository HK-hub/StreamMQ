/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** 当前 latch 已通过存量补偿的数量（原子跟踪，防止补偿超量 countDown） */
    private final AtomicInteger compensatedCount = new AtomicInteger(0);

    private final List<Exception> exceptions = new ArrayList<>();
    private volatile CountDownLatch latch;

    private volatile ConsumeAction nextAction = ConsumeAction.SUCCESS;
    private volatile boolean shouldFail = false;
    private volatile int failAfterCount = Integer.MAX_VALUE;

    @Override
    public ConsumeAction onMessage(Message<T> message, ConsumeContext context) throws Exception {
        // 入表与 countDown 必须原子：awaitMessages 在同一锁下建 latch 并补偿存量，
        // 保证任一消息要么被补偿计数、要么触发新 latch，二者只取其一（消除竞态）
        synchronized (this) {
            receivedMessages.add(message);

            CountDownLatch current = latch;
            if (current != null) {
                current.countDown();
            }
        }

        LOG.debug(
                "Test listener received message: topic={}, keys={}, body={}",
                message.getTopic(),
                message.getKeys(),
                message.getBody());

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
        compensatedCount.set(0);
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
        // 与 onMessage 的 countDown 在同一把锁下完成"创建 latch + 补偿已收消息"，
        // 消除并发到达的消息被重复计数导致 latch 提前归零的竞态
        synchronized (this) {
            installLatch(expectedCount);
        }
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new AssertionError(
                    "Timeout waiting for "
                            + expectedCount
                            + " messages, received "
                            + getReceivedCount());
        }
    }

    /**
     * 预设置 latch，用于在发送消息前初始化等待。
     *
     * <p>与 {@link #awaitMessages(int, long)} 共用「创建 latch + 补偿存量」逻辑：对已到达的存量消息逐条补偿（此前实现只补偿一次，
     * expectedCount&gt;1 且消息已全部到达时调用方会永久挂起）；补偿数量经 {@code compensatedCount} 原子跟踪并封顶于期望值， 防止重复补偿导致超量
     * countDown。
     *
     * @param expectedCount 期望的消息数量
     */
    public void prepareAwait(int expectedCount) {
        // 与 onMessage 的 countDown 在同一把锁下完成"创建 latch + 补偿存量"，
        // 保证任一消息要么被补偿计数、要么触发新 latch，二者只取其一
        synchronized (this) {
            installLatch(expectedCount);
        }
    }

    /**
     * 在持有 {@code this} 锁的前提下创建新 latch 并补偿已到达的存量消息。
     *
     * @param expectedCount 期望的消息数量
     */
    private void installLatch(int expectedCount) {
        CountDownLatch fresh = new CountDownLatch(expectedCount);
        int alreadyArrived = receivedMessages.size();
        // 逐条补偿存量消息；封顶于 expectedCount，且以原子计数防重复补偿超量
        int compensate = Math.min(alreadyArrived, expectedCount);
        compensatedCount.set(0);
        for (int i = 0; i < compensate && compensatedCount.get() < expectedCount; i++) {
            fresh.countDown();
            compensatedCount.incrementAndGet();
        }
        this.latch = fresh;
    }

    /**
     * 等待预设的 latch。
     *
     * @param timeoutMillis 超时时间（毫秒）
     * @throws InterruptedException 如果等待被中断
     */
    public void waitForMessages(long timeoutMillis) throws InterruptedException {
        if (latch == null) {
            throw new IllegalStateException("prepareAwait must be called before waitForMessages");
        }
        if (!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new AssertionError(
                    "Timeout waiting for messages, received " + getReceivedCount());
        }
    }
}
