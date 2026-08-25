/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.*;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

/**
 * 消费超时（BUG-11）与广播模式重试（BUG-13）针对性集成测试。
 *
 * <p>覆盖：
 *
 * <ul>
 *   <li>并发消费超时：{@code onMessage} 阻塞超过 {@code consumeTimeout} 时，容器中断业务线程并调度重试
 *   <li>广播模式重试：多实例广播消费时，重试消息只应被一个实例消费，避免被所有实例重复消费
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("消费超时与广播重试集成测试")
class ConsumeTimeoutAndBroadcastIT extends AbstractRedisIT {

    /** 测试用快速重试策略，可配置固定延迟与最大重试次数。 */
    static class FastRetryPolicy implements RetryPolicy {
        private final long delayMs;
        private final int maxRetries;

        FastRetryPolicy(long delayMs, int maxRetries) {
            this.delayMs = delayMs;
            this.maxRetries = maxRetries;
        }

        @Override
        public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
            if (reconsumeTimes >= maxRetries) {
                return null;
            }
            return Duration.ofMillis(delayMs);
        }

        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
            return reconsumeTimes >= maxRetries;
        }
    }

    /** 通过动态代理构造 {@link StreamMQConsumer} 注解实例（可指定消费模式与超时）。 */
    @SuppressWarnings("unchecked")
    private static StreamMQConsumer mkAnnotation(
            String topic, String group, int maxReconsumeTimes, ConsumeMode mode, long timeout) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "topic" -> topic;
                                    case "consumerGroup" -> group;
                                    case "consumeMode" -> mode;
                                    case "messageModel" -> MessageModel.CONCURRENT;
                                    case "maxReconsumeTimes" -> maxReconsumeTimes;
                                    case "consumeThreadMin" -> 1;
                                    case "consumeThreadMax" -> 64;
                                    case "consumeTimeout" -> timeout;
                                    case "selectorExpression" -> "*";
                                    case "serializer" -> MessageSerializer.class;
                                    case "namespace" -> "";
                                    case "enable" -> true;
                                    case "selectorType" -> SelectorType.TAG;
                                    case "pullBatchSize" -> 32;
                                    case "retryPolicy" -> RetryPolicy.class;
                                    case "enableMsgTrace" -> false;
                                    case "streamMaxLen" -> 0;
                                    case "messageConverter" -> MessageConverter.class;
                                    case "rebalanceStrategy" -> RebalanceStrategy.class;
                                    case "pullInterval" -> 0L;
                                    case "suspendCurrentQueueTimeMillis" -> 1000L;
                                    case "shardCount" -> 4;
                                    case "consumerName" -> "";
                                    case "annotationType" -> StreamMQConsumer.class;
                                    case "hashCode" -> (topic + group).hashCode();
                                    case "equals" ->
                                            args != null && args.length > 0 && proxy == args[0];
                                    case "toString" ->
                                            "@StreamMQConsumer(topic="
                                                    + topic
                                                    + ", consumerGroup="
                                                    + group
                                                    + ")";
                                    default -> defaultAnnotationValue(method.getReturnType());
                                });
    }

    /** 根据返回类型返回注解属性的默认值，避免新增注解属性时测试代理崩溃。 */
    private static Object defaultAnnotationValue(Class<?> returnType) {
        if (returnType == String.class) return "";
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == boolean.class) return false;
        if (returnType == Class.class) return null;
        if (returnType.isEnum()) return returnType.getEnumConstants()[0];
        return null;
    }

    @Test
    @DisplayName("消费超时:onMessage 阻塞超过 consumeTimeout 时被中断并进入重试")
    void consumeTimeout_interruptsBlockedConsumerAndRetries() {
        String topic = "timeout-topic";
        String group = "timeout-group";

        RetryPolicy fastPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, fastPolicy, namespace);

        AtomicInteger calls = new AtomicInteger(0);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        StreamMessageConcurrentlyConsumer<String> listener =
                (msg, ctx) -> {
                    int c = calls.incrementAndGet();
                    if (c == 1) {
                        // 第一次处理阻塞 5s，超过 consumeTimeout(500ms) 触发超时取消
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            interrupted.set(true);
                            throw new RuntimeException("interrupted by consume timeout", e);
                        }
                    }
                    return ConsumeAction.SUCCESS;
                };
        // consumeTimeout=500ms：验证超时中断 + 重试后第二次成功
        container.registerConsumer(
                listener, mkAnnotation(topic, group, 16, ConsumeMode.CLUSTERING, 500L));
        RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10);
        container.registerRetryTargets(scheduler);
        createConsumerGroup(topic, group);
        scheduler.start();
        container.start();

        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("timeout-body").build());
            producer.close();

            // 第一次调用被超时中断 → 进入 retry → 第二次调用成功
            await().atMost(15, TimeUnit.SECONDS).until(() -> calls.get() >= 2);
            // 验证超时确实中断了业务线程（cancel 后宽限等待生效）
            assertThat(interrupted.get())
                    .as("first attempt should be interrupted by consume timeout")
                    .isTrue();
            assertThat(calls.get()).isEqualTo(2);

            // 最终成功消费后 PEL 应为空
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, topic));
                                assertThat(
                                                stream.listPending(
                                                        group,
                                                        StreamMessageId.MIN,
                                                        StreamMessageId.MAX,
                                                        100))
                                        .isEmpty();
                            });
        } finally {
            container.stop();
            scheduler.stop();
        }
    }

    @Test
    @DisplayName("广播重试:两个广播实例下,重试消息仅被一个实例消费一次")
    void broadcastRetry_processedExactlyOnceByOneInstance() {
        String topic = "bcast-retry-topic";
        String group = "bcast-retry-group";

        RetryPolicy fastPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);

        // 实例 1：首次处理失败（触发重试）；实例 2：始终成功
        AtomicInteger instance1Calls = new AtomicInteger(0);
        AtomicInteger instance2Calls = new AtomicInteger(0);
        AtomicBoolean instance1SawRetry = new AtomicBoolean(false);
        AtomicBoolean instance2SawRetry = new AtomicBoolean(false);

        StreamMessageConcurrentlyConsumer<String> listener1 =
                (msg, ctx) -> {
                    if (msg.getReconsumeTimes() > 0) {
                        instance1SawRetry.set(true);
                    }
                    if (instance1Calls.incrementAndGet() == 1) {
                        throw new RuntimeException("instance1 first attempt fails");
                    }
                    return ConsumeAction.SUCCESS;
                };
        StreamMessageConcurrentlyConsumer<String> listener2 =
                (msg, ctx) -> {
                    if (msg.getReconsumeTimes() > 0) {
                        instance2SawRetry.set(true);
                    }
                    instance2Calls.incrementAndGet();
                    return ConsumeAction.SUCCESS;
                };

        StreamMQConsumer ann = mkAnnotation(topic, group, 16, ConsumeMode.BROADCASTING, 30000L);
        DefaultStreamMQListenerContainer container1 =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, fastPolicy, namespace);
        DefaultStreamMQListenerContainer container2 =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, fastPolicy, namespace);
        container1.registerConsumer(listener1, ann);
        container2.registerConsumer(listener2, ann);

        // 共享一个 RetryScheduler，两个实例的重试目标都注册进去
        RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10);
        container1.registerRetryTargets(scheduler);
        container2.registerRetryTargets(scheduler);
        scheduler.start();
        container1.start();
        container2.start();

        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(
                    MessageBuilder.<String>withTopic(topic).body("broadcast-retry").build());
            producer.close();

            // 等待重试消息被消费（任一实例观察到 reconsumeTimes>0 的副本）
            await().atMost(15, TimeUnit.SECONDS)
                    .until(() -> instance1SawRetry.get() || instance2SawRetry.get());
            // 稳定窗口：若存在“所有实例重复消费重试”的 bug，会在此窗口内暴露
            Thread.sleep(2000);

            // 关键断言：重试副本只能被一个实例消费（XOR），不能被两个实例都消费
            assertThat(instance1SawRetry.get() ^ instance2SawRetry.get())
                    .as("retried message must be consumed by exactly one broadcast instance")
                    .isTrue();
            // 总调用 = 2 次原始（每个实例 1 次）+ 1 次重试（任一实例）
            assertThat(instance1Calls.get() + instance2Calls.get()).isEqualTo(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            container1.stop();
            container2.stop();
            scheduler.stop();
        }
    }
}
