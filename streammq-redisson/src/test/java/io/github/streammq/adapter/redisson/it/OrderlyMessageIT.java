/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.ContainerState;
import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.*;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

/**
 * 顺序消费({@code @StreamMQConsumer(messageModel = ORDERLY)})端到端 Redis 联动集成测试。
 *
 * <p>覆盖 {@link DefaultStreamMQListenerContainer#registerOrderlyConsumer} 注册的 {@link
 * StreamMessageOrderlyConsumer} 在真实 Redis 环境下的消息接收、顺序消费、 {@link
 * OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT} 挂起行为以及容器生命周期管理。
 */
@DisplayName("顺序消费集成测试")
class OrderlyMessageIT extends AbstractRedisIT {

    /** 测试用快速重试策略,固定延迟与最大重试次数。 当 {@code reconsumeTimes >= maxRetries} 时返回 {@code null},触发 DLQ 路由。 */
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

    /**
     * 通过动态代理构造 {@link StreamMQConsumer} 注解实例（messageModel=ORDERLY）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @param maxReconsumeTimes 最大重试次数
     * @return 注解代理实例
     */
    private static StreamMQConsumer mkOrderlyAnnotation(
            String topic, String group, int maxReconsumeTimes) {
        return mkOrderlyAnnotation(topic, group, maxReconsumeTimes, 0L, 1000L);
    }

    /**
     * 通过动态代理构造 {@link StreamMQConsumer} 注解实例（messageModel=ORDERLY），可指定顺序消费超时与挂起时长。
     *
     * @param topic 主题
     * @param group 消费者组
     * @param maxReconsumeTimes 最大重试次数
     * @param orderlyConsumeTimeout 顺序消费超时（毫秒），0 表示不启用
     * @param suspendMillis 每次失败后的分片挂起时长（毫秒）
     * @return 注解代理实例
     */
    @SuppressWarnings("unchecked")
    private static StreamMQConsumer mkOrderlyAnnotation(
            String topic,
            String group,
            int maxReconsumeTimes,
            long orderlyConsumeTimeout,
            long suspendMillis) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "topic" -> topic;
                                    case "consumerGroup" -> group;
                                    case "consumeMode" -> ConsumeMode.CLUSTERING;
                                    case "messageModel" -> MessageModel.ORDERLY;
                                    case "maxReconsumeTimes" -> maxReconsumeTimes;
                                    case "consumeThreadMin" -> 1;
                                    case "consumeThreadMax" -> 1;
                                    case "consumeTimeout" -> 30000L;
                                    case "shardCount" -> 4;
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
                                    case "orderlyConsumeTimeout" -> orderlyConsumeTimeout;
                                    case "suspendCurrentQueueTimeMillis" -> suspendMillis;
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
                                                    + ", messageModel=ORDERLY)";
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
    @DisplayName("单条消息:OrderlyListener 收到消息并返回 SUCCESS 后消息被 ACK")
    void orderlyListener_receivesMessage() {
        String topic = "orderly-recv-topic";
        String group = "orderly-recv-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);

        AtomicReference<Message<?>> receivedRef = new AtomicReference<>();
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    receivedRef.set(msg);
                    return ConsumeAction.SUCCESS;
                };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            producer.syncSend(
                    MessageBuilder.<String>withTopic(topic)
                            .tag("t1")
                            .keys("k1")
                            .body("orderly-body")
                            .build());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(receivedRef.get()).isNotNull());

            Message<?> received = receivedRef.get();
            assertThat(received.getTopic()).isEqualTo(topic);
            assertThat(received.getTag()).isEqualTo("t1");
            assertThat(received.getKeys()).isEqualTo("k1");
            assertThat(received.getBody()).isEqualTo("orderly-body");

            // SUCCESS 后 PEL 应为空
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
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("多条消息:全部被 OrderlyListener 顺序消费,内容与发送顺序一致")
    void orderlyListener_multipleMessages_allConsumed() {
        String topic = "orderly-multi-topic";
        String group = "orderly-multi-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);

        List<String> consumedBodies = new java.util.concurrent.CopyOnWriteArrayList<>();
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    consumedBodies.add((String) msg.getBody());
                    return ConsumeAction.SUCCESS;
                };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            // 按顺序发送 5 条消息
            for (int i = 0; i < 5; i++) {
                producer.syncSend(MessageBuilder.<String>withTopic(topic).body("m-" + i).build());
            }

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(consumedBodies).hasSize(5));

            // 验证消费顺序与发送顺序一致
            assertThat(consumedBodies).containsExactly("m-0", "m-1", "m-2", "m-3", "m-4");

            // 全部 ACK 后 PEL 应为空
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
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName(
            "SUSPEND_CURRENT_QUEUE_A_MOMENT:OrderlyListener 返回 SUSPEND 后消息留在 PEL,不进入 retry ZSet")
    void orderlyListener_suspend_staysInPel() {
        String topic = "orderly-suspend-topic";
        String group = "orderly-suspend-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);

        AtomicInteger attempt = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    attempt.incrementAndGet();
                    // 始终返回 SUSPEND,消息应留在 PEL
                    return ConsumeAction.RECONSUME_LATER;
                };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 16));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("suspend-body").build());

            // 等待消费者被调用至少一次
            await().atMost(10, TimeUnit.SECONDS).until(() -> attempt.get() >= 1);

            // SUSPEND 后消息应留在 PEL 中(未 ACK)
            RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .as("SUSPEND 后消息应留在 PEL 中")
                    .isNotEmpty();

            // retry ZSet 不应有消息(SUSPEND 不写入 retry ZSet)
            String retryKey = StreamMQKeys.retryZSet(namespace, topic, group);
            RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
            assertThat(zset.size()).as("SUSPEND 不应写入 retry ZSet").isZero();
        } finally {
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("生命周期:start 后容器运行,stop 后容器停止且不再消费消息")
    void orderlyListener_lifecycle_startStop() {
        String topic = "orderly-lifecycle-topic";
        String group = "orderly-lifecycle-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);

        AtomicInteger consumed = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    consumed.incrementAndGet();
                    return ConsumeAction.SUCCESS;
                };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);

        // 启动前应为 INIT 状态且非运行
        assertThat(container.isRunning()).isFalse();
        assertThat(container.getState()).isEqualTo(ContainerState.INIT);

        container.start();
        try {
            // 启动后应为 RUNNING 状态
            assertThat(container.isRunning()).isTrue();
            assertThat(container.getState()).isEqualTo(ContainerState.RUNNING);

            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            try {
                producer.syncSend(MessageBuilder.<String>withTopic(topic).body("lc-body").build());

                // 等待消费发生
                await().atMost(10, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(consumed.get()).isGreaterThan(0));
            } finally {
                producer.close();
            }
        } finally {
            container.stop();
        }

        // 停止后应为 STOPPED 状态且非运行
        assertThat(container.isRunning()).isFalse();
        assertThat(container.getState()).isEqualTo(ContainerState.STOPPED);
    }

    @Test
    @DisplayName("顺序消费失败时原地重试：同分片不越过失败消息（严格有序）")
    void orderlyListener_failureRetriedInPlace_preservesOrder() {
        String topic = "orderly-retry-order-topic";
        String group = "orderly-retry-order-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);

        java.util.List<String> processed = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.Map<String, AtomicInteger> calls = new java.util.concurrent.ConcurrentHashMap<>();
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    String key = msg.getKeys();
                    processed.add(key);
                    AtomicInteger c = calls.computeIfAbsent(key, k -> new AtomicInteger());
                    // k2 前两次调用失败，之后成功
                    if ("k2".equals(key) && c.incrementAndGet() <= 2) {
                        return ConsumeAction.RECONSUME_LATER;
                    }
                    return ConsumeAction.SUCCESS;
                };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            // 同分片（相同 shardingKey）发送 3 条消息
            for (int i = 1; i <= 3; i++) {
                producer.syncSend(
                        MessageBuilder.<String>withTopic(topic)
                                .shardingKey("shard-1")
                                .keys("k" + i)
                                .body("b" + i)
                                .build());
            }

            // 等待 k2 重试完成且 k3 被消费
            await().atMost(15, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    calls.get("k2") != null
                                            && calls.get("k2").get() >= 3
                                            && processed.contains("k3"));

            int i1 = processed.indexOf("k1");
            int i2First = processed.indexOf("k2");
            int i2Last = processed.lastIndexOf("k2");
            int i3 = processed.indexOf("k3");

            // 严格有序：k1 在 k2 首次调用前处理；k2 最后一次（成功）在 k3 之前处理；k3 不得在 k2 成功前处理
            assertThat(i1).isGreaterThanOrEqualTo(0);
            assertThat(i2First).isGreaterThan(i1);
            assertThat(i2Last).isLessThan(i3);
            assertThat(calls.get("k2").get()).isGreaterThanOrEqualTo(3);
        } finally {
            producer.close();
            container.stop();
        }
    }

    // ===================== 顺序消费超时（orderlyConsumeTimeout） =====================

    @Test
    @DisplayName("顺序消费超时:卡死 handler 不再阻塞分片,耗尽重试后进入 DLQ")
    void orderlyListener_consumeTimeout_stuckHandlerRoutedToDlq() {
        String topic = "orderly-timeout-topic";
        String group = "orderly-timeout-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);
        // 压缩取消宽限期，加快「超时取消 → 重投」收敛，避免 IT 空等
        container.setTimeoutCancelGraceMillis(100L);

        AtomicInteger attempts = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    attempts.incrementAndGet();
                    try {
                        // 卡死：远超超时阈值；响应中断，超时取消后立即退出
                        Thread.sleep(30_000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return ConsumeAction.RECONSUME_LATER;
                    }
                    return ConsumeAction.SUCCESS;
                };
        // orderlyConsumeTimeout=300ms，maxReconsumeTimes=2 → 首投 1 次 + 重试 2 次后耗尽
        container.registerOrderlyConsumer(
                listener, mkOrderlyAnnotation(topic, group, 2, 300L, 50L));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("stuck-body").build());

            String dlqKey = StreamMQKeys.dlqStream(namespace, group);
            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                RStream<String, String> dlq = redisson.getStream(dlqKey);
                                assertThat(dlq.size()).as("耗尽重试后应进入 DLQ").isEqualTo(1L);
                            });

            // 核心断言：卡死 handler 被完整重试（首投 + 2 次重试）。这同时证明两件事：
            // 1. 消费循环未被永久阻塞；2. 超时取消后分片锁已释放，重试真正调用了业务 handler
            //    （而非空耗在 tryLock 上——该缺陷曾导致 handler 只被调用 1 次）
            assertThat(attempts.get()).as("应触发首投 + 2 次重试").isGreaterThanOrEqualTo(3);
        } finally {
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("顺序消费超时:过小的超时会把慢但可完成的消息误杀进 DLQ（故默认不启用）")
    void orderlyListener_tooSmallTimeout_slowHandlerMisroutedToDlq() {
        String topic = "orderly-slow-topic";
        String group = "orderly-slow-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);
        container.setTimeoutCancelGraceMillis(100L);

        AtomicInteger attempts = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    attempts.incrementAndGet();
                    try {
                        // 慢但能完成：600ms 后返回 SUCCESS，本应正常 ACK
                        Thread.sleep(600L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return ConsumeAction.RECONSUME_LATER;
                    }
                    return ConsumeAction.SUCCESS;
                };
        // 超时(300ms) < 实际耗时(600ms)：同一条慢消息每次尝试都被中断
        container.registerOrderlyConsumer(
                listener, mkOrderlyAnnotation(topic, group, 1, 300L, 50L));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("slow-body").build());

            String dlqKey = StreamMQKeys.dlqStream(namespace, group);
            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                RStream<String, String> dlq = redisson.getStream(dlqKey);
                                assertThat(dlq.size()).as("超时小于实际耗时，慢消息会被误杀进 DLQ").isEqualTo(1L);
                            });
            assertThat(attempts.get()).as("同一消息被多次尝试后才耗尽").isGreaterThanOrEqualTo(2);
        } finally {
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("顺序消费超时:注解未声明时回落到容器全局默认值")
    void orderlyListener_globalDefaultTimeoutApplied() {
        String topic = "orderly-timeout-default-topic";
        String group = "orderly-timeout-default-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory =
                new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson, consumerFactory, converter, retryPolicy, namespace);
        container.setTimeoutCancelGraceMillis(100L);
        // 全局默认值：注解未显式声明（传 0）时回落到此值
        container.setDefaultOrderlyConsumeTimeoutMillis(300L);

        AtomicInteger attempts = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener =
                (msg, ctx) -> {
                    attempts.incrementAndGet();
                    try {
                        Thread.sleep(30_000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return ConsumeAction.RECONSUME_LATER;
                    }
                    return ConsumeAction.SUCCESS;
                };
        // 三参重载 → orderlyConsumeTimeout 为 0（未声明），应继承全局的 300ms
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 1));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("inherit-body").build());

            String dlqKey = StreamMQKeys.dlqStream(namespace, group);
            await().atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                RStream<String, String> dlq = redisson.getStream(dlqKey);
                                assertThat(dlq.size())
                                        .as("注解未声明时应继承全局超时，卡死消息最终进 DLQ")
                                        .isEqualTo(1L);
                            });
            // maxReconsumeTimes=1 → 首投 1 次 + 重试 1 次
            assertThat(attempts.get()).as("应触发首投 + 1 次重试").isGreaterThanOrEqualTo(2);
        } finally {
            producer.close();
            container.stop();
        }
    }
}
