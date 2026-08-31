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
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.message.MessageBuilder;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 高压力并发与重启恢复集成测试。
 *
 * <p>覆盖「并发压力测试」与「故障注入-客户端重启恢复」两类此前缺失的场景：
 *
 * <ol>
 *   <li>{@link #highConcurrencyExactlyOnce()}：2 个并发生产者共 3000 条消息，消费并发度 8， 验证高压下不丢不重（恰好一次投递语义）。
 *   <li>{@link #containerRestartRecovers()}：消费端停止后再启动（模拟客户端重启）， 期间积压的消息在恢复后全部被消费，不丢不重。
 * </ol>
 *
 * <p>沿用 {@link RetryAndDlqIT} 的注解代理模式，避免重复实现。
 */
@DisplayName("高压力并发与重启恢复集成测试")
class HighConcurrencyStressIT extends AbstractRedisIT {

    private static final int TOTAL = 3000;
    private static final int CONCURRENCY = 8;

    /** 构造指定并发度的消费者注解（consumeThreadMin/Max 生效，见 F-06-08 修复）。 */
    @SuppressWarnings("unchecked")
    private StreamMQConsumer mkAnnotation(String topic, String group, int concurrency) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "topic" -> topic;
                                    case "consumerGroup" -> group;
                                    case "consumeMode" -> ConsumeMode.CLUSTERING;
                                    case "messageModel" -> MessageModel.CONCURRENT;
                                    case "consumeThreadMin" -> concurrency;
                                    case "consumeThreadMax" -> concurrency;
                                    case "selectorExpression" -> "*";
                                    case "selectorType" -> SelectorType.TAG;
                                    case "maxReconsumeTimes" -> 16;
                                    case "annotationType" -> StreamMQConsumer.class;
                                    default -> defaultAnnotationValue(method.getReturnType());
                                });
    }

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
    @DisplayName("2 个并发生产者 3000 条消息，并发消费不丢不重")
    void highConcurrencyExactlyOnce() throws InterruptedException {
        String topic = "stress-topic";
        String group = "stress-group";

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new RetryAndDlqIT.FastRetryPolicy(100, 100),
                        namespace);

        Map<String, AtomicInteger> seen = new ConcurrentHashMap<>();
        AtomicInteger processed = new AtomicInteger();
        StreamMessageConcurrentlyConsumer<String> listener =
                (msg, ctx) -> {
                    seen.computeIfAbsent(msg.getBody(), k -> new AtomicInteger()).incrementAndGet();
                    processed.incrementAndGet();
                    return ConsumeAction.SUCCESS;
                };
        container.registerConsumer(listener, mkAnnotation(topic, group, CONCURRENCY));
        createConsumerGroup(topic, group);
        container.start();

        try {
            // 2 个并发生产者，各投递 TOTAL/2 条，验证并发 XADD 路径
            CountDownLatch producersDone = new CountDownLatch(2);
            for (int p = 1; p <= 2; p++) {
                final String producerName = "producer-" + p;
                new Thread(
                                () -> {
                                    RedissonStreamProducer producer = null;
                                    try {
                                        producer =
                                                new RedissonStreamProducer(
                                                        redisson,
                                                        namespace,
                                                        group + "-" + producerName,
                                                        converter,
                                                        3000L,
                                                        0,
                                                        0,
                                                        0);
                                        for (int i = 0; i < TOTAL / 2; i++) {
                                            producer.syncSend(
                                                    MessageBuilder.<String>withTopic(topic)
                                                            .body(producerName + "-" + i)
                                                            .build());
                                        }
                                    } finally {
                                        if (producer != null) {
                                            producer.close();
                                        }
                                        producersDone.countDown();
                                    }
                                },
                                producerName)
                        .start();
            }
            assertThat(producersDone.await(60, TimeUnit.SECONDS)).as("两个生产者应在 60s 内完成投递").isTrue();

            // 不丢：全部送达
            await().atMost(60, TimeUnit.SECONDS).until(() -> processed.get() >= TOTAL);

            // 不重：观察窗口内无额外消息进入（重复投递会先 >= 后 >）
            await().pollDelay(2, TimeUnit.SECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> processed.get() <= TOTAL);
            assertThat(processed).hasValue(TOTAL);
            assertThat(seen).hasSize(TOTAL);
            assertThat(seen.values())
                    .as("每条消息恰好消费一次")
                    .allSatisfy(c -> assertThat(c.get()).isEqualTo(1));
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("消费端重启后积压消息不丢不重地全部恢复消费")
    void containerRestartRecovers() {
        String topic = "restart-topic";
        String group = "restart-group";

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new RetryAndDlqIT.FastRetryPolicy(100, 100),
                        namespace);

        Map<String, AtomicInteger> seen = new ConcurrentHashMap<>();
        StreamMessageConcurrentlyConsumer<String> listener =
                (msg, ctx) -> {
                    seen.computeIfAbsent(msg.getBody(), k -> new AtomicInteger()).incrementAndGet();
                    return ConsumeAction.SUCCESS;
                };
        container.registerConsumer(listener, mkAnnotation(topic, group, CONCURRENCY));
        createConsumerGroup(topic, group);

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        try {
            // 第一阶段：1000 条
            for (int i = 0; i < 1000; i++) {
                producer.syncSend(
                        MessageBuilder.<String>withTopic(topic).body("phase1-" + i).build());
            }
            container.start();
            await().atMost(30, TimeUnit.SECONDS).until(() -> seen.size() >= 1000);

            // 第二阶段：停掉容器（模拟客户端重启），期间再投 500 条
            container.stop();
            for (int i = 0; i < 500; i++) {
                producer.syncSend(
                        MessageBuilder.<String>withTopic(topic).body("phase2-" + i).build());
            }

            // 重启容器：积压消息应全部被消费
            container.start();
            await().atMost(30, TimeUnit.SECONDS).until(() -> seen.size() >= 1500);

            await().pollDelay(2, TimeUnit.SECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> seen.size() <= 1500);
            assertThat(seen).hasSize(1500);
            assertThat(seen.values())
                    .as("重启前后均不重")
                    .allSatisfy(c -> assertThat(c.get()).isEqualTo(1));
        } finally {
            container.stop();
            producer.close();
        }
    }
}
