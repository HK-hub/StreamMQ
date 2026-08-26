/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.dlq.SecondaryDlqFailureStrategy;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.RetryPolicy;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamCreateGroupArgs;

/**
 * DLQ 重试计数往返回归测试（红队审查 F-06-01）。
 *
 * <p>历史缺陷：{@code __dlqRetryCount} 以顶层 Entry 字段写入 DLQ Stream，但经 converter decode → encode
 * 往返后丢失，策略层看到的计数恒为 0 —— {@code LimitedRetryDlqFailureStrategy} 无限重试、{@link
 * SecondaryDlqFailureStrategy} 的二级死信永不可达。
 *
 * <p>修复后：保留字段（{@code __} 前缀）解码时捕获进用户属性并随 props JSON 往返持久化。 本测试以「二级 DLQ 在有限轮次内可达」作为计数存活的端到端证明。
 */
@DisplayName("DLQ 重试计数往返回归测试")
class DlqRetryRoundtripIT extends AbstractRedisIT {

    static class NoRetryPolicy implements RetryPolicy {
        @Override
        public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
            return null;
        }

        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private StreamMQConsumer mkAnnotation(String topic, String group, boolean dlqMode) {
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
                                    case "maxReconsumeTimes" -> 0;
                                    case "dlqMode" -> dlqMode;
                                    case "annotationType" -> StreamMQConsumer.class;
                                    default -> defaultValue(method.getReturnType());
                                });
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == String.class) return "";
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == boolean.class) return false;
        if (returnType == Class.class) return null;
        if (returnType.isEnum()) return returnType.getEnumConstants()[0];
        return null;
    }

    @Test
    @DisplayName("DLQ 消费持续失败时，重试计数存活并在 maxAttempts 轮内转投二级 DLQ")
    void dlqRetryCountSurvivesRoundtrip_secondaryDlqReached() {
        String topic = "rt-dlq-topic";
        String group = "rt-dlq-group";

        // maxAttempts=1：第 1 轮失败 → RETRY(count 0→1)；重投后再次失败必须解析到 count=1 ≥ 1
        // → SECONDARY_DLQ。若计数在往返中丢失（历史缺陷），将永远 RETRY 形成无限循环。
        DlqConfig dlqConfig =
                DlqConfig.builder()
                        .maxDlqRetryAttempts(1)
                        .dlqRetryDelayMs(200)
                        .dlqRetryBackoffMultiplier(1.0)
                        .build();
        SecondaryDlqFailureStrategy strategy = new SecondaryDlqFailureStrategy(dlqConfig);

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new NoRetryPolicy(),
                        strategy,
                        namespace);

        AtomicInteger dlqFailures = new AtomicInteger();
        container.registerConsumer(
                (msg, ctx) -> {
                    throw new RuntimeException("business always fails");
                },
                mkAnnotation(topic, group, false));

        container.registerConsumer(
                (StreamMessageConcurrentlyConsumer<String>)
                        (msg, ctx) -> {
                            dlqFailures.incrementAndGet();
                            throw new RuntimeException("dlq consumer always fails");
                        },
                mkAnnotation(topic, group, true));

        createConsumerGroup(topic, group);
        createGroupOn(StreamMQKeys.dlqStream(namespace, group), group);
        createGroupOn(StreamMQKeys.secondaryDlqStream(namespace, group, "dlq2"), group);

        // DLQ 重试依赖 RetryScheduler 扫描（生产环境由 starter 接线，测试内显式组装）
        io.github.streammq.adapter.redisson.scheduler.RetryScheduler retryScheduler =
                new io.github.streammq.adapter.redisson.scheduler.RetryScheduler(
                        redisson, namespace, 100L, 10);
        container.registerRetryTargets(retryScheduler);
        retryScheduler.start();

        container.start();
        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(
                    MessageBuilder.<String>withTopic(topic).body("roundtrip-body").build());
            producer.close();

            String dlq2Key = StreamMQKeys.secondaryDlqStream(namespace, group, "dlq2");
            RStream<String, String> dlq2 = redisson.getStream(dlq2Key);

            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(
                                                    dlq2.range(
                                                            10,
                                                            StreamMessageId.MIN,
                                                            StreamMessageId.MAX))
                                            .isNotEmpty());

            // 计数存活 => DLQ 消费次数有界（首投 + 有限重试），而非无限 ping-pong
            assertThat(dlqFailures.get())
                    .as("DLQ consumer invocations must stay bounded")
                    .isLessThanOrEqualTo(5);
            // 一级 DLQ 不再积压（消息已转投二级）
            RStream<String, String> dlq =
                    redisson.getStream(StreamMQKeys.dlqStream(namespace, group));
            assertThat(dlq.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                    .isEmpty();
        } finally {
            container.stop();
            retryScheduler.stop();
        }
    }

    /** 在指定 Stream 上创建消费者组（BUSYGROUP 幂等）。 */
    private void createGroupOn(String streamKey, String group) {
        try {
            redisson.<String, String>getStream(streamKey)
                    .createGroup(
                            StreamCreateGroupArgs.name(group)
                                    .makeStream()
                                    .id(new StreamMessageId(0, 0)));
        } catch (RuntimeException ex) {
            if (ex.getMessage() == null || !ex.getMessage().contains("BUSYGROUP")) {
                throw ex;
            }
        }
    }
}
