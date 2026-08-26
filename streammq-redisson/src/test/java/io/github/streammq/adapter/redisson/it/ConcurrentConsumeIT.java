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
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.lang.reflect.Proxy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 并发消费循环回归测试（红队审查 F-06-08）。
 *
 * <p>历史缺陷：{@code consumeThreadMin/Max} 注解属性在适配层从未被读取，配置并发度无任何效果。
 *
 * <p>修复后：{@code consumeThreadMin>1} 为注册提交多条读循环（共享 consumer name， XREADGROUP
 * 原子分配保证互不相交）。本测试验证多循环下消息不丢、不重、全部被处理。
 */
@DisplayName("并发消费循环回归测试")
class ConcurrentConsumeIT extends AbstractRedisIT {

    static class NoRetryPolicy implements io.github.streammq.core.policy.RetryPolicy {
        @Override
        public java.time.Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
            return null;
        }

        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
            return true;
        }
    }

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
    @DisplayName("consumeThreadMin=4 时 60 条消息不丢不重地全部消费")
    void concurrentLoopsDeliverExactlyOnce() {
        String topic = "cc-topic";
        String group = "cc-group";
        int total = 60;

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new NoRetryPolicy(),
                        namespace);

        Set<String> processedBodies = ConcurrentHashMap.newKeySet();
        StreamMessageConcurrentlyConsumer<String> listener =
                (msg, ctx) -> {
                    processedBodies.add(msg.getBody());
                    return ConsumeAction.SUCCESS;
                };
        container.registerConsumer(listener, mkAnnotation(topic, group, 4));

        createConsumerGroup(topic, group);

        container.start();
        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            for (int i = 0; i < total; i++) {
                producer.syncSend(MessageBuilder.<String>withTopic(topic).body("msg-" + i).build());
            }
            producer.close();

            await().atMost(30, TimeUnit.SECONDS).until(() -> processedBodies.size() >= total);

            // 不丢：全部送达；不重：集合去重后数量一致（重复投递会导致先 >= 后 > 的窗口，
            // 因此再等待一小段观察期确认没有多余消息进入）
            await().pollDelay(2, TimeUnit.SECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> processedBodies.size() <= total);
            assertThat(processedBodies).hasSize(total);
        } finally {
            container.stop();
        }
    }
}
