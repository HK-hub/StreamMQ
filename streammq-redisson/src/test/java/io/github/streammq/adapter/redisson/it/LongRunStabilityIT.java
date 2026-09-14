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
 * 长跑稳定性测试（红队审查 P2-6）：在较高并发度下持续灌入较大批量消息，验证（1）全部送达（不丢）、（2）稳定态无重复投递（不重）。
 *
 * <p>与 {@link ConcurrentConsumeIT} 的区别在于放大体量（500 条）与并发度（3），更贴近长跑/突增场景的线程与 PEL 压力。
 */
@DisplayName("长跑稳定性：高并发大体量不丢不重")
class LongRunStabilityIT extends AbstractRedisIT {

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
    @DisplayName("500 条消息 / 并发 3 稳定送达且不重")
    void sustainedLoadDeliversExactlyOnce() {
        String topic = "lr-topic";
        String group = "lr-group";
        int total = 500;

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
        container.registerConsumer(listener, mkAnnotation(topic, group, 3));

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

            await().atMost(90, TimeUnit.SECONDS).until(() -> processedBodies.size() >= total);

            // 稳定态观察窗口：确认没有多余（重复）消息在投递后继续进入
            await().pollDelay(2, TimeUnit.SECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> processedBodies.size() <= total);
            assertThat(processedBodies).hasSize(total);
        } finally {
            container.stop();
        }
    }
}
