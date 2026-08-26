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
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.lang.reflect.Proxy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SQL92 用户属性过滤端到端回归测试（红队审查 F-05-02）。
 *
 * <p>历史缺陷：解码后所有属性落在用户属性表，而 {@code PropertyExpression} 只读系统属性表， 导致按文档示例使用 SQL92
 * 选择器的消费者静默收不到任何消息。修复后属性查找回退到用户属性。
 */
@DisplayName("SQL92 用户属性过滤回归测试")
class Sql92UserPropertyIT extends AbstractRedisIT {

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
    private StreamMQConsumer mkSqlAnnotation(String topic, String group) {
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
                                    case "selectorType" -> SelectorType.SQL92;
                                    case "selectorExpression" -> "amount > 100";
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
    @DisplayName("withUserProperty 写入的属性可被 SQL92 表达式匹配（此前永不匹配）")
    void sql92MatchesUserProperty() {
        String topic = "sql92-topic";
        String group = "sql92-group";

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new NoRetryPolicy(),
                        namespace);

        Set<String> receivedBodies = ConcurrentHashMap.newKeySet();
        StreamMessageConcurrentlyConsumer<String> listener =
                (msg, ctx) -> {
                    Object amount = msg.getUserProperties().get("amount");
                    receivedBodies.add(msg.getBody() + ":" + amount);
                    return ConsumeAction.SUCCESS;
                };
        container.registerConsumer(listener, mkSqlAnnotation(topic, group));

        createConsumerGroup(topic, group);

        container.start();
        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(
                    MessageBuilder.<String>withTopic(topic)
                            .body("big-order")
                            .withUserProperty("amount", "500")
                            .build());
            producer.syncSend(
                    MessageBuilder.<String>withTopic(topic)
                            .body("small-order")
                            .withUserProperty("amount", "50")
                            .build());
            producer.close();

            await().atMost(20, TimeUnit.SECONDS)
                    .until(() -> receivedBodies.contains("big-order:500"));

            // 给潜在误投留出观察窗口后断言：不匹配消息不得送达
            await().pollDelay(2, TimeUnit.SECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> !receivedBodies.contains("small-order:50"));
            assertThat(receivedBodies).containsExactly("big-order:500");
        } finally {
            container.stop();
        }
    }
}
