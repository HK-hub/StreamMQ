/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.message.MessageBuilder;
import java.lang.reflect.Proxy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScoredSortedSet;

/**
 * 广播暂停期心跳保活回归测试（发布前红队审查 R9）。
 *
 * <p>历史缺陷：广播组心跳仅在拉取时刷新（doRead）；容器暂停期间读循环不拉取， 停留超过 {@code BROADCAST_GROUP_STALE_TTL_MS}
 * 后僵尸组回收任务销毁该组，resume 时全量重放历史。 修复后消费循环在暂停休眠周期内补发心跳，暂停再久也不会被误回收。
 */
@DisplayName("广播暂停期心跳保活集成测试")
class BroadcastPauseHeartbeatIT extends AbstractRedisIT {

    @SuppressWarnings("unchecked")
    private StreamMQConsumer mkBroadcastAnnotation(String topic, String group) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "topic" -> topic;
                                    case "consumerGroup" -> group;
                                    case "consumeMode" -> ConsumeMode.BROADCASTING;
                                    case "selectorExpression" -> "*";
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
        if (returnType == Class[].class) return new Class<?>[0];
        return null;
    }

    @Test
    @DisplayName("暂停的广播消费者持续刷新心跳不被回收；resume 只收新消息不重放")
    void pausedBroadcastGroupKeepsHeartbeat_noReplayOnResume() throws Exception {
        String topic = "bp-topic";
        String group = "bp-group";

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new Sql92UserPropertyIT.NoRetryPolicy(),
                        namespace);

        Set<String> receivedBodies = ConcurrentHashMap.newKeySet();
        StreamMessageConcurrentlyConsumer<String> listener =
                (msg, ctx) -> {
                    receivedBodies.add((String) msg.getBody());
                    return ConsumeAction.SUCCESS;
                };
        container.registerConsumer(listener, mkBroadcastAnnotation(topic, group));

        RedissonStreamProducer producer =
                new RedissonStreamProducer(
                        redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
        producer.syncSend(MessageBuilder.<String>withTopic(topic).body("M1").build());

        container.start();
        try {
            await().atMost(15, TimeUnit.SECONDS).until(() -> receivedBodies.contains("M1"));

            // 进入暂停：读循环停止拉取（旧实现此处心跳随之停止）
            container.pause();

            RScoredSortedSet<String> registry =
                    redisson.<String>getScoredSortedSet(StreamMQKeys.broadcastRegistry(namespace));
            // 将注册表条目回拨到"刚过期"水位：若无暂停期心跳，下一次 sweep 必然回收
            long staleCutoff =
                    System.currentTimeMillis()
                            - 2 * RedissonStreamListener.BROADCAST_GROUP_STALE_TTL_MS;
            for (String member :
                    registry.valueRange(
                            Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true)) {
                if (member.endsWith(":" + group)) {
                    registry.add(staleCutoff, member);
                }
            }

            // 暂停期心跳应把 score 刷回当前时间附近
            await().atMost(10, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    registry
                                            .valueRange(
                                                    Double.NEGATIVE_INFINITY,
                                                    true,
                                                    Double.POSITIVE_INFINITY,
                                                    true)
                                            .stream()
                                            .filter(m -> m.endsWith(":" + group))
                                            .allMatch(
                                                    m -> {
                                                        Double s = registry.getScore(m);
                                                        return s != null
                                                                && s
                                                                        > System.currentTimeMillis()
                                                                                - 60_000L;
                                                    }));

            // 僵尸回收显式触发：心跳活跃 ⇒ 组必须存活
            int swept = RedissonStreamListener.sweepStaleBroadcastGroups(redisson, namespace);
            assertThat(swept).isZero();
            assertThat(redisson.getStream(StreamMQKeys.topicStream(namespace, topic)).listGroups())
                    .anySatisfy(
                            g ->
                                    org.assertj.core.api.Assertions.assertThat(g.getName())
                                            .contains(group));

            // resume 后只收到新消息 M2，绝不重放 M1（组未被销毁、位点未重置）
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("M2").build());
            container.resume();
            await().atMost(15, TimeUnit.SECONDS).until(() -> receivedBodies.contains("M2"));
            assertThat(receivedBodies).containsExactlyInAnyOrder("M1", "M2");
        } finally {
            container.stop();
            producer.close();
        }
    }
}
