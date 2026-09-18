/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;

/**
 * {@link ConsumeAction#defer(Duration)} 延迟重投端到端集成测试（红队 U-11 缺口）。
 *
 * <p>覆盖 {@code DefaultRetryAndDlqHandler#handleDefer} 的设计语义：
 *
 * <ul>
 *   <li>延迟生效：消息按指定延迟重新投递（既不是立即重投，也不是永久不投）
 *   <li>不消耗重试预算：DEFER 调度写 {@code __deferred=true} 且不递增 {@code retryTimes}
 *   <li>RetryScheduler 转投时不做 MAX_RETRY 判定，延迟重投不占用失败重试配额
 *   <li>调度元数据落在 retry ZSet + payload Hash（复用 {@link RetryScheduler} 调度通道）
 * </ul>
 */
@DisplayName("ConsumeAction.defer 延迟重投集成测试")
class DeferActionIT extends AbstractRedisIT {

    private static final String TOPIC = "defer-action-topic";
    private static final String GROUP = "defer-action-group";
    private static final Duration DEFER_DELAY = Duration.ofSeconds(1);

    @Test
    @DisplayName("defer(1s)：延迟后重投且不消耗重试预算，期间不进入 DLQ")
    void deferAction_reDeliveredAfterDelay_withoutConsumingRetryBudget() {
        String topic = TOPIC + "-redeliver";
        String group = GROUP + "-redeliver";
        AtomicInteger attempts = new AtomicInteger();
        List<Message<String>> deliveries = new CopyOnWriteArrayList<>();
        long[] firstDeliveredAt = new long[1];
        StreamMessageConcurrentlyConsumer<String> listener =
                (message, context) -> {
                    deliveries.add(message);
                    if (attempts.incrementAndGet() == 1) {
                        firstDeliveredAt[0] = System.currentTimeMillis();
                        return ConsumeAction.defer(DEFER_DELAY);
                    }
                    return ConsumeAction.SUCCESS;
                };

        // maxReconsumeTimes=0：若 DEFER 消耗重试预算，retryCount(0) >= 0 会直接把消息送进 DLQ
        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new RetryAndDlqIT.FastRetryPolicy(100L, 10),
                        namespace);
        container.registerConsumer(listener, RetryAndDlqIT.annotationOf(topic, group, 0));
        RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10);
        container.registerRetryTargets(scheduler);
        createConsumerGroup(topic, group);
        scheduler.start();
        container.start();

        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("defer-body").build());
            producer.close();

            // ① 第二条投递发生在延迟到期之后：1s 延迟 + 扫描/派发余量（含消费循环启动与首次拉取延迟）
            await().atMost(15, TimeUnit.SECONDS).until(() -> attempts.get() >= 2);
            long gapMillis = System.currentTimeMillis() - firstDeliveredAt[0];
            assertThat(gapMillis).isGreaterThanOrEqualTo(500L);

            // ③ 不消耗重试预算：两次投递的 reconsumeTimes 均为 0（普通重试会递增为 1）
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(deliveries).hasSize(2);
            assertThat(deliveries.get(0).getReconsumeTimes()).isZero();
            assertThat(deliveries.get(1).getReconsumeTimes()).isZero();
            assertThat(deliveries.get(1).getBody()).isEqualTo("defer-body");

            // ② 期间与事后均未进入 DLQ
            RStream<String, String> dlqStream =
                    redisson.getStream(StreamMQKeys.dlqStream(namespace, group));
            assertThat(dlqStream.size()).isZero();

            // 成功消费后 ACK：PEL 与 retry 调度条目均已清空
            RStream<String, String> topicStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(
                                                    topicStream.listPending(
                                                            group,
                                                            StreamMessageId.MIN,
                                                            StreamMessageId.MAX,
                                                            100))
                                            .isEmpty());
            RScoredSortedSet<String> retryZset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.retryZSet(namespace, topic, group), StringCodec.INSTANCE);
            assertThat(retryZset.size()).isZero();
        } finally {
            container.stop();
            scheduler.stop();
        }
    }

    @Test
    @DisplayName("defer 调度元数据：retry ZSet 记 score=now+delay，payload 写 __deferred 且不递增 retryTimes")
    void deferAction_schedulesThroughRetryZsetWithDeferredMarker() {
        String topic = TOPIC + "-metadata";
        String group = GROUP + "-metadata";
        Duration longDelay = Duration.ofSeconds(5);
        StreamMessageConcurrentlyConsumer<String> listener =
                (message, context) -> ConsumeAction.defer(longDelay);

        DefaultStreamMQListenerContainer container =
                new DefaultStreamMQListenerContainer(
                        redisson,
                        new RedissonStreamListenerFactory(redisson, converter),
                        converter,
                        new RetryAndDlqIT.FastRetryPolicy(100L, 10),
                        namespace);
        container.registerConsumer(listener, RetryAndDlqIT.annotationOf(topic, group, 0));
        RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10);
        container.registerRetryTargets(scheduler);
        createConsumerGroup(topic, group);
        scheduler.start();
        container.start();

        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            long before = System.currentTimeMillis();
            producer.syncSend(
                    MessageBuilder.<String>withTopic(topic).body("defer-meta-body").build());
            producer.close();

            // 5s 延迟远大于本测试观察窗口：调度条目在断言期间不会被转移
            RScoredSortedSet<String> retryZset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.retryZSet(namespace, topic, group), StringCodec.INSTANCE);
            await().pollInterval(Duration.ofMillis(50))
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> retryZset.size() == 1);

            String msgId = retryZset.iterator().next();
            // defer 分数 = handler 执行时刻 + 5s：至少比发送时刻晚 3s（证明是"延迟"而非"立即重投"），
            // 且不超过"当前时刻 + 5s + 余量"（消费循环启动延迟不影响上界语义）
            assertThat(retryZset.getScore(msgId))
                    .isGreaterThanOrEqualTo((double) before + 3_000)
                    .isLessThanOrEqualTo(System.currentTimeMillis() + 5_200);

            RMap<String, String> payload =
                    redisson.getMap(
                            StreamMQKeys.retryPayloadHash(namespace, topic, group, msgId),
                            StringCodec.INSTANCE);
            assertThat(payload.readAllMap())
                    .containsEntry(StreamMQConstants.FIELD_DEFERRED, "true")
                    .containsEntry(RetryScheduler.FIELD_RETRY_COUNT, "0")
                    .containsEntry(RetryScheduler.FIELD_TARGET_TOPIC, topic)
                    // DEFER 不占用 retryTimes：payload 中不存在该字段（普通重试会写入递增后的值）
                    .doesNotContainKey(DefaultMessageConverter.FIELD_RETRY_TIMES);

            // 延迟未到期：消息尚未重投，也未进入 DLQ
            RStream<String, String> dlqStream =
                    redisson.getStream(StreamMQKeys.dlqStream(namespace, group));
            assertThat(dlqStream.size()).isZero();
        } finally {
            container.stop();
            scheduler.stop();
        }
    }
}
