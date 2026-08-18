package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

/**
 * 延时消息 Redis 联动集成测试。
 *
 * <p>实际操作本地 Redis（{@code localhost:6379}），覆盖 {@link DelayMessageScheduler} 与 {@link
 * RedissonStreamProducer} 的延时消息全流程：发送、调度、转投、清理。
 *
 * <p>每个测试方法使用独立 namespace（由 {@link AbstractRedisIT} 提供），避免相互干扰。
 */
@DisplayName("延时消息 Redis 集成测试")
class DelayMessageIT extends AbstractRedisIT {

    /** 调度器扫描间隔（毫秒），使用较短间隔以加速测试 */
    private static final long SCAN_INTERVAL_MS = 200L;

    /** 默认批量大小 */
    private static final int BATCH_SIZE = 100;

    /** Awaitility 最大等待秒数 */
    private static final long AWAIT_SECONDS = 10L;

    @Test
    @DisplayName("发送延时消息后立即检查目标 Stream 无消息")
    void testDelayMessageNotInTargetStreamImmediately() {
        String topic = "delay-immediate-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-immediate-group");

        Message<String> msg =
                MessageBuilder.<String>withTopic(topic)
                        .tag("immediate")
                        .body("payload-immediate")
                        .delayLevel(DelayLevel.SECOND_1)
                        .build();

        SendResult result = producer.syncSend(msg);
        assertThat(result).isNotNull();
        assertThat(result.getTopic()).isEqualTo(topic);

        // 立即检查目标 Stream 应为空
        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        assertThat(stream.size()).isZero();

        // ZSet 中应有 1 条
        RScoredSortedSet<String> zset =
                redisson.getScoredSortedSet(
                        StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));
        assertThat(zset.size()).isEqualTo(1);

        producer.close();
    }

    @Test
    @DisplayName("启动调度器后 1 秒级延时消息被转投到目标 Stream")
    void testDelayMessageTransferredToTargetStream() {
        String topic = "delay-transfer-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-transfer-group");
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(redisson, namespace, SCAN_INTERVAL_MS, BATCH_SIZE);

        try {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic)
                            .tag("transfer")
                            .body("payload-transfer")
                            .delayLevel(DelayLevel.SECOND_1)
                            .build();

            producer.syncSend(msg);

            scheduler.start();

            // 等待目标 Stream 出现消息
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, topic));
                                return stream.size() > 0;
                            });

            // 验证 Stream 内容
            RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            Map<StreamMessageId, Map<String, String>> entries =
                    stream.range(10, StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(entries).hasSize(1);
            Map<String, String> fields = entries.values().iterator().next();
            // 验证 body 字段非空（Base64 编码后的 JSON）
            assertThat(fields.get("body")).isNotEmpty();
            // 验证 tag 一致
            assertThat(fields.get("tag")).isEqualTo("transfer");
        } finally {
            scheduler.stop();
            producer.close();
        }
    }

    @Test
    @DisplayName("延时消息投递后 ZSet 中的 msgId 被移除")
    void testMsgIdRemovedFromZSetAfterDelivery() {
        String topic = "delay-zset-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-zset-group");
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(redisson, namespace, SCAN_INTERVAL_MS, BATCH_SIZE);

        try {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic)
                            .body("payload-zset")
                            .delayLevel(DelayLevel.SECOND_1)
                            .build();

            producer.syncSend(msg);

            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));
            assertThat(zset.size()).isEqualTo(1);
            String msgIdBefore = zset.iterator().next();

            scheduler.start();

            // 等待 ZSet 被清空
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RScoredSortedSet<String> z =
                                        redisson.getScoredSortedSet(
                                                StreamMQKeys.delayZSet(
                                                        namespace, DelayLevel.SECOND_1.name()));
                                return z.size() == 0;
                            });

            // 验证 msgId 已不在 ZSet 中
            assertThat(zset.contains(msgIdBefore)).isFalse();
        } finally {
            scheduler.stop();
            producer.close();
        }
    }

    @Test
    @DisplayName("延时消息投递后 payload Hash 被清理")
    void testPayloadHashCleanedAfterDelivery() {
        String topic = "delay-payload-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-payload-group");
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(redisson, namespace, SCAN_INTERVAL_MS, BATCH_SIZE);

        try {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic)
                            .body("payload-hash")
                            .delayLevel(DelayLevel.SECOND_1)
                            .build();

            producer.syncSend(msg);

            // 投递前应存在 payload Hash（通过 ZSet member 反查）
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));
            String msgId = zset.iterator().next();
            String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
            RMap<String, String> payloadMap = redisson.getMap(payloadKey);
            assertThat(payloadMap.isExists()).isTrue();
            assertThat(payloadMap.get("targetTopic")).isEqualTo(topic);

            scheduler.start();

            // 等待 payload Hash 被删除
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RMap<String, String> pm = redisson.getMap(payloadKey);
                                return !pm.isExists();
                            });

            assertThat(redisson.getMap(payloadKey).isExists()).isFalse();
        } finally {
            scheduler.stop();
            producer.close();
        }
    }

    @Test
    @DisplayName("多条延时消息并发投递全部转投到目标 Stream")
    void testMultipleDelayMessagesAllDelivered() {
        String topic = "delay-multi-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-multi-group");
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(redisson, namespace, SCAN_INTERVAL_MS, BATCH_SIZE);
        int count = 3;

        try {
            for (int i = 0; i < count; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic)
                                .tag("multi-" + i)
                                .body("payload-multi-" + i)
                                .delayLevel(DelayLevel.SECOND_1)
                                .build();
                producer.syncSend(msg);
            }

            // 验证 ZSet 中有 3 条
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));
            assertThat(zset.size()).isEqualTo(count);

            scheduler.start();

            // 等待所有消息被转投
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, topic));
                                return stream.size() >= count;
                            });

            RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            Map<StreamMessageId, Map<String, String>> entries =
                    stream.range(count, StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(entries).hasSize(count);

            // ZSet 应被清空
            assertThat(zset.size()).isZero();
        } finally {
            scheduler.stop();
            producer.close();
        }
    }

    @Test
    @DisplayName("不同级别延时消息（1 级和 5 级）按时序投递")
    void testDifferentDelayLevelsDeliveredInOrder() {
        String topicLevel1 = "delay-l1-" + UUID.randomUUID().toString().substring(0, 6);
        String topicLevel5 = "delay-l5-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-level-group");
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(redisson, namespace, SCAN_INTERVAL_MS, BATCH_SIZE);

        try {
            // 同时发送 1 秒级和 5 秒级延时消息
            Message<String> msg1 =
                    MessageBuilder.<String>withTopic(topicLevel1)
                            .tag("level1")
                            .body("payload-l1")
                            .delayLevel(DelayLevel.SECOND_1)
                            .build();
            Message<String> msg5 =
                    MessageBuilder.<String>withTopic(topicLevel5)
                            .tag("level5")
                            .body("payload-l5")
                            .delayLevel(DelayLevel.SECOND_5)
                            .build();

            producer.syncSend(msg1);
            producer.syncSend(msg5);

            scheduler.start();

            // 1 秒级应先到达
            await().atMost(3, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, topicLevel1));
                                return stream.size() > 0;
                            });

            // 此时 5 秒级应尚未到达
            RStream<String, String> streamL5Before =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topicLevel5));
            assertThat(streamL5Before.size()).isZero();

            // 等待 5 秒级到达
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, topicLevel5));
                                return stream.size() > 0;
                            });

            // 两个目标 Stream 均有消息
            RStream<String, String> streamL1 =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topicLevel1));
            RStream<String, String> streamL5 =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topicLevel5));
            assertThat(streamL1.size()).isEqualTo(1);
            assertThat(streamL5.size()).isEqualTo(1);
        } finally {
            scheduler.stop();
            producer.close();
        }
    }

    @Test
    @DisplayName("延时消息投递后 ZSet 大小归零（投递计数验证）")
    void testZSetSizeReturnsToZeroAfterDelivery() {
        String topic = "delay-count-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-count-group");
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(redisson, namespace, SCAN_INTERVAL_MS, BATCH_SIZE);
        int total = 5;

        try {
            // 发送 5 条延时消息
            for (int i = 0; i < total; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic)
                                .body("payload-count-" + i)
                                .delayLevel(DelayLevel.SECOND_1)
                                .build();
                producer.syncSend(msg);
            }

            AtomicInteger transferred = new AtomicInteger(0);
            // 启动调度器后周期统计已投递数量
            scheduler.start();

            // 等待全部投递完成
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RScoredSortedSet<String> zset =
                                        redisson.getScoredSortedSet(
                                                StreamMQKeys.delayZSet(
                                                        namespace, DelayLevel.SECOND_1.name()));
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, topic));
                                return zset.size() == 0 && stream.size() >= total;
                            });

            // 最终 ZSet 为空,目标 Stream 有 total 条
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));
            RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            assertThat(zset.size()).isZero();
            assertThat(stream.size()).isEqualTo(total);

            transferred.set(total);
            assertThat(transferred.get()).isEqualTo(total);
        } finally {
            scheduler.stop();
            producer.close();
        }
    }

    @Test
    @DisplayName("同时设置 delayLevel 与 delayTimeMillis 时，delayTimeMillis 优先（写入 custom ZSet）")
    void testDelayTimeMillisTakesPrecedenceWhenBothSet() {
        String topic = "delay-both-" + UUID.randomUUID().toString().substring(0, 6);
        RedissonStreamProducer producer = newProducer("delay-both-group");

        try {
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic)
                            .body("both-set")
                            .delayLevel(DelayLevel.SECOND_1)
                            .delayTimeMillis(10_000L)
                            .build();

            producer.syncSend(msg);

            // 应进入 custom ZSet（任意延时），而非 SECOND_1 级别 ZSet
            RScoredSortedSet<String> customZset =
                    redisson.getScoredSortedSet(StreamMQKeys.delayCustomZSet(namespace));
            RScoredSortedSet<String> levelZset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));

            assertThat(customZset.size()).isEqualTo(1);
            assertThat(levelZset.size()).isZero();
        } finally {
            producer.close();
        }
    }

    // ===================== 辅助方法 =====================

    /**
     * 创建一个新的 Producer，使用当前测试命名空间。
     *
     * @param group 生产组名
     * @return Producer 实例
     */
    private RedissonStreamProducer newProducer(String group) {
        return new RedissonStreamProducer(redisson, namespace, group, converter, 3000L, 0, 0, 0);
    }
}
