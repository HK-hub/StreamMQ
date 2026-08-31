/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.transaction.TransactionChecker;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.client.codec.StringCodec;

/**
 * 发布前红队审查修复的回归测试。
 *
 * <p>覆盖：事务执行权锁 TTL 接管、HALF_MISSING 有界回查终结、重试 payload 跨 Topic 隔离、 广播组停止后持久（不重放）、PelClaim
 * 对活消费者的分片锁保护。
 */
@DisplayName("红队审查修复回归测试")
class RedTeamRegressionIT extends AbstractRedisIT {

    private static final String TARGET_FIELD = "body";

    private Map<String, String> fieldsOf(String body) {
        return converter.toStreamFields(MessageBuilder.<String>withTopic("t").body(body).build());
    }

    @SuppressWarnings("unchecked")
    private TransactionScanner newScanner(long intervalMs, int maxCheckTimes) {
        TransactionScanner scanner =
                new TransactionScanner(
                        redisson, namespace, converter, intervalMs, maxCheckTimes, 32);
        scanner.registerChecker("any-group", (TransactionChecker<Object>) (message, ctx) -> null);
        return scanner;
    }

    private String stateOf(String txGroup, String txId) {
        return redisson.<String, String>getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup))
                .get(txId);
    }

    @Test
    @DisplayName("P0 回归：并发 markCommit 原子去重，恰好投递一次、无重复窗口")
    void concurrentCommit_noDuplicateDelivery() throws Exception {
        String txGroup = "tx-concurrent-group";
        String txId = "tx-concurrent-1";
        String targetTopic = "tx-concurrent-target";
        TransactionScanner scanner = newScanner(150, 10);
        try {
            scanner.registerHalfMessage(txId, txGroup, targetTopic, fieldsOf("race"));

            // 两个线程同时提交。旧实现依赖执行权锁 + TTL，锁在原子批执行期间过期时另一实例
            // 会重复转投（已知缺陷）；新实现单 Lua 脚本原子完成 XRANGE+XADD+XDEL+HSET COMMIT，
            // 后执行者读到 HALF_MISSING 且不覆盖终态，天然去重——此即去锁化的回归防护。
            CountDownLatch start = new CountDownLatch(1);
            int contenders = 2;
            CountDownLatch done = new CountDownLatch(contenders);
            for (int i = 0; i < contenders; i++) {
                new Thread(
                                () -> {
                                    try {
                                        start.await();
                                        scanner.markCommit(txId, txGroup);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    } finally {
                                        done.countDown();
                                    }
                                },
                                "tx-commit-contender-" + i)
                        .start();
            }
            start.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).as("并发提交应在 10s 内完成").isTrue();

            // 终态一致且恰好投递一次：不出现重复消息，也不残留半消息
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> TransactionScanner.STATE_COMMIT.equals(stateOf(txGroup, txId)));
            assertThat(targetSize(targetTopic)).isEqualTo(1);
            RStream<String, String> half =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, txGroup));
            assertThat(half.size()).isZero();
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("P1 回归：提交时半消息缺失降级有界回查，最终以 ROLLBACK 安全终结而非伪 COMMIT")
    void halfMissing_boundedRecheck_endsAsRollback() {
        String txGroup = "tx-halfmiss-group";
        String txId = "tx-halfmiss-1";
        String targetTopic = "tx-halfmiss-target";

        // 注册半消息（scanner 未启动，仅写入状态/调度/半消息）
        TransactionScanner registrar = newScanner(150, 3);
        var halfId = registrar.registerHalfMessage(txId, txGroup, targetTopic, fieldsOf("vanish"));

        // 半消息在提交前消失（模拟裁剪 / 崩溃窗口残留）
        redisson.getStream(StreamMQKeys.halfStream(namespace, txGroup)).remove(halfId);

        TransactionScanner scanner = newScanner(150, 3);
        scanner.registerChecker(
                txGroup,
                (io.github.streammq.core.transaction.TransactionChecker<Object>)
                        (message, ctx) ->
                                io.github.streammq.core.enums.LocalTransactionState.COMMIT_MESSAGE);
        scanner.start();
        try {
            // 直接触发模板路径的显式提交：publishHalfAndMarkCommit 应返回 HALF_MISSING
            scanner.markCommit(txId, txGroup);

            // 不允许出现伪 COMMIT；有界回查耗尽后必须以 ROLLBACK 终结
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> TransactionScanner.STATE_ROLLBACK.equals(stateOf(txGroup, txId)));
            assertThat(targetSize(targetTopic)).isZero();
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("P0 回归：COMMITTING 状态不可被并发 ROLLBACK 覆盖")
    void intermediateCommitState_cannotBeOverwrittenByRollback() {
        String txGroup = "tx-state-race-group";
        String txId = "tx-state-race-1";
        String targetTopic = "tx-state-race-target";
        TransactionScanner scanner = newScanner(150, 3);
        try {
            scanner.registerHalfMessage(txId, txGroup, targetTopic, fieldsOf("race"));
            // 模拟另一实例已原子抢占事务（casState 置位 COMMITTING）、正在执行转投脚本的中间窗口。
            // 去锁化后状态机 CAS 是唯一串行化手段——ROLLBACK 竞争者必须看到 COMMITTING 并让路，
            // 不得覆盖为 ROLLBACKING/ROLLBACK。
            RMap<String, String> stateMap =
                    redisson.getMap(
                            StreamMQKeys.transactionStateHash(namespace, txGroup),
                            StringCodec.INSTANCE);
            stateMap.put(txId, TransactionScanner.STATE_COMMITTING);

            scanner.markRollback(txId, txGroup);
            assertThat(stateOf(txGroup, txId))
                    .as("a rollback contender must not overwrite an in-flight commit")
                    .isEqualTo(TransactionScanner.STATE_COMMITTING);
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("P1 回归：多 Topic 并发重试时 payload 相互隔离，不发生跨 Topic 错投")
    void retryPayload_isolatedAcrossTopics() throws Exception {
        String topicA = "iso-topic-a";
        String topicB = "iso-topic-b";
        String groupA = "iso-group-a";
        String groupB = "iso-group-b";

        var factory =
                new io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory(
                        redisson, converter);
        var policy = new RetryAndDlqIT.FastRetryPolicy(60_000, 16);
        var container =
                new io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer(
                        redisson, factory, converter, policy, namespace);
        io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer<String> failing =
                (msg, ctx) -> {
                    throw new RuntimeException("fail");
                };
        container.registerConsumer(failing, RetryAndDlqIT.annotationOf(topicA, groupA, 16));
        container.registerConsumer(failing, RetryAndDlqIT.annotationOf(topicB, groupB, 16));
        createConsumerGroup(topicA, groupA);
        createConsumerGroup(topicB, groupB);
        container.start();
        try {
            RedissonStreamProducer producer =
                    new RedissonStreamProducer(
                            redisson, namespace, "iso-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topicA).body("BODY-A").build());
            producer.syncSend(MessageBuilder.<String>withTopic(topicB).body("BODY-B").build());
            producer.close();

            String zsetA = StreamMQKeys.retryZSet(namespace, topicA, groupA);
            String zsetB = StreamMQKeys.retryZSet(namespace, topicB, groupB);
            await().atMost(10, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    redisson.getScoredSortedSet(zsetA).size() >= 1
                                            && redisson.getScoredSortedSet(zsetB).size() >= 1);

            String idA = redisson.<String>getScoredSortedSet(zsetA).first();
            String idB = redisson.<String>getScoredSortedSet(zsetB).first();
            String keyA = StreamMQKeys.retryPayloadHash(namespace, topicA, groupA, idA);
            String keyB = StreamMQKeys.retryPayloadHash(namespace, topicB, groupB, idB);
            assertThat(keyA).isNotEqualTo(keyB);
            Map<String, String> payloadA = redisson.<String, String>getMap(keyA).readAllMap();
            Map<String, String> payloadB = redisson.<String, String>getMap(keyB).readAllMap();
            // 各自 payload 必须是自己的消息体（body 经 Base64(JSON) 编码，比较编码后的一致性）；
            // 旧实现同 Entry-ID 时两键相同，后写覆盖先写导致错投
            String expectedA =
                    converter
                            .toStreamFields(
                                    MessageBuilder.<String>withTopic(topicA).body("BODY-A").build())
                            .get(TARGET_FIELD);
            String expectedB =
                    converter
                            .toStreamFields(
                                    MessageBuilder.<String>withTopic(topicB).body("BODY-B").build())
                            .get(TARGET_FIELD);
            assertThat(payloadA.get(TARGET_FIELD)).isEqualTo(expectedA);
            assertThat(payloadB.get(TARGET_FIELD)).isEqualTo(expectedB);
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("P1 回归：广播监听器关闭不再销毁组，重启从原位点继续且不重放历史")
    void broadcastGroup_survivesClose_noReplayOnRestart() {
        String topic = "bc-topic";
        String group = "bc-group";
        String consumer = "bc-consumer-1";
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, "bc-p", converter, 3000L, 0, 0, 0);
        producer.syncSend(MessageBuilder.<String>withTopic(topic).body("M1").build());

        RedissonStreamListener first = newListener(topic, group, consumer);
        var got1 = first.pull(10);
        assertThat(got1).hasSize(1); // 组创建于 0-0，收到 M1
        first.ack(got1.get(0).getMessageId());
        first.close();

        // 关键断言：close 后组仍存在（旧实现 removeGroup 导致 PEL 丢失 + 重启重放）
        RStream<String, String> stream = redisson.getStream(streamKey);
        assertThat(stream.listGroups())
                .anySatisfy(g -> assertThat(g.getName()).hasToString(group + ":" + consumer));

        producer.syncSend(MessageBuilder.<String>withTopic(topic).body("M2").build());
        producer.close();

        // 重启同名消费者：只应收到新消息 M2，不重放 M1
        RedissonStreamListener second = newListener(topic, group, consumer);
        var got2 = second.pull(10);
        assertThat(got2).hasSize(1);
        assertThat((String) got2.get(0).getBody()).contains("M2");
        second.ack(got2.get(0).getMessageId());
        second.close();
    }

    private RedissonStreamListener newListener(String topic, String group, String consumer) {
        return RedissonStreamListener.builder()
                .redisson(redisson)
                .namespace(namespace)
                .topic(topic)
                .group(group)
                .consumerName(consumer)
                .converter(converter)
                .broadcast(true)
                .build();
    }

    @Test
    @DisplayName("P1 回归：ORDERLY 分片锁被持有时 PelClaim 跳过认领，释放后才重投")
    void pelClaim_respectsHeldShardLock() {
        String topic = "pl-topic";
        String group = "pl-group";
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        RStream<String, String> stream = redisson.getStream(streamKey);
        stream.add(StreamAddArgsEntries.entries(Map.of("f", "v")));
        stream.createGroup(
                org.redisson.api.stream.StreamCreateGroupArgs.name(group)
                        .makeStream()
                        .id(new org.redisson.api.StreamMessageId(0, 0)));
        // 以消费者 c1 读取制造 PEL pending（不 ACK）
        stream.readGroup(
                group,
                "c1",
                org.redisson.api.stream.StreamReadGroupArgs.neverDelivered().count(10));

        PelClaimScheduler scheduler = new PelClaimScheduler(redisson, namespace, 100, 32, 50);
        scheduler.registerTarget(topic, group, 16, true, 1);
        scheduler.start();
        try {
            RLock shardLock = redisson.getLock(StreamMQKeys.shardLock(namespace, topic, group, 0));
            shardLock.lock();
            try {
                long sizeDuringProcessing = stream.size();
                org.awaitility.Awaitility.await()
                        .pollDelay(Duration.ofMillis(400))
                        .atMost(1, TimeUnit.SECONDS)
                        .until(() -> stream.size() == sizeDuringProcessing);
                assertThat(stream.size()).isEqualTo(sizeDuringProcessing); // 未被复制重投
            } finally {
                if (shardLock.isHeldByCurrentThread()) {
                    shardLock.unlock();
                }
            }
            // 锁释放后应完成「XADD 副本 + ACK 原条目」的重投
            org.awaitility.Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(() -> stream.size() == 2);
        } finally {
            scheduler.stop();
        }
    }

    private long targetSize(String topic) {
        RStream<String, String> s = redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        try {
            return s.size();
        } catch (RuntimeException ex) {
            return 0;
        }
    }

    /** StreamAddArgs 的静态导入替代（避免与用例内其他 import 冲突）。 */
    private static final class StreamAddArgsEntries {
        static <K, V> org.redisson.api.stream.StreamAddArgs<K, V> entries(Map<K, V> map) {
            return org.redisson.api.stream.StreamAddArgs.entries(map);
        }
    }
}
