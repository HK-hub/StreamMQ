/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.transaction.TransactionChecker;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

/**
 * 事务消息 Redis 联动集成测试。
 *
 * <p>实际操作本地 Redis（{@code localhost:6379}），覆盖 {@link TransactionScanner} 的 半消息注册、显式
 * COMMIT/ROLLBACK、回查触发、{@link TransactionChecker} 回调等全流程。
 *
 * <p>测试策略：
 *
 * <ul>
 *   <li>直接调用 {@link TransactionScanner#registerHalfMessage} 写入半消息
 *   <li>调用 {@link TransactionScanner#markCommit} / {@link TransactionScanner#markRollback} 验证显式终结
 *   <li>启动调度器并注册 {@link TransactionChecker},验证回查触发与状态流转
 * </ul>
 *
 * <p>每个测试方法使用独立 namespace（由 {@link AbstractRedisIT} 提供），避免相互干扰。
 */
@DisplayName("事务消息 Redis 集成测试")
class TransactionMessageIT extends AbstractRedisIT {

    /** 回查间隔（毫秒），足够大以避免断言期间的竞态条件 */
    private static final long CHECK_INTERVAL_MS = 1500L;

    /** 最大回查次数 */
    private static final int MAX_CHECK_TIMES = 3;

    /** 单次扫描批量 */
    private static final int BATCH_SIZE = 100;

    /** Awaitility 最大等待秒数 */
    private static final long AWAIT_SECONDS = 10L;

    /** 测试用事务组名 */
    private static final String TX_GROUP = "tx-it-group";

    @Test
    @DisplayName("半消息注册后 half Stream / txstate / txcheck ZSet 均有记录")
    void testHalfMessageRegistration() {
        TransactionScanner scanner = newScanner();
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-half-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "half-body");

            StreamMessageId halfId =
                    scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);
            assertThat(halfId).isNotNull();

            // half Stream 应有 1 条
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isEqualTo(1);

            // txstate Hash 应有 3 个字段：txId, txId.target, txId.halfId
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_PREPARE);
            assertThat(stateMap.get(txId + ".target")).isEqualTo(targetTopic);
            assertThat(stateMap.get(txId + ".halfId")).isEqualTo(halfId.toString());

            // txcheck ZSet 应有 1 条
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.transactionCheckZSet(namespace, TX_GROUP));
            assertThat(zset.size()).isEqualTo(1);
            assertThat(zset.contains(txId)).isTrue();
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("显式 COMMIT 后半消息转投到目标 Stream 且 ZSet 清理")
    void testMarkCommitTransfersMessageToTargetStream() {
        TransactionScanner scanner = newScanner();
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-commit-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "commit-body");

            scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);

            // 显式 COMMIT
            scanner.markCommit(txId, TX_GROUP);

            // 目标 Stream 应有 1 条消息
            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isEqualTo(1);
            Map<StreamMessageId, Map<String, String>> entries =
                    targetStream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            Map<String, String> transferredFields = entries.values().iterator().next();
            assertThat(transferredFields.get("body")).isNotEmpty();

            // half Stream 应被清理（XDEL）
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();

            // txstate 主状态应为 COMMIT
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_COMMIT);

            // txcheck ZSet 应为空
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.transactionCheckZSet(namespace, TX_GROUP));
            assertThat(zset.size()).isZero();
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("显式 ROLLBACK 后半消息被删除且不在目标 Stream")
    void testMarkRollbackDiscardsMessage() {
        TransactionScanner scanner = newScanner();
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-rollback-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "rollback-body");

            scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);

            // 显式 ROLLBACK
            scanner.markRollback(txId, TX_GROUP);

            // 目标 Stream 应为空
            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isZero();

            // half Stream 应被清理（XDEL）
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();

            // txstate 主状态应为 ROLLBACK
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_ROLLBACK);

            // txcheck ZSet 应为空
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.transactionCheckZSet(namespace, TX_GROUP));
            assertThat(zset.size()).isZero();
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("回查器返回 COMMIT 后半消息转投到目标 Stream")
    void testCheckerCommitTransfersMessage() {
        // 使用能立即触发的回查间隔
        TransactionScanner scanner =
                new TransactionScanner(
                        redisson,
                        namespace,
                        converter,
                        CHECK_INTERVAL_MS,
                        MAX_CHECK_TIMES,
                        BATCH_SIZE);
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic =
                    "tx-checker-commit-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "checker-commit-body");

            scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);

            // 注册回查器，返回 COMMIT_MESSAGE
            scanner.registerChecker(
                    TX_GROUP,
                    (TransactionChecker<Object>)
                            (message, ctx) -> {
                                assertThat(ctx.getTransactionId()).isEqualTo(txId);
                                return LocalTransactionState.COMMIT_MESSAGE;
                            });

            scanner.start();

            // 等待目标 Stream 出现消息
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RStream<String, String> stream =
                                        redisson.getStream(
                                                StreamMQKeys.topicStream(namespace, targetTopic));
                                return stream.size() > 0;
                            });

            // 验证目标 Stream 有消息
            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isEqualTo(1);

            // half Stream 应被清理
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();

            // txstate 应为 COMMIT
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_COMMIT);

            // txcheck ZSet 应为空
            RScoredSortedSet<String> zset =
                    redisson.getScoredSortedSet(
                            StreamMQKeys.transactionCheckZSet(namespace, TX_GROUP));
            assertThat(zset.size()).isZero();
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("回查器返回 ROLLBACK 后半消息被丢弃")
    void testCheckerRollbackDiscardsMessage() {
        TransactionScanner scanner =
                new TransactionScanner(
                        redisson,
                        namespace,
                        converter,
                        CHECK_INTERVAL_MS,
                        MAX_CHECK_TIMES,
                        BATCH_SIZE);
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic =
                    "tx-checker-rollback-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "checker-rollback-body");

            scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);

            // 注册回查器，返回 ROLLBACK_MESSAGE
            scanner.registerChecker(
                    TX_GROUP,
                    (TransactionChecker<Object>)
                            (message, ctx) -> LocalTransactionState.ROLLBACK_MESSAGE);

            scanner.start();

            // 等待 txcheck ZSet 被清空（说明回查已执行）
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                RScoredSortedSet<String> zset =
                                        redisson.getScoredSortedSet(
                                                StreamMQKeys.transactionCheckZSet(
                                                        namespace, TX_GROUP));
                                return zset.size() == 0;
                            });

            // 目标 Stream 应为空
            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isZero();

            // half Stream 应被清理
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();

            // txstate 应为 ROLLBACK
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_ROLLBACK);
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("回查器返回 UNKNOWN 后状态更新并重新调度回查")
    void testCheckerUnknownReschedulesAndCounterIncrements() {
        TransactionScanner scanner =
                new TransactionScanner(
                        redisson,
                        namespace,
                        converter,
                        CHECK_INTERVAL_MS,
                        MAX_CHECK_TIMES,
                        BATCH_SIZE);
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic =
                    "tx-checker-unknown-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "checker-unknown-body");

            scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);

            // 计数器记录回查次数，前 MAX_CHECK_TIMES-1 次返回 UNKNOWN
            AtomicReference<Integer> checkCount = new AtomicReference<>(0);
            scanner.registerChecker(
                    TX_GROUP,
                    (TransactionChecker<Object>)
                            (message, ctx) -> {
                                checkCount.updateAndGet(v -> v + 1);
                                return LocalTransactionState.UNKNOW;
                            });

            scanner.start();

            // 等待回查计数器递增（至少 1 次）
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS).until(() -> checkCount.get() >= 1);

            // 此时 txstate 主状态应为 UNKNOWN
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_UNKNOWN);

            // 回查计数 Hash 应 >= 1
            RMap<String, String> counterMap =
                    redisson.getMap(StreamMQKeys.transactionCheckCounter(namespace, TX_GROUP));
            String countStr = counterMap.get(txId);
            assertThat(countStr).isNotNull();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);

            // 等待达到最大回查次数后强制 ROLLBACK
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                String state = stateMap.get(txId);
                                return TransactionScanner.STATE_ROLLBACK.equals(state);
                            });

            assertThat(checkCount.get()).isGreaterThanOrEqualTo(MAX_CHECK_TIMES);
        } finally {
            scanner.stop();
        }
    }

    @Test
    @DisplayName("无 checker 注册时超时半消息被强制 ROLLBACK")
    void testNoCheckerForcesRollback() throws Exception {
        TransactionScanner scanner =
                new TransactionScanner(
                        redisson,
                        namespace,
                        converter,
                        CHECK_INTERVAL_MS,
                        MAX_CHECK_TIMES,
                        BATCH_SIZE);
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-no-checker-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "no-checker-body");

            // 不注册 checker:scanAllGroups 仅扫描已注册 checker 的 txGroup,
            // 因此通过反射直接调用 scanTimeoutHalf 验证 triggerCheck 的 no-checker 分支
            scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);

            // 等待回查时间到达(txcheck ZSet score = 注册时间 + checkIntervalMs)
            Thread.sleep(CHECK_INTERVAL_MS + 100L);

            // 反射调用 scanTimeoutHalf(TX_GROUP) 触发单次扫描
            java.lang.reflect.Method m =
                    TransactionScanner.class.getDeclaredMethod("scanTimeoutHalf", String.class);
            m.setAccessible(true);
            m.invoke(scanner, TX_GROUP);

            // 验证状态为 ROLLBACK
            RMap<String, String> stateMap =
                    redisson.getMap(StreamMQKeys.transactionStateHash(namespace, TX_GROUP));
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_ROLLBACK);

            // 目标 Stream 应为空
            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isZero();

            // half Stream 应被清理
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();
        } finally {
            scanner.stop();
        }
    }

    // ===================== 辅助方法 =====================

    /**
     * 创建默认配置的 TransactionScanner（不启动）。
     *
     * @return Scanner 实例
     */
    private TransactionScanner newScanner() {
        return new TransactionScanner(
                redisson, namespace, converter, CHECK_INTERVAL_MS, MAX_CHECK_TIMES, BATCH_SIZE);
    }

    /**
     * 构造半消息 Stream Entry 字段（模拟 MessageConverter 输出）。
     *
     * @param targetTopic 目标 Topic
     * @param body 消息体
     * @return Stream Entry 字段 Map
     */
    private Map<String, String> buildFields(String targetTopic, String body) {
        Message<String> message =
                MessageBuilder.<String>withTopic(targetTopic).tag("tx-tag").body(body).build();
        return converter.toStreamFields(message);
    }
}
