/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionCommitExecutor;
import io.github.streammq.adapter.redisson.scheduler.TransactionRetentionSweeper;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;

/**
 * 第六轮红队修复的真实 Redis 回归用例（失败即红）。
 *
 * <ul>
 *   <li><b>R2-1（事务强制终结竞态）：</b>「A 已提交（消息已投递 + 状态 COMMIT）」与「B 基于过期读取执行强制终结」竞争时， 终态必须与真实投递一致——B 不得把
 *       COMMIT 覆盖成 ROLLBACK；反向：已被强制终结为 ROLLBACK 的事务（半消息因 XDEL 失败仍存在）绝不能再投递；
 *   <li><b>R2-2②（txstate 清理吞吐）：</b>单轮清理量必须突破历史固定上限 128，且游标分页最终覆盖全部过期字段；
 *   <li><b>R2-2③（孤儿半消息公平性）：</b>half Stream 头部被长期存活事务占用时，尾部孤儿条目仍必须被扫到并清理；
 *   <li><b>R2-6（重试流不得有损裁剪）：</b>配置了 {@code streamMaxLen} 时重试消息也不得被 MAXLEN 裁掉（重试流条目是 消息的唯一副本）。
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("第六轮红队修复：事务状态机 / 保留期清理 / 重试流不裁剪（真实 Redis）")
class Round6SchedulerRedTeamIT extends AbstractRedisIT {

    // ===================== R2-1 =====================

    @Test
    @DisplayName("R2-1：已 COMMIT（消息已投递）的事务不得被并发强制终结覆盖为 ROLLBACK")
    void forceFinalize_doesNotOverwriteCommittedState() throws Exception {
        String txGroup = "g-race-commit";
        String topic = "t-race-commit";
        TransactionScanner scanner = new TransactionScanner(redisson, namespace, converter);
        String txId = "tx-committed-1";
        StreamMessageId halfId =
                scanner.registerHalfMessage(
                        txId, txGroup, topic, converter.toStreamFields(delayFreeMessage(topic)));

        // A 提交成功：消息已 XADD 到目标流、半消息已 XDEL、状态置 COMMIT
        scanner.markCommit(txId, txGroup);

        RMap<String, String> stateMap =
                redisson.getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup),
                        StringCodec.INSTANCE);
        RStream<String, String> targetStream =
                redisson.getStream(
                        StreamMQKeys.topicStream(namespace, topic), StringCodec.INSTANCE);
        assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_COMMIT);
        assertThat(targetStream.size()).as("提交后目标流应有且仅有一条消息").isEqualTo(1L);

        // B 基于过期读取（曾读到 COMMITTING 且回查预算耗尽）执行强制终结：
        // 修复前该路径无条件 HSET ROLLBACK，把「已投递」覆盖成「回滚」→ 本用例红。
        Method forceFinalize =
                TransactionScanner.class.getDeclaredMethod(
                        "forceFinalizeStuckCommit",
                        String.class,
                        String.class,
                        RMap.class,
                        int.class);
        forceFinalize.setAccessible(true);
        forceFinalize.invoke(scanner, txId, txGroup, stateMap, 16);

        assertThat(stateMap.get(txId))
                .as("终态必须与真实投递一致：已投递的事务不得被改写成 ROLLBACK")
                .isEqualTo(TransactionScanner.STATE_COMMIT);
        assertThat(stateMap.get(txId + ".failureReason")).as("未发生强制终结，不得写入失败原因").isNull();
        assertThat(targetStream.size()).as("投递结果不受影响（不重复投递）").isEqualTo(1L);
        assertThat(
                        redisson.getStream(
                                        StreamMQKeys.halfStream(namespace, txGroup),
                                        StringCodec.INSTANCE)
                                .range(1, halfId, halfId))
                .as("半消息已被提交脚本消费")
                .isEmpty();
    }

    @Test
    @DisplayName("R2-1：状态已被强制终结为 ROLLBACK 时，提交脚本绝不再投递半消息")
    void commitAborted_whenStateAlreadyRolledBack() {
        String txGroup = "g-race-rollback";
        String topic = "t-race-rollback";
        TransactionScanner scanner = new TransactionScanner(redisson, namespace, converter);
        String txId = "tx-rolledback-1";
        StreamMessageId halfId =
                scanner.registerHalfMessage(
                        txId, txGroup, topic, converter.toStreamFields(delayFreeMessage(topic)));

        // 模拟「B 的强制终结已获胜（ROLLBACK），但 XDEL 半消息失败/尚未执行」的窗口
        RMap<String, String> stateMap =
                redisson.getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup),
                        StringCodec.INSTANCE);
        stateMap.put(txId, TransactionScanner.STATE_ROLLBACK);

        TransactionCommitExecutor executor = new TransactionCommitExecutor(redisson, namespace);
        TransactionCommitExecutor.Outcome outcome =
                executor.publishHalfAndMarkCommit(txGroup, halfId.toString(), topic, txId);

        assertThat(outcome).isEqualTo(TransactionCommitExecutor.Outcome.ABORTED_TERMINAL);
        assertThat(stateMap.get(txId))
                .as("已终态的事务不得被提交脚本改写")
                .isEqualTo(TransactionScanner.STATE_ROLLBACK);
        assertThat(
                        redisson.getStream(
                                        StreamMQKeys.topicStream(namespace, topic),
                                        StringCodec.INSTANCE)
                                .size())
                .as("ROLLBACK 对应未投递：目标流必须为空")
                .isZero();
        assertThat(
                        redisson.getStream(
                                        StreamMQKeys.halfStream(namespace, txGroup),
                                        StringCodec.INSTANCE)
                                .range(1, halfId, halfId))
                .as("脚本拒绝时不得 XDEL 半消息（保持现状，由回滚/保留期清理）")
                .isNotEmpty();
    }

    @Test
    @DisplayName("R2-1 回归：正常提交仍然投递一次且幂等（状态 CAS 不破坏既有路径）")
    void normalCommit_stillPublishesExactlyOnce() {
        String txGroup = "g-happy";
        String topic = "t-happy";
        TransactionScanner scanner = new TransactionScanner(redisson, namespace, converter);
        String txId = "tx-happy-1";
        scanner.registerHalfMessage(
                txId, txGroup, topic, converter.toStreamFields(delayFreeMessage(topic)));

        scanner.markCommit(txId, txGroup);
        // 重复提交（调用方重试 / 并发实例）：必须幂等，不得重复投递
        scanner.markCommit(txId, txGroup);

        RStream<String, String> targetStream =
                redisson.getStream(
                        StreamMQKeys.topicStream(namespace, topic), StringCodec.INSTANCE);
        RMap<String, String> stateMap =
                redisson.getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup),
                        StringCodec.INSTANCE);
        assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_COMMIT);
        assertThat(targetStream.size()).isEqualTo(1L);
        assertThat(stateMap.get(txId + ".done")).as("终态必须带保留期标记").isNotNull();
        assertThat(
                        redisson.getScoredSortedSet(
                                        StreamMQKeys.transactionCheckZSet(namespace, txGroup),
                                        StringCodec.INSTANCE)
                                .size())
                .isZero();
    }

    // ===================== R2-2② =====================

    @Test
    @DisplayName("R2-2②：单轮清理量突破历史上限 128（批量提升），且分页最终覆盖全部过期字段")
    void expiredTerminalSweep_exceedsLegacyBatchLimit() {
        String txGroup = "g-sweep-throughput";
        TransactionRetentionSweeper sweeper = new TransactionRetentionSweeper(redisson, namespace);
        RMap<String, String> stateMap =
                redisson.getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup),
                        StringCodec.INSTANCE);
        long expiredDone = System.currentTimeMillis() - 8L * 24 * 60 * 60 * 1000;
        int total = 300;
        for (int i = 0; i < total; i++) {
            stateMap.put("tx-" + i, TransactionScanner.STATE_COMMIT);
            stateMap.put("tx-" + i + ".done", Long.toString(expiredDone));
        }

        int removed = sweeper.sweepExpiredTerminalStates(txGroup);

        assertThat(removed)
                .as("单轮清理量必须随批量上限提升（历史实现固定 ≤128，高事务量下 Hash 无界增长）")
                .isGreaterThan(128)
                .isEqualTo(total);
        assertThat(stateMap.size()).as("过期字段全部清空").isZero();
    }

    @Test
    @DisplayName("R2-2②：批量上限可配（setSweepBatchSize）并驱动单轮清理量")
    void expiredTerminalSweep_respectsConfiguredBatchSize() {
        String txGroup = "g-sweep-config";
        TransactionRetentionSweeper sweeper = new TransactionRetentionSweeper(redisson, namespace);
        RMap<String, String> stateMap =
                redisson.getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup),
                        StringCodec.INSTANCE);
        long expiredDone = System.currentTimeMillis() - 8L * 24 * 60 * 60 * 1000;
        int total = 600;
        for (int i = 0; i < total; i++) {
            stateMap.put("tx-" + i, TransactionScanner.STATE_ROLLBACK);
            stateMap.put("tx-" + i + ".done", Long.toString(expiredDone));
        }
        sweeper.setSweepBatchSize(200);
        assertThat(sweeper.getSweepBatchSize()).isEqualTo(200);

        int totalRemoved = sweeper.sweepExpiredTerminalStates(txGroup);
        assertThat(totalRemoved).as("配置的批量上限生效（旧实现硬编码 128）").isGreaterThan(128);
        // 游标分页：反复清理必然收敛（不会因头部占用而只清头部）
        for (int round = 0; round < 10 && stateMap.size() > 0; round++) {
            totalRemoved += sweeper.sweepExpiredTerminalStates(txGroup);
        }
        assertThat(stateMap.size()).as("分页最终覆盖全部过期字段").isZero();
        assertThat(totalRemoved).isEqualTo(total);
    }

    // ===================== R2-2③ =====================

    @Test
    @DisplayName("R2-2③：half Stream 头部被长期存活事务占用时，尾部孤儿仍被扫到并清理")
    void orphanHalfSweep_reachesTailBehindLiveHead() {
        String txGroup = "g-orphan-fairness";
        long oldTs = System.currentTimeMillis() - 3L * 24 * 60 * 60 * 1000;
        RStream<String, String> halfStream =
                redisson.getStream(
                        StreamMQKeys.halfStream(namespace, txGroup), StringCodec.INSTANCE);
        RMap<String, String> stateMap =
                redisson.getMap(
                        StreamMQKeys.transactionStateHash(namespace, txGroup),
                        StringCodec.INSTANCE);
        int liveHead = 300;
        for (int i = 0; i < liveHead; i++) {
            String liveTxId = "live-" + i;
            halfStream.add(
                    new StreamMessageId(oldTs, i + 1),
                    StreamAddArgs.entries(Map.of("txId", liveTxId, "body", "live")));
            // 非终态：半消息仍被引用，保留期清理必须跳过（不能误删）
            stateMap.put(liveTxId, TransactionScanner.STATE_PREPARE);
        }
        halfStream.add(
                new StreamMessageId(oldTs, liveHead + 1),
                StreamAddArgs.entries(Map.of("txId", "orphan-1", "body", "orphan")));

        TransactionRetentionSweeper sweeper = new TransactionRetentionSweeper(redisson, namespace);
        sweeper.setSweepBatchSize(500);
        int removed = sweeper.sweepOrphanHalves(txGroup);

        assertThat(removed).as("头部 300 条存活条目占用窗口时，尾部孤儿仍必须被清理（旧实现固定取最早 128 条 → 饿死）").isEqualTo(1);
        assertThat(halfStream.size()).as("存活事务的半消息一条都不能被误删").isEqualTo(liveHead);
    }

    // ===================== R2-6 =====================

    @Test
    @DisplayName("R2-6：配置了 streamMaxLen 也不得裁剪重试流（重试流条目是消息唯一副本）")
    void retryStream_isNeverTrimmedByMaxLen() throws Exception {
        String topic = "t-retry-no-trim";
        String group = "g-retry-no-trim";
        int messageCount = 6;
        // 旧实现：destStreamKey == retry 目标流时对 XADD 施加 maxLen=2 → 6 条只剩 2 条（静默丢 4 条）
        RetryScheduler scheduler = new RetryScheduler(redisson, namespace, 100L, 10, 2);
        scheduler.registerRetryTarget(topic, group, 5);
        String retryZSetKey = StreamMQKeys.retryZSet(namespace, topic, group);
        RScoredSortedSet<String> zset =
                redisson.getScoredSortedSet(retryZSetKey, StringCodec.INSTANCE);
        for (int i = 0; i < messageCount; i++) {
            String msgId = "m-" + i;
            Map<String, String> payload = new LinkedHashMap<>();
            payload.put(DefaultMessageConverter.FIELD_BODY, "payload-" + i);
            payload.put(RetryScheduler.FIELD_TARGET_TOPIC, topic);
            payload.put(RetryScheduler.FIELD_RETRY_COUNT, "0");
            redisson.getMap(
                            StreamMQKeys.retryPayloadHash(namespace, topic, group, msgId),
                            StringCodec.INSTANCE)
                    .putAll(payload);
            zset.add(0.0, msgId);
        }
        RStream<String, String> retryStream =
                redisson.getStream(
                        StreamMQKeys.retryStream(namespace, topic, group), StringCodec.INSTANCE);

        scheduler.start();
        try {
            long deadline = System.currentTimeMillis() + 15_000L;
            while (retryStream.size() < messageCount && System.currentTimeMillis() < deadline) {
                Thread.sleep(50L);
            }
        } finally {
            scheduler.stop();
        }

        assertThat(retryStream.size())
                .as("重试消息一条都不能丢：retry/DLQ 流的 XADD 不得施加有损 MAXLEN 裁剪")
                .isEqualTo(messageCount);
        assertThat(zset.size()).as("调度条目应随转投清理完毕").isZero();
    }

    // ===================== 夹具 =====================

    /** 非延时消息（避免走延时链路，直接使用普通发送语义）。 */
    private static Message<String> delayFreeMessage(String topic) {
        return MessageBuilder.<String>withTopic(topic).body("tx-payload").build();
    }
}
