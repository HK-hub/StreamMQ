/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.RedisClusterCompatibility;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.exception.StreamMQBrokerException;
import java.util.Arrays;
import java.util.Objects;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务提交执行器：将半消息原子转投到目标 Stream 并置位 COMMIT。
 *
 * <p><b>原子性设计：</b>「HGET 状态校验 + XRANGE 读取半消息 + XADD 目标流 + XDEL 半消息 + HSET 状态 COMMIT」在<b>单个 Lua
 * 脚本</b>中完成。Redis 执行脚本期间会阻塞服务器，不会与任何其它客户端命令交错，因此：
 *
 * <ul>
 *   <li><b>不重复投递：</b>多实例并发提交时，后执行的脚本会因前一脚本已 XDEL 半消息而读到空（返回 {@code HALF_MISSING}），天然去重——无需额外分布式锁；
 *   <li><b>状态单调（R2-1）：</b>脚本首步校验状态必须仍为 {@code COMMITTING} 才允许转投——已被并发路径强制终结为 {@code ROLLBACK}
 *       的事务（半消息可能尚未被删除）绝不再投递，杜绝「状态 ROLLBACK 但消息已投递」的状态机错乱；
 *   <li><b>不丢失：</b>脚本要么完整执行、要么不执行，不存在"已 XADD 但未置 COMMIT"的中间状态；
 *   <li><b>无锁泄漏 / 无锁过期窗口：</b>不再依赖带 TTL 的执行权锁，彻底消除异常路径锁泄漏，以及"锁在原子批 执行期间过期、另一实例重复转投"的竞态（此前实现的已知缺陷）。
 * </ul>
 *
 * <p><b>编解码一致性：</b>脚本仅将 {@link StringCodec} 作用于 Redis key 名与参数；半消息字段在脚本内以<b>原始 字节</b>形式从 {@code
 * XRANGE} 读取并原样写入目标流（Lua 字符串为字节安全），与客户端读写半消息/目标流所用 codec 无关，因此对二进制默认 codec 与字符串 codec 均兼容。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionCommitExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionCommitExecutor.class);

    /**
     * Lua 脚本：状态 CAS 校验 → 读取并转投半消息，全程原子。
     *
     * <p>KEYS[1] = half Stream key, KEYS[2] = 目标 topic Stream key, KEYS[3] = txstate Hash key
     *
     * <p>ARGV[1] = 半消息 Stream Entry ID, ARGV[2] = txId
     *
     * <p>返回：{@code 'PUBLISHED'}（已转投并置位 COMMIT） / {@code 'ALREADY_COMMIT'}（并发实例已投递，幂等重入） / {@code
     * 'HALF_MISSING'}（半消息已被其它实例转投或从未写入） / {@code 'ABORTED:<state>'}（状态已非 COMMITTING，禁止投递）
     */
    static final String LUA_COMMIT_AND_MARK =
            // R2-1：状态 CAS。终态/回退态一律不得转投——
            //  · COMMIT  → 幂等重入（半消息应已被前一脚本 XDEL），不重复投递；
            //  · 其它非 COMMITTING（ROLLBACK 强制终结 / ROLLBACKING / PREPARE / UNKNOWN）
            //    → 本次绝不能 XADD，否则出现「消息已投递但状态为 ROLLBACK」的状态机错乱。
            "local current = redis.call('HGET', KEYS[3], ARGV[2]);"
                    + "if current == 'COMMIT' then return 'ALREADY_COMMIT'; end;"
                    + "if current ~= 'COMMITTING' then return 'ABORTED:' .. tostring(current); end;"
                    + "local entries = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1]);"
                    + "if #entries == 0 then return 'HALF_MISSING'; end;"
                    + "local fields = entries[1][2];"
                    + "redis.call('XADD', KEYS[2], '*', unpack(fields));"
                    + "redis.call('XDEL', KEYS[1], ARGV[1]);"
                    + "redis.call('HSET', KEYS[3], ARGV[2], 'COMMIT');"
                    + "return 'PUBLISHED';";

    private final RedissonClient redisson;
    private final String namespace;

    /**
     * 构造提交执行器。
     *
     * @param redisson Redisson 客户端
     * @param namespace 命名空间
     */
    public TransactionCommitExecutor(RedissonClient redisson, String namespace) {
        this.redisson = redisson;
        this.namespace = namespace;
    }

    /**
     * 原子转投半消息并置位 COMMIT。
     *
     * <p>该方法不抛业务异常；脚本执行失败（网络异常等）时抛出运行时异常，由调用方决定降级路径。
     *
     * @param txGroup 事务组名
     * @param halfIdStr 半消息 Stream Entry ID（形如 {@code 1234567890-0}）
     * @param targetTopic 目标 Topic
     * @param txId 事务 ID
     * @return {@link Outcome#PUBLISHED} / {@link Outcome#ALREADY_COMMIT} / {@link
     *     Outcome#HALF_MISSING} / {@link Outcome#ABORTED_TERMINAL}
     */
    public Outcome publishHalfAndMarkCommit(
            String txGroup, String halfIdStr, String targetTopic, String txId) {
        // 半消息流 + 目标流 + 状态 Hash 三 key 脚本：Cluster 上必然 CROSSSLOT，
        // 前置拒绝以给出可操作的错误，而不是把服务端裸错误抛给调用方
        RedisClusterCompatibility.requireCrossKeyAtomicity(
                redisson, "Transaction commit (half stream + target stream + state hash)");
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        String result =
                script.eval(
                        RScript.Mode.READ_WRITE,
                        LUA_COMMIT_AND_MARK,
                        RScript.ReturnType.STATUS,
                        Arrays.asList(halfStreamKey, targetStreamKey, stateHashKey),
                        halfIdStr,
                        txId);
        if ("HALF_MISSING".equals(result)) {
            return Outcome.HALF_MISSING;
        }
        if ("ALREADY_COMMIT".equals(result)) {
            // 并发实例已完成转投并置位 COMMIT：半消息已被其 XDEL，本次为幂等重入
            LOG.debug(
                    "Transaction already committed by a concurrent instance: txId={}, txGroup={}",
                    txId,
                    txGroup);
            return Outcome.ALREADY_COMMIT;
        }
        if (Objects.nonNull(result) && result.startsWith("ABORTED")) {
            // R2-1：状态已非 COMMITTING（并发路径强制终结为 ROLLBACK / 回滚中 / 回退态）。
            // 脚本未 XADD、未 XDEL、未改写状态——调用方按「未投递」处理，绝不覆盖已写入的终态。
            LOG.warn(
                    "Transaction commit aborted, state is no longer COMMITTING ({}), half message"
                            + " NOT published: txId={}, txGroup={}",
                    result,
                    txId,
                    txGroup);
            return Outcome.ABORTED_TERMINAL;
        }
        if (!"PUBLISHED".equals(result)) {
            // 脚本契约只返回 PUBLISHED / ALREADY_COMMIT / HALF_MISSING / ABORTED:*；其余结果（含 null）
            // 说明协议异常，必须显式失败而不是乐观地当作已发布，否则事务会停在 COMMITTING 且消息未投递。
            throw new StreamMQBrokerException(
                    "Unexpected transaction commit result: "
                            + result
                            + ", txId="
                            + txId
                            + ", txGroup="
                            + txGroup,
                    null,
                    null);
        }
        return Outcome.PUBLISHED;
    }

    /** 提交结果。 */
    public enum Outcome {
        /** 半消息已转投且状态已置 COMMIT */
        PUBLISHED,
        /** 半消息不存在：已被其它实例的转投脚本 XDEL（已发布），或注册期写入失败遗留的孤儿元数据 */
        HALF_MISSING,
        /** 并发实例已置 COMMIT（本实例的转投为幂等重入，未重复投递） */
        ALREADY_COMMIT,
        /** 状态已非 COMMITTING（被强制终结 / 回滚 / 回退）：未投递，状态未被本次调用改写 */
        ABORTED_TERMINAL
    }
}
