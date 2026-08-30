/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import java.util.Arrays;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * 事务提交执行器：将半消息原子转投到目标 Stream 并置位 COMMIT。
 *
 * <p><b>原子性设计：</b>「XRANGE 读取半消息 + XADD 目标流 + XDEL 半消息 + HSET 状态 COMMIT」在<b>单个 Lua 脚本</b>中完成。Redis
 * 执行脚本期间会阻塞服务器，不会与任何其它客户端命令交错，因此：
 *
 * <ul>
 *   <li><b>不重复投递：</b>多实例并发提交时，后执行的脚本会因前一脚本已 XDEL 半消息而读到空（返回 {@code HALF_MISSING}），天然去重——无需额外分布式锁；
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

    /**
     * Lua 脚本：读取并转投半消息，全程原子。
     *
     * <p>KEYS[1] = half Stream key, KEYS[2] = 目标 topic Stream key, KEYS[3] = txstate Hash key
     *
     * <p>ARGV[1] = 半消息 Stream Entry ID, ARGV[2] = txId
     *
     * <p>返回：{@code 'PUBLISHED'}（已转投并置位 COMMIT） / {@code 'HALF_MISSING'}（半消息已被其它实例转投或从未写入）
     */
    static final String LUA_COMMIT_AND_MARK =
            "local entries = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1]);"
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
     * @return {@link Outcome#PUBLISHED} 或 {@link Outcome#HALF_MISSING}
     */
    public Outcome publishHalfAndMarkCommit(
            String txGroup, String halfIdStr, String targetTopic, String txId) {
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
        return Outcome.PUBLISHED;
    }

    /** 提交结果。 */
    public enum Outcome {
        /** 半消息已转投且状态已置 COMMIT */
        PUBLISHED,
        /** 半消息不存在：已被其它实例的转投脚本 XDEL（已发布），或注册期写入失败遗留的孤儿元数据 */
        HALF_MISSING
    }
}
