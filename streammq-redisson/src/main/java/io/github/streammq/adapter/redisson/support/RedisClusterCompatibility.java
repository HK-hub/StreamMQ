/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import io.github.streammq.core.exception.StreamMQException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis Cluster 部署形态诊断 + 「跨 key 原子性」前置守卫（第六轮发布前审查 R6-CLUSTER）。
 *
 * <p><b>为什么需要它：</b>0.1.x 明确不支持 Redis Cluster（数据面刻意依赖跨 key 原子性；见 README「部署形态」）。
 * 此前这条约束<b>只存在于文档</b>，而 Cluster 上的实际运行期表现有两种，且都不可接受：
 *
 * <ul>
 *   <li><b>多 key Lua</b>（事务提交 3 key、跨流 PEL 认领 2 key）：服务端以 {@code CROSSSLOT} 拒绝，零副作用
 *       （fail-safe，但功能不推进）；
 *   <li><b>多 key {@code REDIS_WRITE_ATOMIC} 批</b>（延时入队/转投、重试与 DLQ 的调度/转投、事务元数据）： Redisson 在 Cluster
 *       上<b>按节点拆分提交</b>——两 key 跨节点时双方各自提交成功、整体却不再原子 （可能半写且不报错），同节点时又退化为 {@code CROSSSLOT}。同一份代码因
 *       key 落在哪个节点而结果不同， 属于静默降级，比"直接报错"危险得多。
 * </ul>
 *
 * <p>因此本类提供两件事：
 *
 * <ol>
 *   <li>{@link #warnIfCluster}：生产者/消费者启动时探测一次，命中即输出一条可操作的 WARN（每次 JVM 一次）， 把「文档约定」变成「运行期可观测事实」；
 *   <li>{@link #requireCrossKeyAtomicity}：依赖跨 key 原子性的功能在调用点<b>显式拒绝</b>（Cluster 配置即抛 {@link
 *       StreamMQException}），把静默降级变成确定性的、可操作的失败。
 * </ol>
 *
 * <p><b>实测口径（{@code RedisClusterCompatibilityIT}，真实 3 主 Cluster，16384 slots 全覆盖）：</b>
 *
 * <ul>
 *   <li><b>可用</b>：单 key 路径——发送（{@code XADD}）、消费（{@code XREADGROUP}）与确认（{@code XACK}） 完整往返；
 *   <li><b>确定性拒绝</b>：多 key Lua 被服务端以 {@code CROSSSLOT} 拒绝，实测零副作用（消息既未被误 ACK 也未丢失，仍在 PEL/Stream 中）；
 *   <li><b>静默降级</b>：多 key 原子批在 Cluster 客户端下按节点拆分提交——这正是 {@link #requireCrossKeyAtomicity}
 *       存在的直接原因（实测见 IT 中的批原语用例）；
 *   <li><b>守卫可命中</b>：配置级（{@code useClusterServers()}）与实测级（{@code CLUSTER KEYSLOT} 探针， 覆盖「单机配置误指向
 *       Cluster 节点」）两级探测均已在真实集群上验证。
 * </ul>
 *
 * <p><b>两级探测的分工（重要）：</b>硬拒绝只依据<b>配置级</b>信号——用户显式把 Redisson 配成 Cluster 客户端， 即已进入"多 key
 * 原子性不被提供"的拓扑；探针级信号只用于 WARN，因为 {@code CLUSTER} 命令族在部分兼容实现 或单点代理上也会应答，用它做硬拒绝会误伤可用部署。单机配置误指向 Cluster
 * 节点时客户端并非 cluster-aware， 多 key 请求退化为单节点提交并被服务端 {@code CROSSSLOT} 拒绝（确定性失败，不存在静默拆分），WARN 足以定位。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public final class RedisClusterCompatibility {

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterCompatibility.class);

    /** 每次 JVM 只提示一次：多容器/多生产者重复告警会淹没真正需要的日志。 */
    private static final AtomicBoolean WARNED_ONCE = new AtomicBoolean();

    /** 探针 key（常量内联进脚本，避免为一次探测引入 varargs keys 参数）。 */
    private static final String PROBE_KEY = "streammq:cluster-probe";

    private static final String KEYSLOT_PROBE_SCRIPT =
            "return redis.call('CLUSTER','KEYSLOT','" + PROBE_KEY + "')";

    private RedisClusterCompatibility() {}

    /**
     * 判定当前 Redisson 客户端是否运行在 Redis Cluster 上（配置级 + 实测级）。
     *
     * <p>探测异常一律视为"非 Cluster"（降级为静默）：拓扑诊断绝不能成为启动故障点。
     *
     * @param redisson 客户端（可为 null）
     * @return true 表示已确认运行在 Cluster 上
     */
    public static boolean isClusterMode(RedissonClient redisson) {
        if (redisson == null) {
            return false;
        }
        return configDeclaresCluster(redisson) || probeConfirmsCluster(redisson);
    }

    /**
     * 客户端是否被<b>显式</b>配置为 Cluster 客户端（{@link Config#isClusterConfig()}）。
     *
     * <p>与 {@link #isClusterMode} 的区别：本方法不走 {@code CLUSTER KEYSLOT} 探针，因此不会把「单机配置指向 Cluster
     * 节点」「兼容实现应答 CLUSTER 命令」这类情况算作 Cluster。它表达的是<b>用户意图</b>，用于 {@link #requireCrossKeyAtomicity}
     * 的硬拒绝——只有 cluster-aware 客户端才会把多 key 原子批按节点拆分 （静默降级），这正是守卫要拦截的场景。
     *
     * <p>只读取本地配置对象，不产生任何 Redis 交互，可直接用于热路径。
     *
     * @param redisson 客户端（可为 null）
     * @return true 表示客户端被配置为 Cluster 客户端
     */
    public static boolean isClusterConfigured(RedissonClient redisson) {
        return configDeclaresCluster(redisson);
    }

    /**
     * 命中 Cluster 时输出一次可操作的 WARN。
     *
     * @param redisson 客户端
     * @param component 触发探测的组件名（用于日志定位，如 {@code "producer"}）
     */
    public static void warnIfCluster(RedissonClient redisson, String component) {
        if (WARNED_ONCE.get()) {
            return;
        }
        if (!isClusterMode(redisson)) {
            return;
        }
        if (WARNED_ONCE.compareAndSet(false, true)) {
            LOG.warn(
                    "Redis Cluster topology detected while starting {}. StreamMQ 0.1.x does NOT"
                        + " support Redis Cluster: single-key paths (produce, basic consume/ACK)"
                        + " work, but every cross-key atomic operation is unavailable — multi-key"
                        + " Lua is rejected with CROSSSLOT, and multi-key REDIS_WRITE_ATOMIC"
                        + " batches are split per node by Redisson, silently losing atomicity"
                        + " (partial writes possible). Such operations (delayed messages, retry/DLQ"
                        + " scheduling and transfer, transaction prepare/commit, cross-stream PEL"
                        + " claim) now fail fast with an actionable StreamMQException instead of"
                        + " degrading silently. Deploy on single-instance or"
                        + " master-replica/Sentinel Redis instead; see README 'Deployment' and"
                        + " docs/REPORT.md (Redis Cluster 实测).",
                    component);
        }
    }

    /**
     * 跨 key 原子性前置守卫：客户端被显式配置为 Cluster 时抛出可操作的 {@link StreamMQException}。
     *
     * <p>调用点覆盖全部「同一事务/脚本内写多个 key」的功能：延时消息入队与转投、重试/DLQ 的调度与转投、事务元数据 写入与提交、跨流 PEL 认领。单机/主从/Sentinel
     * 下本方法为 no-op（只读一次本地配置，无 Redis 交互）， 因此可以直接放在热路径上。
     *
     * @param redisson Redisson 客户端（null 视为非 Cluster）
     * @param operation 操作名（英文，出现在异常信息中，用于定位触发点）
     * @throws StreamMQException 客户端为 Cluster 配置时
     */
    public static void requireCrossKeyAtomicity(RedissonClient redisson, String operation) {
        if (!isClusterConfigured(redisson)) {
            return;
        }
        throw new StreamMQException(
                operation
                        + " requires cross-key atomicity (multiple keys written in one"
                        + " transaction/script), which Redis Cluster does not provide: a multi-key"
                        + " request is either rejected with CROSSSLOT or split per node with silent"
                        + " loss of atomicity (partial writes possible, no error raised). StreamMQ"
                        + " 0.1.x does not support Redis Cluster — deploy on a single-instance or"
                        + " master-replica/Sentinel Redis, or configure the client against a single"
                        + " cluster-aware proxy endpoint. See README 'Deployment'.");
    }

    /** 仅供同包测试复位"仅提示一次"状态。 */
    static void resetWarnedStateForTests() {
        WARNED_ONCE.set(false);
    }

    private static boolean configDeclaresCluster(RedissonClient redisson) {
        if (redisson == null) {
            return false;
        }
        try {
            Config config = redisson.getConfig();
            return config != null && config.isClusterConfig();
        } catch (RuntimeException ex) {
            LOG.debug(
                    "Cluster detection via Config failed, falling back to probe: {}",
                    ex.toString());
            return false;
        }
    }

    private static boolean probeConfirmsCluster(RedissonClient redisson) {
        try {
            Object slot =
                    redisson.getScript(StringCodec.INSTANCE)
                            .eval(
                                    RScript.Mode.READ_ONLY,
                                    KEYSLOT_PROBE_SCRIPT,
                                    RScript.ReturnType.INTEGER);
            return slot != null;
        } catch (RuntimeException ex) {
            // 单机 Redis（未开启 cluster 模式）对 CLUSTER KEYSLOT 返回错误，属预期分支
            LOG.debug("Cluster probe negative: {}", ex.toString());
            return false;
        }
    }
}
