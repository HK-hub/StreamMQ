/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import java.util.List;
import lombok.NonNull;

/**
 * 广播消费实例注册中心的 SPI：为广播消费者分配<b>跨重启稳定</b>的持久化实例身份。
 *
 * <h2>为什么需要它</h2>
 *
 * <p>Redis 消费者组天生是"组内竞争消费"。要实现广播（每个实例都收到全量消息），StreamMQ 给每个容器实例 一个<b>独立的 Redis
 * 消费者组</b>，组名由实例身份派生。因此<b>实例身份是否稳定，直接决定广播消费是否正确</b>：
 *
 * <ul>
 *   <li>身份稳定 → 组名稳定 → 重启后复用同一消费者组 → PEL 保留、位点连续、不重放不丢失；
 *   <li>身份漂移 → 每次重启新建组 → 旧组成为僵尸组持续占用 Redis 内存，且重启期间的消息不会被补投。
 * </ul>
 *
 * <h2>与 RocketMQ 的对照</h2>
 *
 * <p>RocketMQ 广播消费使用客户端本地 offset 存储（{@code LocalFileOffsetStore}，路径 {@code
 * ~/.rocketmq_offsets/{clientId}}），{@code clientId = ip@pid}。该模型在<b>有状态主机</b>上成立， 但在 K8s / 容器（空盘、漂移
 * IP、PID 复用）环境下身份会失效。
 *
 * <p>本 SPI 采用"Redis 作为注册中心 + 本地文件作为快路径"的混合模型：
 *
 * <ol>
 *   <li><b>本地持久文件</b>——对齐 RocketMQ，零 Redis 往返的快路径；
 *   <li><b>Redis 租约回收</b>——本地文件丢失（空盘重建）时，按 <code>host</code> 匹配回收同主机历史槽位， 覆盖容器化场景；
 *   <li><b>Redis 分配</b>——全新实例由注册中心单调递增分配，并立即落盘。
 * </ol>
 *
 * <h2>失败语义</h2>
 *
 * <p>注册中心<b>永远不得阻塞启动</b>：Redis 不可用时各方法返回"无结果"（{@code null} / {@code 0} / {@code -1}），由 {@link
 * BroadcastInstanceIdResolver} 降级到本地文件或随机值。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public interface BroadcastInstanceRegistry {

    /**
     * 申请（或续租）一个持久化实例身份。
     *
     * <p>实现必须按以下优先级解析：
     *
     * <ol>
     *   <li>{@code preferredId} 非空且该槽位可占用（不存在，或存在但属同一 host 且租约已过期）→ 占用它；
     *   <li>否则查找<b>同 host</b> 且处于回收宽限期内的历史槽位（取最久未心跳者）→ 回收它；
     *   <li>否则分配全新槽位。
     * </ol>
     *
     * @param request 申请参数（必填）
     * @return 租约；注册中心不可用或无法分配时返回 {@code null}（绝不抛异常）
     */
    BroadcastInstanceLease acquire(@NonNull BroadcastInstanceRequest request);

    /**
     * 续租（心跳）。
     *
     * @param namespace 命名空间
     * @param group 消费者组
     * @param instanceId 实例身份
     * @return true 表示续租成功；槽位不存在或存储不可用时返回 false
     */
    boolean heartbeat(@NonNull String namespace, @NonNull String group, @NonNull String instanceId);

    /**
     * 主动释放槽位（优雅停机）。
     *
     * <p><b>语义：释放 ≠ 销毁。</b>释放仅把心跳推进到一个"已停止"标记， 槽位与其消费者组<b>保留</b>，供同主机实例重启后回收，从而保住 PEL。 真正的销毁由
     * {@link #sweep} 在超过回收宽限期后执行。
     *
     * @param namespace 命名空间
     * @param group 消费者组
     * @param instanceId 实例身份
     */
    void release(@NonNull String namespace, @NonNull String group, @NonNull String instanceId);

    /**
     * 清扫：销毁超过回收宽限期仍未回收的实例槽位，并销毁其对应的 Redis 消费者组，释放 PEL 与元数据。
     *
     * <p>必须幂等；重复调用无副作用。
     *
     * @param namespace 命名空间
     * @param group 消费者组
     * @param leaseTimeoutMillis 租约超时（毫秒）
     * @param reclaimGraceMillis 回收宽限期（毫秒）
     * @param maxSweep 单次扫描上限
     * @return 本次销毁的槽位数；存储不可用时返回 0
     */
    int sweep(
            @NonNull String namespace,
            @NonNull String group,
            long leaseTimeoutMillis,
            long reclaimGraceMillis,
            int maxSweep);

    /**
     * 返回某消费者组下已登记的实例槽位数（含活跃与待回收）。
     *
     * @param namespace 命名空间
     * @param group 消费者组
     * @return 槽位数；查询失败时返回 -1
     */
    long countInstances(@NonNull String namespace, @NonNull String group);

    /**
     * 返回某消费者组下全部租约快照（供运维端点展示）。
     *
     * @param namespace 命名空间
     * @param group 消费者组
     * @return 租约列表；查询失败时返回空列表
     */
    List<BroadcastInstanceLease> listInstances(@NonNull String namespace, @NonNull String group);
}
