/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

/**
 * 广播消费组注册表的 SPI 接口（依赖倒置：调用方只依赖本接口，不依赖具体 Redis 客户端实现）。
 *
 * <p><b>为什么需要这个抽象：</b>广播模式下每个容器实例使用一个独立的 Redis 消费者组，组名随容器实例标识 （跨重启不保证相同）生成。注册表以 {@code
 * topic|effectiveGroup} 为成员、心跳时间为 score，用于：
 *
 * <ul>
 *   <li><b>僵尸组回收</b>——识别已死实例（崩溃或长时间停止）遗留的消费者组并销毁，释放其占用的 PEL 与元数据；
 *   <li><b>容量可观测</b>——注册表条目数约等于心跳超时窗口内「实例数 × 重启次数」的累积量，持续增长 意味着实例崩溃循环或心跳超时配置过长。
 * </ul>
 *
 * <p><b>可替换性：</b>默认实现由 Redisson 适配层提供（{@code RedissonBroadcastGroupRegistry}）。用户可通过 注册自定义 {@code
 * BroadcastGroupRegistry} Bean 覆盖——例如接入外部监控系统、改用不同的存储布局， 或在不希望自动销毁消费者组的环境中提供空实现（此时应自行承担 PEL 泄漏风险）。
 *
 * <p><b>线程安全：</b>实现必须线程安全——回收任务与运维端点查询会并发调用。
 *
 * <p><b>失败语义：</b>方法在实现内部应吞掉存储异常并返回安全值（回收返回 0、计数返回 -1）， 避免单次 Redis 抖动阻塞 PEL 认领调度或运维总览。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
public interface BroadcastGroupRegistry {

    /**
     * 回收僵尸广播消费者组：心跳超过实现定义的过期阈值的广播组，其所属实例已确认死亡， 销毁其消费者组以释放 PEL 与元数据。
     *
     * <p>由 PEL 认领调度器低频搭车调用。实现必须保证：
     *
     * <ul>
     *   <li>活实例每次拉取都刷新心跳，绝不会被误回收（除非暂停超过过期阈值）；
     *   <li>整个方法幂等——重复调用不得产生副作用。
     * </ul>
     *
     * @return 本次回收的组数量；异常时返回 0
     */
    int sweepStaleBroadcastGroups();

    /**
     * 返回当前注册表中的广播消费组数量（含活跃组与尚未被回收的僵尸组）。
     *
     * <p>该数字持续增长意味着实例处于崩溃循环，或过期阈值配置过长——两者都会持续占用 Redis 内存， 每个消费者组都有自己的 PEL。建议与 {@link
     * #sweepStaleBroadcastGroups()} 的结果共同建立监控。
     *
     * @return 广播消费组条目数；查询失败时返回 -1
     */
    long countBroadcastGroups();
}
