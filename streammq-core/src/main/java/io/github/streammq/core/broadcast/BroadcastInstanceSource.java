/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

/**
 * 广播消费实例身份的<b>来源</b>。
 *
 * <p>来源决定了实例身份的<b>稳定性</b>——只有稳定来源才能保证广播消费者组名跨重启不变， 从而保留 Redis PEL（Pending Entries
 * List）、既不重放历史也不丢失在途消息。
 *
 * <ol>
 *   <li>{@link #CONFIGURED} —— 显式配置（最稳定，推荐生产使用）
 *   <li>{@link #LOCAL_FILE} —— 本地持久文件（对齐 RocketMQ {@code LocalFileOffsetStore} 思路）
 *   <li>{@link #RECLAIMED} —— 从注册中心回收同主机遗留槽位（K8s 空盘重启场景的关键路径）
 *   <li>{@link #ALLOCATED} —— 注册中心新分配（首次启动，之后会落盘并被后续重启复用）
 *   <li>{@link #FALLBACK} —— 注册中心不可用时的降级值（<b>不稳定</b>，会导致广播组漂移）
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public enum BroadcastInstanceSource {

    /**
     * 显式配置值（{@code streammq.consumer.broadcast.instance-id} / {@code streammq.instance.id} / {@code
     * STREAMMQ_INSTANCE_ID}）。
     */
    CONFIGURED(true),

    /** 本地持久化身份文件（默认 {@code ${user.home}/.streammq/instance-id}）。 */
    LOCAL_FILE(true),

    /** 从注册中心回收的同主机历史槽位。 */
    RECLAIMED(true),

    /** 注册中心新分配并已落盘的槽位。 */
    ALLOCATED(true),

    /**
     * 注册中心不可用时的随机回退值。
     *
     * <p><b>不稳定</b>：每次进程启动都可能不同，等价于 0.1.1 及以前的行为——重启会产生新的广播消费者组。 出现该来源意味着 Redis 在启动期不可用，应视为告警信号。
     */
    FALLBACK(false);

    private final boolean stable;

    BroadcastInstanceSource(boolean stable) {
        this.stable = stable;
    }

    /**
     * 该来源产生的实例身份是否<b>跨重启稳定</b>。
     *
     * @return true 表示同一逻辑实例重启后仍解析出相同身份（广播消费者组名不变）
     */
    public boolean isStable() {
        return stable;
    }
}
