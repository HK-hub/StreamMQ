/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import java.util.Collections;
import java.util.Objects;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * Redis 服务器时钟读取（跨主机时间对齐的唯一来源）。
 *
 * <p><b>为什么需要它：</b>实例心跳写入与存活判定（{@code instanceTimeoutMs} 默认 20s）、PEL 认领的空闲 判定（{@code
 * pel-claim-min-idle-ms} 默认 35s）都必须使用同一时钟源。若各自使用本机时钟，跨主机 NTP 偏差（数十秒量级）会让快钟实例被误判存活、慢钟实例被误踢出组并触发无谓
 * rebalance，或把仍在处理的慢消费者 判死并复制重投。
 *
 * <p><b>为什么要抽成一处（发布前红队审查 R5）：</b>此前 {@code PelClaimScheduler} 与 {@code
 * RedissonConsumerGroupManager} 各自内联了一份相同的 Lua 脚本，且都把返回值声明为 {@link
 * RScript.ReturnType#MULTI}（期望数组回复），而脚本实际返回的是<b>标量</b>整数。类型不匹配导致 解码抛异常并被各自的 {@code catch
 * (RuntimeException)} 吞掉，于是"读取 Redis 服务器时钟"这条被注释反复
 * 声明的能力<b>从未真正生效</b>，始终静默回退到本机时钟——与旧行为无异，且无任何可观测信号。 修复要点：① 声明 {@link
 * RScript.ReturnType#INTEGER}（与脚本返回值一致）；② 单一实现 + 单测覆盖。
 *
 * <p>读取失败（脚本禁用 / ACL 拒绝 / 连接故障）返回 {@link #UNKNOWN}，由调用方决定回退策略。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public final class RedisServerClock {

    /** 读取失败哨兵值（真实时间戳恒为正值，不会与正常值混淆）。 */
    public static final long UNKNOWN = -1L;

    /**
     * 返回 Redis 服务器当前时间（Unix 毫秒）；读取失败返回 {@link #UNKNOWN}。
     *
     * <p>Lua 以 {@code TIME} 命令为唯一时钟源，{@code READ_ONLY} 模式可在从节点执行。
     *
     * @param redisson Redisson 客户端，不可为 null
     * @return 服务器时间毫秒；失败为 {@link #UNKNOWN}
     */
    public static long nowMillis(RedissonClient redisson) {
        Objects.requireNonNull(redisson, "redisson");
        try {
            Long millis =
                    redisson.getScript(StringCodec.INSTANCE)
                            .eval(
                                    RScript.Mode.READ_ONLY,
                                    "local t = redis.call('TIME');"
                                            + " return tonumber(t[1]) * 1000 +"
                                            + " math.floor(tonumber(t[2]) / 1000);",
                                    RScript.ReturnType.INTEGER,
                                    Collections.emptyList());
            return Objects.isNull(millis) ? UNKNOWN : millis;
        } catch (RuntimeException ex) {
            return UNKNOWN;
        }
    }
}
