/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.support.RedisServerClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Redis 服务器时钟读取回归测试（发布前红队审查 R5）。
 *
 * <p>缺陷背景：{@code PelClaimScheduler} 与 {@code RedissonConsumerGroupManager} 内联的 Lua 脚本返回<b>标量</b>
 * 整数，却声明为 {@code ReturnType.MULTI}（期望数组回复）。类型不匹配导致解码抛异常、被 {@code catch (RuntimeException)} 吞掉——"使用
 * Redis 服务器时钟做跨主机对齐"这一被注释反复声明的能力从未生效，始终静默回退 到本机时钟（心跳判活 / PEL 空闲判定在跨主机时钟偏差下会误判）。
 *
 * <p>本用例是"失败即红"的守卫：任何把返回类型改回不匹配值、或脚本语义被改坏的改动都会立刻失败。
 */
@DisplayName("Redis 服务器时钟读取")
class RedisServerClockIT extends AbstractRedisIT {

    @Test
    @DisplayName("能真正取到 Redis 服务器时间（不是 UNKNOWN、不回退本地时钟）")
    void readsServerTimeWithoutFallingBack() {
        long before = System.currentTimeMillis();
        long serverNow = RedisServerClock.nowMillis(redisson);
        long after = System.currentTimeMillis();

        assertThat(serverNow)
                .as("取不到时间说明 ReturnType 与脚本返回值再次不匹配（旧 MULTI 缺陷）")
                .isNotEqualTo(RedisServerClock.UNKNOWN);
        // 服务器与本机同机，允许 ±60s 的宽松带——足以证明"取到的是真实时间戳"而非 0/负数。
        assertThat(serverNow).isBetween(before - 60_000L, after + 60_000L);
    }
}
