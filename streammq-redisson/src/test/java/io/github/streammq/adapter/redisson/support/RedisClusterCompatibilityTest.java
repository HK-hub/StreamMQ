/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.exception.StreamMQException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

/**
 * {@link RedisClusterCompatibility} 拓扑诊断 + 跨 key 原子性守卫回归测试（R6-CLUSTER）。
 *
 * <p>锁定的失败路径：Cluster 部署此前只有文档约定、没有运行期提示——用户把 Redisson 指向 Cluster 后， 只有重试/DLQ/事务路径在运行中抛裸 {@code
 * CROSSSLOT} 才暴露问题。本测试保证两级探测（配置 + 实测） 都可用、探测失败时<b>绝不</b>成为启动故障点，并锁定守卫的判据边界：<b>硬拒绝只看配置级意图</b>
 * （避免探针误伤兼容实现），且守卫为无 Redis 交互的热路径调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("Redis Cluster 拓扑诊断：配置级 + 实测级探测")
class RedisClusterCompatibilityTest {

    @BeforeEach
    void resetWarnState() {
        // "仅提示一次"是进程级状态，测试之间必须复位，否则用例顺序会影响结果
        RedisClusterCompatibility.resetWarnedStateForTests();
    }

    @Test
    @DisplayName("配置级命中：useClusterServers() 即判定为 Cluster")
    void isClusterMode_trueWhenConfigDeclaresCluster() {
        Config clusterConfig = new Config();
        clusterConfig.useClusterServers().addNodeAddress("redis://127.0.0.1:7000");
        RedissonClient client = mock(RedissonClient.class);
        when(client.getConfig()).thenReturn(clusterConfig);

        assertThat(RedisClusterCompatibility.isClusterMode(client)).isTrue();
    }

    @Test
    @DisplayName("实测级命中：单机配置指向 Cluster 节点时由 CLUSTER KEYSLOT 探针兜底")
    void isClusterMode_trueWhenProbeReturnsSlot() {
        RedissonClient client = mock(RedissonClient.class);
        when(client.getConfig()).thenReturn(new Config());
        RScript script = mock(RScript.class);
        when(client.getScript(StringCodec.INSTANCE)).thenReturn(script);
        when(script.<Long>eval(
                        eq(RScript.Mode.READ_ONLY), anyString(), eq(RScript.ReturnType.INTEGER)))
                .thenReturn(1234L);

        assertThat(RedisClusterCompatibility.isClusterMode(client)).isTrue();
    }

    @Test
    @DisplayName("单机（未开启集群）：配置与探针均为否")
    void isClusterMode_falseOnSingleInstance() {
        RedissonClient client = mock(RedissonClient.class);
        when(client.getConfig()).thenReturn(new Config());
        RScript script = mock(RScript.class);
        when(client.getScript(StringCodec.INSTANCE)).thenReturn(script);
        when(script.<Long>eval(
                        eq(RScript.Mode.READ_ONLY), anyString(), eq(RScript.ReturnType.INTEGER)))
                .thenThrow(new IllegalStateException("This instance has cluster support disabled"));

        assertThat(RedisClusterCompatibility.isClusterMode(client)).isFalse();
    }

    @Test
    @DisplayName("探测异常绝不外溢：诊断能力不得成为启动故障点")
    void warnIfCluster_neverThrowsOnBrokenClient() {
        RedissonClient broken = mock(RedissonClient.class);
        when(broken.getConfig()).thenThrow(new IllegalStateException("no config"));
        when(broken.getScript(StringCodec.INSTANCE))
                .thenThrow(new IllegalStateException("no script"));

        assertThatCode(
                        () -> {
                            RedisClusterCompatibility.warnIfCluster(broken, "producer");
                            RedisClusterCompatibility.warnIfCluster(broken, "consumer container");
                        })
                .doesNotThrowAnyException();
        assertThat(RedisClusterCompatibility.isClusterMode(null)).isFalse();
    }

    @Test
    @DisplayName("跨 key 原子性守卫：Cluster 配置下抛可操作异常（含定位信息与替代部署建议）")
    void requireCrossKeyAtomicity_throwsOnClusterConfiguredClient() {
        Config clusterConfig = new Config();
        clusterConfig.useClusterServers().addNodeAddress("redis://127.0.0.1:7000");
        RedissonClient client = mock(RedissonClient.class);
        when(client.getConfig()).thenReturn(clusterConfig);

        assertThatThrownBy(
                        () ->
                                RedisClusterCompatibility.requireCrossKeyAtomicity(
                                        client, "Delayed message enqueue"))
                .as("Cluster 配置下跨 key 原子操作必须显式拒绝，而不是静默降级或抛裸 CROSSSLOT")
                .isInstanceOf(StreamMQException.class)
                .hasMessageContaining("Delayed message enqueue")
                .hasMessageContaining("Redis Cluster")
                .hasMessageContaining("CROSSSLOT")
                .hasMessageContaining("silent loss of atomicity")
                .hasMessageContaining("README");
    }

    @Test
    @DisplayName("跨 key 原子性守卫：单机/主从/Sentinel 下为 no-op，且不做任何 Redis 交互（可热路径）")
    void requireCrossKeyAtomicity_noopOnSingleInstanceAndNeverProbes() {
        RedissonClient client = mock(RedissonClient.class);
        when(client.getConfig()).thenReturn(new Config());

        assertThatCode(
                        () ->
                                RedisClusterCompatibility.requireCrossKeyAtomicity(
                                        client, "Delayed message enqueue"))
                .doesNotThrowAnyException();
        // 热路径属性：只读本地配置对象，绝不触发 CLUSTER KEYSLOT 探针（否则每次发送都要多一次往返）
        verify(client, never()).getScript(StringCodec.INSTANCE);
        assertThatCode(
                        () ->
                                RedisClusterCompatibility.requireCrossKeyAtomicity(
                                        null, "Delayed message enqueue"))
                .as("null 客户端视为非 Cluster")
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("拒绝只看配置级意图：探针级命中（单机配置指向 Cluster 节点）不触发硬拒绝，仅告警")
    void requireCrossKeyAtomicity_ignoresProbeOnlyDetection() {
        RedissonClient client = mock(RedissonClient.class);
        when(client.getConfig()).thenReturn(new Config());
        RScript script = mock(RScript.class);
        when(client.getScript(StringCodec.INSTANCE)).thenReturn(script);
        when(script.<Long>eval(
                        eq(RScript.Mode.READ_ONLY), anyString(), eq(RScript.ReturnType.INTEGER)))
                .thenReturn(1234L);

        // isClusterMode（含探针）为真，但 isClusterConfigured（仅配置）为假
        assertThat(RedisClusterCompatibility.isClusterMode(client)).isTrue();
        assertThat(RedisClusterCompatibility.isClusterConfigured(client)).isFalse();
        assertThatCode(
                        () ->
                                RedisClusterCompatibility.requireCrossKeyAtomicity(
                                        client, "Delayed message enqueue"))
                .as("单机配置误指向 Cluster 节点时不能硬拒绝：CLUSTER 命令族在兼容实现/代理上也会应答")
                .doesNotThrowAnyException();
    }
}
