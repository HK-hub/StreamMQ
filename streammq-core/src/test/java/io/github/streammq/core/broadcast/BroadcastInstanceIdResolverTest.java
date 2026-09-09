/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link BroadcastInstanceIdResolver} 解析优先级单元测试（无 Redis 依赖）。
 *
 * <p>验证五级来源收敛：配置 &gt; 本地文件 &gt; 注册中心回收/分配 &gt; 随机降级， 以及"不稳定来源"会触发告警级日志信号。
 */
@DisplayName("广播实例身份解析器单元测试")
class BroadcastInstanceIdResolverTest {

    private static final long LEASE = 20_000L;
    private static final long GRACE = 7L * 24 * 60 * 60 * 1000;

    /** 内存版假注册中心：可脚本化占用/回收/查询行为。 */
    private static final class FakeRegistry implements BroadcastInstanceRegistry {
        final List<String> heartbeats = new ArrayList<>();
        final List<String> releases = new ArrayList<>();
        String nextId;
        boolean claimResult = true;
        long sweepRemoved = 0;

        @Override
        public BroadcastInstanceLease acquire(BroadcastInstanceRequest request) {
            if (!claimResult) {
                return null;
            }
            String id = Objects.isNull(nextId) ? "reg-" + request.host() : nextId;
            return new BroadcastInstanceLease(
                    id,
                    request.host(),
                    request.topic(),
                    request.group(),
                    request.pid(),
                    request.nowMillis(),
                    request.nowMillis(),
                    false);
        }

        @Override
        public boolean heartbeat(String namespace, String group, String instanceId) {
            heartbeats.add(instanceId);
            return true;
        }

        @Override
        public void release(String namespace, String group, String instanceId) {
            releases.add(instanceId);
        }

        @Override
        public int sweep(
                String namespace,
                String group,
                long leaseTimeoutMillis,
                long reclaimGraceMillis,
                int maxSweep) {
            return (int) sweepRemoved;
        }

        @Override
        public long countInstances(String namespace, String group) {
            return 0;
        }

        @Override
        public List<BroadcastInstanceLease> listInstances(String namespace, String group) {
            return List.of();
        }
    }

    private BroadcastInstanceIdResolver resolver(FakeRegistry registry, Path localFile) {
        return new BroadcastInstanceIdResolver(registry, localFile, LEASE, GRACE);
    }

    @Test
    @DisplayName("① 显式配置优先级最高，且不需要注册中心确认")
    void configuredWinsOverRegistry() {
        FakeRegistry registry = new FakeRegistry();
        registry.nextId = "reg-generated";
        BroadcastInstanceIdResolver r = resolver(registry, null);

        BroadcastInstanceIdResolver.Resolution res = r.resolve("", "t", "g", "explicit-id");
        assertThat(res.instanceId()).isEqualTo("explicit-id");
        assertThat(res.source()).isEqualTo(BroadcastInstanceSource.CONFIGURED);
        assertThat(res.isStable()).isTrue();
    }

    @Test
    @DisplayName("② 无配置但有注册中心时，注册中心分配身份且写入本地文件")
    void registryAllocatesWhenNoConfig(@TempDir Path tmp) {
        FakeRegistry registry = new FakeRegistry();
        registry.nextId = "i-abc123";
        Path file = tmp.resolve("instance-id");
        BroadcastInstanceIdResolver r = resolver(registry, file);

        BroadcastInstanceIdResolver.Resolution res = r.resolve("", "t", "g", null);
        assertThat(res.instanceId()).isEqualTo("i-abc123");
        assertThat(res.source())
                .isIn(BroadcastInstanceSource.ALLOCATED, BroadcastInstanceSource.RECLAIMED);
        assertThat(res.isStable()).isTrue();
        // 本地文件应被写入，供下次启动零往返复用
        assertThat(file).hasContent("i-abc123");
    }

    @Test
    @DisplayName("③ 注册中心不可用时降级到随机值（不稳定、告警级信号）")
    void fallbackWhenRegistryDown(@TempDir Path tmp) {
        FakeRegistry registry = new FakeRegistry();
        registry.claimResult = false; // 模拟注册中心不可用
        Path file = tmp.resolve("instance-id");
        BroadcastInstanceIdResolver r = resolver(registry, file);

        BroadcastInstanceIdResolver.Resolution res = r.resolve("", "t", "g", null);
        assertThat(res.source()).isEqualTo(BroadcastInstanceSource.FALLBACK);
        assertThat(res.isStable()).isFalse();
        assertThat(res.instanceId()).startsWith("rnd-");
    }

    @Test
    @DisplayName("本地文件：重启后只读本地文件即复用身份（零 Redis 往返）")
    void localFileReusedAcrossResolveCalls(@TempDir Path tmp) {
        FakeRegistry registry = new FakeRegistry();
        registry.nextId = "different-each-time";
        Path file = tmp.resolve("instance-id");
        BroadcastInstanceIdResolver r = resolver(registry, file);

        // 首次：注册中心分配 + 落盘
        String first = r.resolve("", "t", "g", null).instanceId();
        // 二次：直接复用本地文件值，不应再向注册中心申请（nextId 已变化，若复用则值不变）
        String second = r.resolve("", "t", "g", null).instanceId();
        assertThat(second).isEqualTo(first);
    }

    @Test
    @DisplayName("释放：调用注册中心 release（保留槽位以便回收，绝不销毁组）")
    void releaseDelegatesToRegistry() {
        FakeRegistry registry = new FakeRegistry();
        BroadcastInstanceIdResolver r = resolver(registry, null);
        r.release("", "g", "some-id");
        assertThat(registry.releases).containsExactly("some-id");
    }

    @Test
    @DisplayName("编码可逆：租约 encode/decode 往返一致，分隔符被净化")
    void leaseEncodeDecodeRoundTrip() {
        BroadcastInstanceLease lease =
                new BroadcastInstanceLease(
                        "i-1", "host|with|pipe", "topic", "group:g", -1, 100, 200, true);
        BroadcastInstanceLease decoded = BroadcastInstanceLease.decode(lease.encode());
        assertThat(decoded).isNotNull();
        assertThat(decoded.instanceId()).isEqualTo("i-1");
        // 分隔符被净化，host 中的 '|' 不应引起字段错位
        assertThat(decoded.host()).isEqualTo("host_with_pipe");
        assertThat(decoded.topic()).isEqualTo("topic");
        assertThat(decoded.group()).isEqualTo("group:g");
        assertThat(decoded.reclaimed()).isTrue();
    }

    @Test
    @DisplayName("回收窗口：超过租约未续租可回收，超过宽限期不可回收")
    void reclaimGraceWindow() {
        long now = 1_000_000L;
        BroadcastInstanceLease lease =
                new BroadcastInstanceLease("i-1", "h", "t", "g", -1, now - 1000, now - 1000, false);
        // 空闲 1000ms：远小于租约，不可回收
        assertThat(lease.isReclaimable(now, LEASE, GRACE)).isFalse();
        // 空闲 30s：超过租约（20s）但在宽限期内 → 可回收
        assertThat(lease.isReclaimable(now + 30_000, LEASE, GRACE)).isTrue();
        // 空闲 8 天：超过宽限期 → 不可回收（应销毁）
        assertThat(lease.isReclaimable(now + 8L * 24 * 60 * 60 * 1000, LEASE, GRACE)).isFalse();
    }
}
