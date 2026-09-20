/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
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
        final List<String> topicReleases = new ArrayList<>();
        String nextId;
        boolean claimResult = true;

        /** true：带 preferredId 时返回另一个 id，模拟"槽位被其它活实例占用"（注册中心明确拒绝）。 */
        boolean refusePreferred;

        /** true：acquire 抛异常，模拟注册中心不可达。 */
        boolean throwOnAcquire;

        long sweepRemoved = 0;

        @Override
        public BroadcastInstanceLease acquire(BroadcastInstanceRequest request) {
            if (throwOnAcquire) {
                throw new IllegalStateException("registry down");
            }
            if (!claimResult) {
                return null;
            }
            // 忠实复刻真实注册中心语义（RedisBroadcastInstanceRegistry#acquire）：带 preferredId 时
            // 优先占用该身份（槽位空闲即原样返回），无 preferredId 才分配新值。若这里一律返回 nextId，
            // "本地文件复用"用例就无法区分「复用本地身份」与「重新向注册中心申请」两条路径，
            // 断言会失去鉴别力（弱测试）。
            String preferred = request.preferredIdOrNull();
            String id;
            if (preferred != null && !refusePreferred) {
                id = preferred;
            } else {
                id = Objects.isNull(nextId) ? "reg-" + request.host() : nextId;
            }
            return new BroadcastInstanceLease(
                    id,
                    request.host(),
                    List.of(request.topic()),
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
        public void release(
                String namespace, String group, String instanceId, Collection<String> topics) {
            topicReleases.add(instanceId + "->" + String.join(",", topics));
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
        // 本地文件应被写入，供下次启动直接复用身份（多记录格式：id pid timestamp）
        assertThat(file).content(StandardCharsets.UTF_8).startsWith("i-abc123 ");
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
    @DisplayName("本地文件：重启后复用本地身份（只做 claim 校验，不再重新分配）")
    void localFileReusedAcrossResolveCalls(@TempDir Path tmp) {
        FakeRegistry registry = new FakeRegistry();
        registry.nextId = "different-each-time";
        Path file = tmp.resolve("instance-id");
        BroadcastInstanceIdResolver r = resolver(registry, file);

        // 首次：注册中心分配 + 落盘
        BroadcastInstanceIdResolver.Resolution first = r.resolve("", "t", "g", null);
        assertThat(first.source()).isEqualTo(BroadcastInstanceSource.ALLOCATED);
        // 关键：让注册中心下次会返回**不同**的 id。若实现真的走了分配路径（而非读本地文件），
        // 第二次 resolve 就会拿到这个新值、断言随之失败——这才能证明"复用本地身份"路径真实生效。
        registry.nextId = "should-never-be-used";
        // 二次：claim 校验通过（注册中心认可该身份），复用本地文件值
        BroadcastInstanceIdResolver.Resolution second = r.resolve("", "t", "g", null);
        assertThat(second.instanceId()).isEqualTo(first.instanceId());
        assertThat(second.source()).isEqualTo(BroadcastInstanceSource.LOCAL_FILE);
    }

    @Test
    @DisplayName("注册中心不可达：信任本地持久身份（停机期间不漂移），不降级为随机值")
    void registryUnreachableStillReusesLocalIdentity(@TempDir Path tmp) {
        FakeRegistry registry = new FakeRegistry();
        registry.nextId = "i-persisted";
        Path file = tmp.resolve("instance-id");
        BroadcastInstanceIdResolver r = resolver(registry, file);

        String first = r.resolve("", "t", "g", null).instanceId();
        assertThat(first).isEqualTo("i-persisted");

        // Redis 停机（acquire 抛异常）：本地文件是身份的持久记忆，必须继续复用——
        // 若此时降级为 rnd-，消费者组名会漂移、PEL 与消费位点丢失，正是本地文件要防止的场景。
        registry.throwOnAcquire = true;
        BroadcastInstanceIdResolver.Resolution degraded = r.resolve("", "t", "g", null);
        assertThat(degraded.instanceId()).isEqualTo(first);
        assertThat(degraded.source()).isEqualTo(BroadcastInstanceSource.LOCAL_FILE);
        assertThat(degraded.isStable()).isTrue();
    }

    @Test
    @DisplayName("注册中心明确拒绝：本地身份被其它活实例占用时轮换，绝不两实例共用同一身份")
    void registryRefusalRotatesIdentity(@TempDir Path tmp) {
        FakeRegistry registry = new FakeRegistry();
        registry.nextId = "i-mine";
        Path file = tmp.resolve("instance-id");
        BroadcastInstanceIdResolver r = resolver(registry, file);
        assertThat(r.resolve("", "t", "g", null).instanceId()).isEqualTo("i-mine");

        // 该身份已被其它主机的活实例占用（文件被复制到别处）→ 必须轮换，否则两个实例共用
        // 同一消费者名，广播静默退化为集群消费
        registry.refusePreferred = true;
        registry.nextId = "i-fresh";
        BroadcastInstanceIdResolver.Resolution rotated = r.resolve("", "t", "g", null);
        assertThat(rotated.instanceId()).isEqualTo("i-fresh");
        assertThat(rotated.source()).isEqualTo(BroadcastInstanceSource.ALLOCATED);
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
    @DisplayName("null 参数兜底：resolve/release 永不抛异常（javadoc 承诺成立）")
    void nullArgumentsNeverThrow(@TempDir Path tmp) {
        BroadcastInstanceIdResolver noRegistry = resolver(null, tmp.resolve("no-registry-id"));
        FakeRegistry fake = new FakeRegistry();
        BroadcastInstanceIdResolver withRegistry = resolver(fake, tmp.resolve("with-registry-id"));

        // resolve：null 命名空间/topic/group 均按空值链路兜底（配置值优先，其余走降级链）
        assertThat(noRegistry.resolve(null, null, null, "id-1").source())
                .isEqualTo(BroadcastInstanceSource.CONFIGURED);
        assertThatCode(() -> noRegistry.resolve(null, null, null, null)).doesNotThrowAnyException();
        assertThatCode(() -> withRegistry.resolve(null, null, null, null))
                .doesNotThrowAnyException();

        // release：null 实例身份无槽位可释放；null topics 视为空集合（不触发注册中心调用）
        assertThatCode(() -> noRegistry.release(null, null, null)).doesNotThrowAnyException();
        assertThatCode(() -> withRegistry.release(null, null, "id-1", null))
                .doesNotThrowAnyException();
        assertThatCode(() -> withRegistry.release(null, null, "id-1", List.of()))
                .doesNotThrowAnyException();
        assertThatCode(() -> withRegistry.release(null, null, null, List.of("t")))
                .doesNotThrowAnyException();
        assertThat(fake.releases).isEmpty();
        assertThat(fake.topicReleases).isEmpty();
    }

    @Test
    @DisplayName("释放(按 topic)：调用注册中心 topic 维度 release")
    void releaseTopicsDelegatesToRegistry() {
        FakeRegistry registry = new FakeRegistry();
        BroadcastInstanceIdResolver r = resolver(registry, null);
        r.release("", "g", "some-id", List.of("t1", "t2"));
        assertThat(registry.topicReleases).containsExactly("some-id->t1,t2");
    }

    @Test
    @DisplayName("编码可逆：租约 encode/decode 往返一致，分隔符被净化")
    void leaseEncodeDecodeRoundTrip() {
        BroadcastInstanceLease lease =
                new BroadcastInstanceLease(
                        "i-1", "host|with|pipe", List.of("topic"), "group:g", -1, 100, 200, true);
        BroadcastInstanceLease decoded = BroadcastInstanceLease.decode(lease.encode());
        assertThat(decoded).isNotNull();
        assertThat(decoded.instanceId()).isEqualTo("i-1");
        // 分隔符被净化，host 中的 '|' 不应引起字段错位
        assertThat(decoded.host()).isEqualTo("host_with_pipe");
        assertThat(decoded.topics()).containsExactly("topic");
        assertThat(decoded.group()).isEqualTo("group:g");
        assertThat(decoded.reclaimed()).isTrue();
    }

    @Test
    @DisplayName("回收窗口：超过租约未续租可回收，超过宽限期不可回收")
    void reclaimGraceWindow() {
        long now = 1_000_000L;
        BroadcastInstanceLease lease =
                new BroadcastInstanceLease(
                        "i-1", "h", List.of("t"), "g", -1, now - 1000, now - 1000, false);
        // 空闲 1000ms：远小于租约，不可回收
        assertThat(lease.isReclaimable(now, LEASE, GRACE)).isFalse();
        // 空闲 30s：超过租约（20s）但在宽限期内 → 可回收
        assertThat(lease.isReclaimable(now + 30_000, LEASE, GRACE)).isTrue();
        // 空闲 8 天：超过宽限期 → 不可回收（应销毁）
        assertThat(lease.isReclaimable(now + 8L * 24 * 60 * 60 * 1000, LEASE, GRACE)).isFalse();
    }
}
