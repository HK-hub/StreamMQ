/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.broadcast.RedisBroadcastInstanceRegistry;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.broadcast.BroadcastInstanceLease;
import io.github.streammq.core.broadcast.BroadcastInstanceRegistry;
import io.github.streammq.core.broadcast.BroadcastInstanceRequest;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;

/**
 * {@link RedisBroadcastInstanceRegistry} 真实 Redis 集成测试。
 *
 * <p>锁定广播实例身份的核心契约：同主机回收保住 PEL、异主机不可抢占、租赁续租、 过期槽位清扫并释放消费者组。
 */
@DisplayName("广播实例注册中心 Redis 集成测试")
class RedisBroadcastInstanceRegistryIT extends AbstractRedisIT {

    private static final long LEASE = 20_000L;
    private static final long GRACE = 7L * 24 * 60 * 60 * 1000;

    private BroadcastInstanceRegistry registry() {
        return new RedisBroadcastInstanceRegistry(redisson);
    }

    private BroadcastInstanceRequest req(
            String topic, String group, String host, String preferred) {
        return BroadcastInstanceRequest.of(
                namespace,
                topic,
                group,
                host,
                42L,
                preferred,
                System.currentTimeMillis(),
                LEASE,
                GRACE);
    }

    @Test
    @DisplayName("分配：全新实例获得稳定身份并登记（单 Hash key 落地）")
    void allocateStoresLease() {
        BroadcastInstanceRegistry reg = registry();
        BroadcastInstanceLease lease = reg.acquire(req("t1", "g1", "host-A", null));
        assertThat(lease).isNotNull();
        assertThat(lease.instanceId()).startsWith("i-");
        assertThat(lease.host()).isEqualTo("host-A");
        assertThat(lease.reclaimed()).isFalse();

        String key = StreamMQKeys.broadcastInstances(namespace, "g1");
        RMap<String, String> map =
                redisson.<String, String>getMap(
                        key, org.redisson.client.codec.StringCodec.INSTANCE);
        assertThat(map).containsKey(lease.instanceId());

        long count = reg.countInstances(namespace, "g1");
        assertThat(count).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("回收：同主机、租约过期后可复用历史身份（保住 PEL）")
    void reclaimSameHostReusesIdentity() {
        BroadcastInstanceRegistry reg = registry();
        BroadcastInstanceLease first = reg.acquire(req("t2", "g2", "host-A", null));
        // 模拟该实例停止且租约过期：直接改写 lastHeartbeat 到很久以前
        String key = StreamMQKeys.broadcastInstances(namespace, "g2");
        RMap<String, String> map =
                redisson.<String, String>getMap(
                        key, org.redisson.client.codec.StringCodec.INSTANCE);
        BroadcastInstanceLease old = BroadcastInstanceLease.decode(map.get(first.instanceId()));
        BroadcastInstanceLease expired =
                new BroadcastInstanceLease(
                        old.instanceId(),
                        old.host(),
                        old.topics(),
                        old.group(),
                        old.pid(),
                        old.createdAtMillis(),
                        old.createdAtMillis() - (LEASE + 1000),
                        old.reclaimed());
        map.put(first.instanceId(), expired.encode());

        // 同主机回收 → 身份不变（reclaimed=true）
        BroadcastInstanceLease reclaimed = reg.acquire(req("t2", "g2", "host-A", null));
        assertThat(reclaimed.instanceId()).isEqualTo(first.instanceId());
        assertThat(reclaimed.reclaimed()).isTrue();
    }

    @Test
    @DisplayName("隔离：异主机不得抢占同身份槽位，退回新分配")
    void differentHostCannotSteal() {
        BroadcastInstanceRegistry reg = registry();
        BroadcastInstanceLease first = reg.acquire(req("t3", "g3", "host-A", "pinned-id"));
        assertThat(first.instanceId()).isEqualTo("pinned-id");

        // 异主机试图占用同一身份（且原实例仍在租约内）→ 必须失败，退回新分配身份
        BroadcastInstanceLease second = reg.acquire(req("t3", "g3", "host-B", "pinned-id"));
        assertThat(second.instanceId()).isNotEqualTo("pinned-id");
        assertThat(second.instanceId()).startsWith("i-");
    }

    @Test
    @DisplayName("续租：heartbeat 刷新 lastHeartbeat，阻止被回收")
    void heartbeatPreventsReclaim() {
        BroadcastInstanceRegistry reg = registry();
        BroadcastInstanceLease lease = reg.acquire(req("t4", "g4", "host-A", null));
        assertThat(reg.heartbeat(namespace, "g4", lease.instanceId())).isTrue();

        String key = StreamMQKeys.broadcastInstances(namespace, "g4");
        RMap<String, String> map =
                redisson.<String, String>getMap(
                        key, org.redisson.client.codec.StringCodec.INSTANCE);
        BroadcastInstanceLease refreshed =
                BroadcastInstanceLease.decode(map.get(lease.instanceId()));
        assertThat(refreshed).isNotNull();
        assertThat(refreshed.lastHeartbeatMillis())
                .isGreaterThanOrEqualTo(lease.lastHeartbeatMillis());
    }

    @Test
    @DisplayName("清扫：超过回收宽限期的槽位被销毁，并释放其 Redis 消费者组")
    void sweepDestroysExpiredSlotAndGroup() {
        BroadcastInstanceRegistry reg = registry();
        String topic = "t5";
        String group = "g5";
        BroadcastInstanceLease lease = reg.acquire(req(topic, group, "host-A", null));

        // 构造一个已超宽限期的槽位：手动写入 lastHeartbeat 于 8 天前
        String key = StreamMQKeys.broadcastInstances(namespace, group);
        RMap<String, String> map =
                redisson.<String, String>getMap(
                        key, org.redisson.client.codec.StringCodec.INSTANCE);
        BroadcastInstanceLease stale =
                new BroadcastInstanceLease(
                        lease.instanceId(),
                        lease.host(),
                        lease.topics(),
                        lease.group(),
                        lease.pid(),
                        lease.createdAtMillis(),
                        System.currentTimeMillis() - (8L * 24 * 60 * 60 * 1000),
                        lease.reclaimed());
        map.put(lease.instanceId(), stale.encode());

        // 该实例对应的广播消费者组（组名 {group}:{group}-{instanceId}）先于清扫存在
        String effectiveGroup = group + ":" + group + "-" + lease.instanceId();
        redisson.getStream(StreamMQKeys.topicStream(namespace, topic))
                .createGroup(
                        org.redisson.api.stream.StreamCreateGroupArgs.name(effectiveGroup)
                                .makeStream()
                                .id(new org.redisson.api.StreamMessageId(0, 0)));

        int removed = reg.sweep(namespace, group, LEASE, GRACE, 100);
        assertThat(removed).isEqualTo(1);

        // 槽位被删除且消费者组被销毁
        assertThat(map).doesNotContainKey(lease.instanceId());
        List<?> groups =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topic)).listGroups();
        assertThat(groups).noneMatch(g -> effectiveGroup.equals(String.valueOf(g)));
    }

    @Test
    @DisplayName("多 topic 同 group：复用同一身份并合并主题集合，清扫释放全部主题组（修复僵尸组泄漏）")
    void multiTopicSameGroupMergesTopicsAndSweepDestroysAll() {
        BroadcastInstanceRegistry reg = registry();
        String group = "g-multi";
        // 第一次：分配身份，单主题
        BroadcastInstanceLease first = reg.acquire(req("t-a", group, "host-A", null));
        assertThat(first.topics()).containsExactly("t-a");
        // 第二次（同主机、偏好同一身份）：应复用身份并将主题并入集合，而非覆盖
        BroadcastInstanceLease second =
                reg.acquire(req("t-b", group, "host-A", first.instanceId()));
        assertThat(second.instanceId()).isEqualTo(first.instanceId());
        assertThat(second.topics()).containsExactlyInAnyOrder("t-a", "t-b");

        // 构造该合并槽位的过期副本（lastHeartbeat 置于回收宽限期之外）
        String key = StreamMQKeys.broadcastInstances(namespace, group);
        RMap<String, String> map =
                redisson.<String, String>getMap(
                        key, org.redisson.client.codec.StringCodec.INSTANCE);
        BroadcastInstanceLease stale =
                new BroadcastInstanceLease(
                        first.instanceId(),
                        first.host(),
                        second.topics(),
                        group,
                        first.pid(),
                        first.createdAtMillis(),
                        System.currentTimeMillis() - (8L * 24 * 60 * 60 * 1000),
                        true);
        map.put(first.instanceId(), stale.encode());

        // 两个主题对应的消费者组先存在，供清扫销毁
        String effectiveGroup = group + ":" + group + "-" + first.instanceId();
        for (String t : new String[] {"t-a", "t-b"}) {
            redisson.getStream(StreamMQKeys.topicStream(namespace, t))
                    .createGroup(
                            org.redisson.api.stream.StreamCreateGroupArgs.name(effectiveGroup)
                                    .makeStream()
                                    .id(new org.redisson.api.StreamMessageId(0, 0)));
        }

        int removed = reg.sweep(namespace, group, LEASE, GRACE, 100);
        assertThat(removed).isEqualTo(1);
        assertThat(map).doesNotContainKey(first.instanceId());
        // 修复前只会销毁最后一个主题组，遗留僵尸组；现在两个主题的组均被清理
        for (String t : new String[] {"t-a", "t-b"}) {
            List<?> groups =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, t)).listGroups();
            assertThat(groups).noneMatch(g -> effectiveGroup.equals(String.valueOf(g)));
        }
    }
}
