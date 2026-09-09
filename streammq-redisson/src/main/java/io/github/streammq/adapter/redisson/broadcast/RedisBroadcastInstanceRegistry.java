/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.broadcast;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.broadcast.BroadcastInstanceLease;
import io.github.streammq.core.broadcast.BroadcastInstanceRegistry;
import io.github.streammq.core.broadcast.BroadcastInstanceRequest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BroadcastInstanceRegistry} 的 Redis 实现：为广播消费者提供<b>跨重启稳定</b>的持久化实例身份。
 *
 * <h2>数据布局（单 Hash，Cluster 安全）</h2>
 *
 * <pre>
 * streammq:{ns}:broadcast-instances:{group}
 *     field = instanceId
 *     value = BroadcastInstanceLease#encode()   // instanceId|host|topic|group|pid|createdAt|lastHb|reclaimed
 * </pre>
 *
 * <p><b>为什么只有一个 key：</b>全部操作（占用 / 回收 / 心跳 / 清扫）都是<b>单 key Lua 脚本</b>。 Redis Cluster 下多 key 脚本会因跨
 * slot 被 {@code CROSSSLOT} 拒绝，而 topic / group / namespace 的命名校验已显式拒绝 {@code { }}， 因此单 key
 * 结构既保证原子性又天然兼容 Cluster。
 *
 * <h2>生命周期与租约</h2>
 *
 * <pre>
 * 分配 ──心跳──▶ active ──停止心跳──▶ reclaimable(同主机可回收，保住 PEL) ──超过宽限期──▶ 销毁(XGROUP DESTROY)
 * </pre>
 *
 * <ul>
 *   <li><b>active</b>：心跳时间在 {@code leaseTimeout} 内，任何其它实例都不得占用；
 *   <li><b>reclaimable</b>：空闲超过 {@code leaseTimeout} 但未超过 {@code reclaimGrace}， 仅允许<b>相同 host</b>
 *       的实例回收——这正是"重启后保住 PEL"的关键窗口；
 *   <li><b>销毁</b>：空闲超过 {@code reclaimGrace}，由 {@link #sweep} 删除槽位并销毁其消费者组。
 * </ul>
 *
 * <p><b>失败语义：</b>所有方法吞掉存储异常并返回安全值（{@code null} / {@code 0} / {@code -1} / 空列表）， 绝不阻塞消费容器启动。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public class RedisBroadcastInstanceRegistry implements BroadcastInstanceRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(RedisBroadcastInstanceRegistry.class);

    /** 新身份分配的重试次数（候选 ID 随机生成，碰撞概率极低） */
    private static final int ALLOCATE_ATTEMPTS = 5;

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * 占用指定身份（CAS）。
     *
     * <p>ARGV: 1=instanceId 2=newEncoded 3=now 4=leaseTimeout 5=host
     *
     * <p>返回 1=占用成功（0x1 新建 / 见调用方按 reclaimed 标志区分），0=槽位被其它活实例占用。
     */
    private static final String LUA_CLAIM =
            "local function field(s, n)                                     \n"
                    + "  local start = 1                                              \n"
                    + "  local idx = 1                                                \n"
                    + "  while true do                                                \n"
                    + "    local p = string.find(s, '|', start, true)                 \n"
                    + "    if not p then                                              \n"
                    + "      if idx == n then return string.sub(s, start) end         \n"
                    + "      return nil                                               \n"
                    + "    end                                                        \n"
                    + "    if idx == n then return string.sub(s, start, p - 1) end    \n"
                    + "    start = p + 1                                              \n"
                    + "    idx = idx + 1                                              \n"
                    + "  end                                                          \n"
                    + "end                                                            \n"
                    + "local cur = redis.call('HGET', KEYS[1], ARGV[1])               \n"
                    + "if not cur then                                                \n"
                    + "  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])                \n"
                    + "  return 1                                                     \n"
                    + "end                                                            \n"
                    + "local owner = field(cur, 2)                                    \n"
                    + "local lastHb = tonumber(field(cur, 7))                         \n"
                    + "if owner == ARGV[5] and lastHb and (tonumber(ARGV[3]) - lastHb)"
                    + " > tonumber(ARGV[4]) then                                      \n"
                    + "  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])                \n"
                    + "  return 1                                                     \n"
                    + "end                                                            \n"
                    + "return 0                                                       \n";

    /** 新分配（仅当 ID 不存在时写入）。ARGV: 1=instanceId 2=newEncoded */
    private static final String LUA_CLAIM_NEW =
            "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end \n"
                    + "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])                \n"
                    + "return 1                                                     \n";

    /** 回收（对旧值做严格 CAS，防止并发抢占）。ARGV: 1=instanceId 2=expectedEncoded 3=newEncoded */
    private static final String LUA_CLAIM_CAS =
            "if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then            \n"
                    + "  redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])               \n"
                    + "  return 1                                                    \n"
                    + "end                                                           \n"
                    + "return 0                                                      \n";

    /** 心跳：仅更新 lastHeartbeat 字段（第 7 个）。ARGV: 1=instanceId 2=now */
    private static final String LUA_HEARTBEAT =
            "local function setfield(s, n, v)                                  \n"
                    + "  local start = 1                                              \n"
                    + "  local idx = 1                                                \n"
                    + "  while true do                                                \n"
                    + "    local p = string.find(s, '|', start, true)                 \n"
                    + "    if not p then                                              \n"
                    + "      if idx == n then return string.sub(s, 1, start - 1) .. v \n"
                    + "      end                                                      \n"
                    + "      return nil                                               \n"
                    + "    end                                                        \n"
                    + "    if idx == n then                                           \n"
                    + "      return string.sub(s, 1, start - 1) .. v .. string.sub(s, p)\n"
                    + "    end                                                        \n"
                    + "    start = p + 1                                              \n"
                    + "    idx = idx + 1                                              \n"
                    + "  end                                                          \n"
                    + "end                                                            \n"
                    + "local cur = redis.call('HGET', KEYS[1], ARGV[1])               \n"
                    + "if not cur then return 0 end                                   \n"
                    + "local updated = setfield(cur, 7, ARGV[2])                      \n"
                    + "if not updated then return 0 end                               \n"
                    + "redis.call('HSET', KEYS[1], ARGV[1], updated)                  \n"
                    + "return 1                                                       \n";

    /**
     * 清扫：删除空闲超过宽限期的槽位，返回被删除的 {@code instanceId|host|topic} 三元组列表（供调用方销毁消费者组）。
     *
     * <p>ARGV: 1=cutoffTs（now - reclaimGrace）2=maxSweep
     */
    private static final String LUA_SWEEP =
            "local function field(s, n)                                     \n"
                    + "  local start = 1                                              \n"
                    + "  local idx = 1                                                \n"
                    + "  while true do                                                \n"
                    + "    local p = string.find(s, '|', start, true)                 \n"
                    + "    if not p then                                              \n"
                    + "      if idx == n then return string.sub(s, start) end         \n"
                    + "      return nil                                               \n"
                    + "    end                                                        \n"
                    + "    if idx == n then return string.sub(s, start, p - 1) end    \n"
                    + "    start = p + 1                                              \n"
                    + "    idx = idx + 1                                              \n"
                    + "  end                                                          \n"
                    + "end                                                            \n"
                    + "local cutoff = tonumber(ARGV[1])                               \n"
                    + "local limit = tonumber(ARGV[2])                                \n"
                    + "local all = redis.call('HGETALL', KEYS[1])                     \n"
                    + "local out = {}                                                 \n"
                    + "local n = 0                                                    \n"
                    + "for i = 1, #all, 2 do                                          \n"
                    + "  if n >= limit then break end                                 \n"
                    + "  local id = all[i]                                            \n"
                    + "  local enc = all[i + 1]                                       \n"
                    + "  local lastHb = tonumber(field(enc, 7))                       \n"
                    + "  if lastHb and lastHb <= cutoff then                          \n"
                    + "    local host = field(enc, 2)                                 \n"
                    + "    local topic = field(enc, 3)                                \n"
                    + "    redis.call('HDEL', KEYS[1], id)                            \n"
                    + "    n = n + 1                                                  \n"
                    + "    out[n] = id .. '|' .. host .. '|' .. topic                 \n"
                    + "  end                                                          \n"
                    + "end                                                            \n"
                    + "return out                                                     \n";

    private final RedissonClient redisson;

    /**
     * 构造注册中心。
     *
     * @param redisson Redisson 客户端（必填）
     */
    public RedisBroadcastInstanceRegistry(RedissonClient redisson) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
    }

    @Override
    public BroadcastInstanceLease acquire(BroadcastInstanceRequest request) {
        Objects.requireNonNull(request, "request");
        String key = StreamMQKeys.broadcastInstances(request.namespace(), request.group());
        try {
            String preferred = request.preferredIdOrNull();
            if (preferred != null) {
                BroadcastInstanceLease claimed = claimPreferred(key, request, preferred);
                if (claimed != null) {
                    return claimed;
                }
                // 偏好槽位被其它活实例占用 → 不允许复用，继续走回收/新分配
                LOG.warn(
                        "Preferred broadcast instance id '{}' is held by another live instance"
                                + " (topic={}, group={}); not reusing it",
                        preferred,
                        request.topic(),
                        request.group());
            }
            BroadcastInstanceLease reclaimed = reclaimSameHost(key, request);
            if (reclaimed != null) {
                return reclaimed;
            }
            return allocateNew(key, request);
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance acquire failed: {}", ex.toString());
            return null;
        }
    }

    @Override
    public boolean heartbeat(String namespace, String group, String instanceId) {
        if (instanceId == null || instanceId.isBlank()) {
            return false;
        }
        try {
            Long ok =
                    redisson.getScript(StringCodec.INSTANCE)
                            .eval(
                                    RScript.Mode.READ_WRITE,
                                    LUA_HEARTBEAT,
                                    RScript.ReturnType.INTEGER,
                                    Collections.singletonList(
                                            StreamMQKeys.broadcastInstances(namespace, group)),
                                    instanceId,
                                    Long.toString(System.currentTimeMillis()));
            return ok != null && ok == 1L;
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance heartbeat failed: {}", ex.toString());
            return false;
        }
    }

    @Override
    public void release(String namespace, String group, String instanceId) {
        // 语义：保留槽位与消费者组，只把心跳刷新到"最后时刻"，使其尽快进入可回收窗口。
        // 绝不 HDEL —— 删除槽位会让重启后的同主机实例无法回收，PEL 与消费位点随之丢失。
        heartbeat(namespace, group, instanceId);
    }

    @Override
    public int sweep(
            String namespace,
            String group,
            long leaseTimeoutMillis,
            long reclaimGraceMillis,
            int maxSweep) {
        long grace = reclaimGraceMillis > 0 ? reclaimGraceMillis : leaseTimeoutMillis * 6;
        int limit = maxSweep > 0 ? maxSweep : 100;
        try {
            List<Object> destroyed =
                    redisson.getScript(StringCodec.INSTANCE)
                            .eval(
                                    RScript.Mode.READ_WRITE,
                                    LUA_SWEEP,
                                    RScript.ReturnType.MULTI,
                                    Collections.singletonList(
                                            StreamMQKeys.broadcastInstances(namespace, group)),
                                    Long.toString(System.currentTimeMillis() - grace),
                                    Integer.toString(limit));
            if (destroyed == null || destroyed.isEmpty()) {
                return 0;
            }
            int removed = 0;
            for (Object raw : destroyed) {
                String[] parts = String.valueOf(raw).split("\\|", -1);
                if (parts.length < 3) {
                    continue;
                }
                String instanceId = parts[0];
                String topic = parts[2];
                try {
                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic))
                            .removeGroup(group + ":" + group + "-" + instanceId);
                } catch (RuntimeException ex) {
                    // NOGROUP 表示组已不存在，视为已清理
                    LOG.debug(
                            "Destroy broadcast consumer group failed (ignored): id={}, {}",
                            instanceId,
                            ex.getMessage());
                }
                removed++;
            }
            if (removed > 0) {
                LOG.info(
                        "Swept {} expired broadcast instance slot(s): namespace={}, group={}",
                        removed,
                        namespace,
                        group);
            }
            return removed;
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance sweep failed: {}", ex.toString());
            return 0;
        }
    }

    @Override
    public long countInstances(String namespace, String group) {
        try {
            return redisson.<String, String>getMap(
                            StreamMQKeys.broadcastInstances(namespace, group), StringCodec.INSTANCE)
                    .size();
        } catch (RuntimeException ex) {
            return -1L;
        }
    }

    @Override
    public List<BroadcastInstanceLease> listInstances(String namespace, String group) {
        try {
            Map<String, String> raw =
                    redisson.<String, String>getMap(
                                    StreamMQKeys.broadcastInstances(namespace, group),
                                    StringCodec.INSTANCE)
                            .readAllMap();
            List<BroadcastInstanceLease> leases = new ArrayList<>(raw.size());
            for (String encoded : raw.values()) {
                BroadcastInstanceLease lease = BroadcastInstanceLease.decode(encoded);
                if (lease != null) {
                    leases.add(lease);
                }
            }
            return leases;
        } catch (RuntimeException ex) {
            return List.of();
        }
    }

    // ===================== 内部实现 =====================

    /** 占用偏好身份：不存在则新建；存在但属同 host 且租约已过期则回收。 */
    private BroadcastInstanceLease claimPreferred(
            String key, BroadcastInstanceRequest request, String preferredId) {
        BroadcastInstanceLease candidate =
                new BroadcastInstanceLease(
                        preferredId,
                        request.host(),
                        request.topic(),
                        request.group(),
                        request.pid(),
                        request.nowMillis(),
                        request.nowMillis(),
                        false);
        Long ok =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_CLAIM,
                                RScript.ReturnType.INTEGER,
                                Collections.singletonList(key),
                                preferredId,
                                candidate.encode(),
                                Long.toString(request.nowMillis()),
                                Long.toString(request.leaseTimeoutMillis()),
                                request.host());
        if (ok == null || ok != 1L) {
            return null;
        }
        // 若槽位此前已存在且属同主机，则本次是"回收"
        return new BroadcastInstanceLease(
                preferredId,
                request.host(),
                request.topic(),
                request.group(),
                request.pid(),
                request.nowMillis(),
                request.nowMillis(),
                false);
    }

    /** 回收同主机、处于可回收窗口内的历史槽位（取最久未心跳者）。 */
    private BroadcastInstanceLease reclaimSameHost(String key, BroadcastInstanceRequest request) {
        Map<String, String> raw;
        try {
            raw = redisson.<String, String>getMap(key, StringCodec.INSTANCE).readAllMap();
        } catch (RuntimeException ex) {
            return null;
        }
        BroadcastInstanceLease best = null;
        String bestEncoded = null;
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            BroadcastInstanceLease lease = BroadcastInstanceLease.decode(entry.getValue());
            if (lease == null) {
                continue;
            }
            if (!request.host().equals(lease.host())) {
                continue;
            }
            if (!lease.isReclaimable(
                    request.nowMillis(),
                    request.leaseTimeoutMillis(),
                    request.reclaimGraceMillis())) {
                continue;
            }
            if (best == null || lease.lastHeartbeatMillis() < best.lastHeartbeatMillis()) {
                best = lease;
                bestEncoded = entry.getValue();
            }
        }
        if (best == null) {
            return null;
        }
        BroadcastInstanceLease renewed =
                new BroadcastInstanceLease(
                        best.instanceId(),
                        request.host(),
                        request.topic(),
                        request.group(),
                        request.pid(),
                        best.createdAtMillis(),
                        request.nowMillis(),
                        true);
        Long ok =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_CLAIM_CAS,
                                RScript.ReturnType.INTEGER,
                                Collections.singletonList(key),
                                renewed.instanceId(),
                                bestEncoded,
                                renewed.encode());
        if (ok == null || ok != 1L) {
            // 并发抢占：本轮放弃，交由调用方重新分配
            return null;
        }
        LOG.info(
                "Reclaimed broadcast instance slot from same host: id={}, idleMs={}, topic={},"
                        + " group={} (PEL preserved)",
                renewed.instanceId(),
                request.nowMillis() - best.lastHeartbeatMillis(),
                request.topic(),
                request.group());
        return renewed;
    }

    /** 分配全新槽位：随机候选 ID + HSETNX 抢占，碰撞则重试。 */
    private BroadcastInstanceLease allocateNew(String key, BroadcastInstanceRequest request) {
        for (int attempt = 0; attempt < ALLOCATE_ATTEMPTS; attempt++) {
            String candidateId = "i-" + randomToken();
            BroadcastInstanceLease lease =
                    new BroadcastInstanceLease(
                            candidateId,
                            request.host(),
                            request.topic(),
                            request.group(),
                            request.pid(),
                            request.nowMillis(),
                            request.nowMillis(),
                            false);
            try {
                Long ok =
                        redisson.getScript(StringCodec.INSTANCE)
                                .eval(
                                        RScript.Mode.READ_WRITE,
                                        LUA_CLAIM_NEW,
                                        RScript.ReturnType.INTEGER,
                                        Collections.singletonList(key),
                                        candidateId,
                                        lease.encode());
                if (ok != null && ok == 1L) {
                    LOG.info(
                            "Allocated new broadcast instance slot: id={}, topic={}, group={},"
                                    + " host={}",
                            candidateId,
                            request.topic(),
                            request.group(),
                            request.host());
                    return lease;
                }
            } catch (RuntimeException ex) {
                LOG.debug("Broadcast instance allocation attempt failed: {}", ex.toString());
                return null;
            }
        }
        LOG.warn(
                "Failed to allocate a broadcast instance slot after {} attempts (topic={},"
                        + " group={})",
                ALLOCATE_ATTEMPTS,
                request.topic(),
                request.group());
        return null;
    }

    private static String randomToken() {
        byte[] buf = new byte[6];
        RANDOM.nextBytes(buf);
        StringBuilder sb = new StringBuilder(buf.length * 2);
        for (byte b : buf) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
