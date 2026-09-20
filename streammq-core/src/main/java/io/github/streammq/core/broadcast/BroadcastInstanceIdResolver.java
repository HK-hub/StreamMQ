/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 广播消费实例身份解析器：把"配置 / 本地文件 / 注册中心回收 / 注册中心分配 / 随机降级"五级来源 收敛为单一稳定身份。
 *
 * <h2>解析优先级</h2>
 *
 * <ol>
 *   <li><b>显式配置</b>——{@code streammq.consumer.broadcast.instance-id}、系统属性 {@code
 *       streammq.instance.id} 或环境变量 {@code STREAMMQ_INSTANCE_ID}。生产环境推荐：运维完全可控。
 *   <li><b>本地持久文件</b>——默认 {@code ${user.home}/.streammq/instance-id-<ns>_<group>}（<b>按应用分片</b>， 同一
 *       OS 用户下多个 StreamMQ 应用/消费者组互不共享身份；文件内按 {@code id pid timestamp} 多记录存储，
 *       重启时优先复用<b>已退出进程</b>的身份，绝不覆盖仍在运行的其它进程的身份）。对齐 RocketMQ {@code
 *       LocalFileOffsetStore}：身份落在本地盘，重启后<b>无需重新分配</b>即可复用（仅 1 次 claim 校验， 见下）。
 *   <li><b>注册中心回收</b>——本地文件丢失（K8s emptyDir 重建、镜像重置）时，向 Redis 注册中心按 {@code host} 匹配回收同主机的历史槽位，保住 PEL
 *       与消费位点。
 *   <li><b>注册中心分配</b>——全新实例由注册中心单调递增分配，并立即写入本地文件。
 *   <li><b>随机降级</b>——注册中心不可用<b>且本地无既有身份</b>时退化为随机 UUID（等价于 0.1.1 行为，会漂移，仅作最后兜底）。
 * </ol>
 *
 * <h2>为什么必须"本地文件 + 注册中心"双写</h2>
 *
 * <p>只有注册中心：每次启动都要往返（回收或分配），且注册中心不可用时会退化为随机值、身份漂移。 只有本地文件：容器空盘重建即漂移，且文件被复制到另一台机器时无人裁决身份冲突。
 *
 * <p>两者结合后覆盖了从物理机到 K8s 的全部部署形态，代价与收益均如实如下：
 *
 * <ul>
 *   <li><b>健康路径</b>：命中本地文件后仍做 1 次 claim 校验（HGET + Lua），用于挡住"文件被复制到别的
 *       机器"与"同机上另一个活进程"两类身份互踩——这两类场景一旦漏判，两个实例会共用同一消费者名， 广播静默退化为集群消费；
 *   <li><b>注册中心不可达</b>：claim 无法裁决，直接信任本地文件（0 次成功往返），身份在 Redis 停机期间 不漂移、PEL
 *       与消费位点得以保留；代价是这时无法识别跨主机文件复制（日志中明确提示）；
 *   <li><b>注册中心明确拒绝</b>（该身份被别的活实例占用）：绝不复用，转回收 / 重新分配。
 * </ul>
 *
 * <p><b>线程安全：</b>本类无可变状态，可共享单例。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public final class BroadcastInstanceIdResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BroadcastInstanceIdResolver.class);

    /**
     * 本地身份文件路径的系统属性名。
     *
     * <p>设为 {@code "false"} 或 {@code "none"} 可显式禁用本地文件（纯注册中心模式，适用于只读根文件系统）。
     */
    public static final String LOCAL_ID_FILE_PROPERTY = "streammq.instance.id-file";

    /** 本地身份文件默认目录（相对 {@code user.home}） */
    public static final String DEFAULT_LOCAL_ID_DIR = ".streammq";

    /** 本地身份文件默认前缀；完整文件名为 {@code instance-id-<namespace>_<group>}（按应用分片） */
    public static final String DEFAULT_LOCAL_ID_FILE_PREFIX = "instance-id-";

    /** 显式禁用本地文件的哨兵值 */
    private static final String DISABLED = "none";

    private final BroadcastInstanceRegistry registry;
    private final Path explicitLocalIdFile;
    private final boolean localIdFileDisabled;
    private final long leaseTimeoutMillis;
    private final long reclaimGraceMillis;

    /** 本地身份记录的最大保留期（超过后从文件中剪除，避免文件无限增长）。 */
    private static final long LOCAL_RECORD_RETENTION_MS = 7L * 24 * 60 * 60 * 1000;

    /**
     * 构造解析器。
     *
     * @param registry 注册中心，可为 null（表示禁用注册中心，只在配置 / 本地文件 / 随机值之间解析）
     * @param localIdFilePath 显式本地身份文件路径（非 null 时全实例共用该文件，跨应用隔离由调用方保证）； null 表示按「应用维度」默认路径 {@code
     *     ${user.home}/}{@link #DEFAULT_LOCAL_ID_DIR}{@code /}{@link
     *     #DEFAULT_LOCAL_ID_FILE_PREFIX}{@code <namespace>_<group>}
     * @param leaseTimeoutMillis 租约超时（毫秒）
     * @param reclaimGraceMillis 回收宽限期（毫秒）
     */
    public BroadcastInstanceIdResolver(
            BroadcastInstanceRegistry registry,
            Path localIdFilePath,
            long leaseTimeoutMillis,
            long reclaimGraceMillis) {
        this.registry = registry;
        IdFileSetting idFileSetting = resolveExplicitLocalIdFile(localIdFilePath);
        this.explicitLocalIdFile = idFileSetting.path();
        this.localIdFileDisabled = idFileSetting.disabled();
        this.leaseTimeoutMillis = requireValidLeaseTimeouts(leaseTimeoutMillis, reclaimGraceMillis);
        this.reclaimGraceMillis = reclaimGraceMillis;
    }

    /**
     * 校验租约参数（R4-A17）：租约超时必须为正，回收宽限期不得小于租约超时。
     *
     * <p>此前零校验——把 {@code leaseTimeout} 配成 0/负数会让<b>任意</b>槽位立刻满足"已过期"，
     * 同主机另一进程可立即回收该身份：两个活跃进程共用同一广播组，广播静默退化为集群消费 （正是同主机守卫想要防止的场景）。这里改为启动期快速失败。
     *
     * @return 校验通过的租约超时
     * @throws IllegalArgumentException 参数非法
     */
    private static long requireValidLeaseTimeouts(
            long leaseTimeoutMillis, long reclaimGraceMillis) {
        if (leaseTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "broadcast lease timeout must be > 0, got: " + leaseTimeoutMillis);
        }
        if (reclaimGraceMillis < leaseTimeoutMillis) {
            throw new IllegalArgumentException(
                    "broadcast reclaim grace must be >= lease timeout ("
                            + leaseTimeoutMillis
                            + "), got: "
                            + reclaimGraceMillis);
        }
        return leaseTimeoutMillis;
    }

    // ===================== 公开 API =====================

    /**
     * 解析（或分配）本实例的持久化广播身份。
     *
     * <p>本方法<b>永不抛异常</b>：任何一级失败都向下一级降级，保证消费容器总能启动。 因此参数刻意不做 {@code @NonNull} 强约束——{@code null}
     * 命名空间按空串处理，{@code null} topic/group 由内部按空值链路（净化 / 注册中心）兜底， 注册中心抛出的任何运行时异常都被捕获并降级。
     *
     * @param namespace 命名空间，可为 null（按空串处理）
     * @param topic 主题，可为 null（仅用于注册中心登记与日志）
     * @param group 消费者组，可为 null（仅用于注册中心登记与日志）
     * @param configuredId 显式配置的身份，可为 null / 空
     * @return 解析结果，永不为 null
     */
    public Resolution resolve(String namespace, String topic, String group, String configuredId) {
        String ns = Objects.isNull(namespace) ? "" : namespace;
        String host = resolveHost();
        long pid = resolvePid();
        long now = System.currentTimeMillis();

        // ① 显式配置：最高优先级，且不需要注册中心确认语义（用户已声明这是"我"）
        String configured = trimToNull(configuredId);
        if (configured != null) {
            claimQuietly(ns, topic, group, host, pid, configured, now);
            return new Resolution(configured, BroadcastInstanceSource.CONFIGURED);
        }

        // ② 本地持久文件（按 namespace+group 分片，多进程共享同一文件时按 pid 记录复用）
        Path idFile = localIdFileFor(ns, group);
        List<LocalIdRecord> records = readLocalIdRecords(idFile);
        LocalIdRecord reusable = pickReusable(records, pid);
        if (reusable != null) {
            ClaimOutcome claim =
                    claimQuietly(ns, topic, group, host, pid, reusable.instanceId(), now);
            if (claim != ClaimOutcome.REFUSED) {
                if (claim == ClaimOutcome.UNREACHABLE) {
                    // 注册中心不可达 ≠ 身份失效：本地文件是本机自己的持久记忆，此时信任它，
                    // 才能兑现"Redis 停机期间身份不漂移、PEL 与消费位点得以保留"的承诺。
                    // 残余风险仅限"文件被复制到另一台机器且此时注册中心恰好不可达"，已在日志中提示。
                    LOG.warn(
                            "Broadcast instance registry unreachable at startup; trusting local"
                                    + " persistent identity {} without adjudication (topic={},"
                                    + " group={}). If this identity file was copied from another"
                                    + " host, two live instances may share one broadcast identity.",
                            reusable.instanceId(),
                            topic,
                            group);
                }
                upsertLocalIdRecord(idFile, reusable.instanceId(), pid, now);
                return new Resolution(reusable.instanceId(), BroadcastInstanceSource.LOCAL_FILE);
            }
            // 注册中心明确拒绝（槽位被其它主机的活进程占用）→ 不复用该值，继续走回收/分配。
            // 注意：不覆盖本地文件（该身份可能属于同机另一个仍在运行的进程）。
            LOG.warn(
                    "Local broadcast instance id {} rejected by registry (slot held by another"
                            + " live process); allocating a fresh identity",
                    reusable.instanceId());
        }

        // ③ / ④ 注册中心回收或新分配
        if (registry != null) {
            try {
                BroadcastInstanceLease lease =
                        registry.acquire(
                                BroadcastInstanceRequest.of(
                                        ns,
                                        topic,
                                        group,
                                        host,
                                        pid,
                                        null,
                                        now,
                                        leaseTimeoutMillis,
                                        reclaimGraceMillis));
                if (lease != null && lease.instanceId() != null) {
                    String id = lease.instanceId();
                    upsertLocalIdRecord(idFile, id, pid, now);
                    BroadcastInstanceSource source =
                            lease.reclaimed()
                                    ? BroadcastInstanceSource.RECLAIMED
                                    : BroadcastInstanceSource.ALLOCATED;
                    LOG.info(
                            "Broadcast instance identity resolved from registry: id={}, source={},"
                                    + " topic={}, group={}, host={}",
                            id,
                            source,
                            topic,
                            group,
                            host);
                    return new Resolution(id, source);
                }
            } catch (RuntimeException ex) {
                LOG.warn(
                        "Broadcast instance registry unavailable, degrading to fallback identity:"
                                + " {}",
                        ex.toString());
            }
        }

        // ⑤ 兜底：随机值（不稳定，等价于旧行为）
        String fallback = "rnd-" + UUID.randomUUID().toString().substring(0, 8);
        LOG.warn(
                "Broadcast instance identity degraded to non-stable fallback: id={}, topic={},"
                        + " group={}. Broadcast consumer group names WILL change across restarts"
                        + " (PEL is not reused). Configure"
                        + " streammq.consumer.broadcast.instance-id or ensure Redis is reachable at"
                        + " startup to obtain a persistent identity.",
                fallback,
                topic,
                group);
        return new Resolution(fallback, BroadcastInstanceSource.FALLBACK);
    }

    /**
     * 主动释放（优雅停机）：把槽位标记为已停止，但<b>保留</b>消费者组以便重启后回收。
     *
     * <p>与 {@link #resolve} 同口径：不做参数强约束，{@code null} 参数按"无可释放"处理（静默跳过）， 注册中心失败只记日志、不影响停机流程。
     *
     * @param namespace 命名空间，可为 null（按空串处理）
     * @param group 消费者组，可为 null
     * @param instanceId 实例身份，可为 null / 空（无可释放）
     */
    public void release(String namespace, String group, String instanceId) {
        if (registry == null || trimToNull(instanceId) == null) {
            return;
        }
        try {
            registry.release(namespace, group, instanceId);
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance release failed: {}", ex.toString());
        }
    }

    /**
     * 按 topic 维度主动释放（注销单个/部分主题）：从槽位主题集合中移除给定主题； 若集合清空则删除槽位，使这些主题的消费者组可被清扫任务回收。
     *
     * @param namespace 命名空间，可为 null（按空串处理）
     * @param group 消费者组，可为 null
     * @param instanceId 实例身份，可为 null / 空（无可释放）
     * @param topics 本次释放的主题；可为 null（视为空集合，无可释放）
     */
    public void release(
            String namespace, String group, String instanceId, Collection<String> topics) {
        if (registry == null
                || trimToNull(instanceId) == null
                || Objects.isNull(topics)
                || topics.isEmpty()) {
            return;
        }
        try {
            registry.release(namespace, group, instanceId, topics);
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance topic release failed: {}", ex.toString());
        }
    }

    /**
     * 返回底层注册中心（可能为 null）。
     *
     * @return 注册中心
     */
    public BroadcastInstanceRegistry registry() {
        return registry;
    }

    // ===================== 内部实现 =====================

    /**
     * 向注册中心声明占用指定身份；失败静默（不阻塞启动）。
     *
     * <p>必须区分三种结局（R6-B1）：<b>确认</b>可复用；<b>明确拒绝</b>（槽位被其它主机的活实例占用） 绝不可复用；<b>不可达</b>（异常 /
     * 无响应）则无法裁决——调用方据此决定"信任本地持久身份"而非降级为随机值。
     */
    private ClaimOutcome claimQuietly(
            String namespace,
            String topic,
            String group,
            String host,
            long pid,
            String preferredId,
            long now) {
        if (registry == null) {
            // 无注册中心时，配置值/本地文件值本身即权威，直接接受
            return ClaimOutcome.CONFIRMED;
        }
        try {
            BroadcastInstanceLease lease =
                    registry.acquire(
                            BroadcastInstanceRequest.of(
                                    namespace,
                                    topic,
                                    group,
                                    host,
                                    pid,
                                    preferredId,
                                    now,
                                    leaseTimeoutMillis,
                                    reclaimGraceMillis));
            if (lease == null || lease.instanceId() == null) {
                return ClaimOutcome.UNREACHABLE;
            }
            return preferredId.equals(lease.instanceId())
                    ? ClaimOutcome.CONFIRMED
                    : ClaimOutcome.REFUSED;
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance claim failed for {}: {}", preferredId, ex.toString());
            return ClaimOutcome.UNREACHABLE;
        }
    }

    /**
     * 解析显式本地身份文件设置。
     *
     * <p>优先级：显式入参 &gt; 系统属性 {@code streammq.instance.id-file}。系统属性取值为 {@code none}（或 {@code
     * false}）时禁用本地文件。两者都没有时返回「未显式配置」，由 {@link #localIdFileFor} 按 namespace+group 分片到默认目录。
     */
    private static IdFileSetting resolveExplicitLocalIdFile(Path explicit) {
        if (explicit != null) {
            return new IdFileSetting(explicit, false);
        }
        String fromProperty = System.getProperty(LOCAL_ID_FILE_PROPERTY);
        if (fromProperty == null) {
            return new IdFileSetting(null, false);
        }
        String trimmed = fromProperty.trim();
        if (trimmed.isEmpty()
                || DISABLED.equalsIgnoreCase(trimmed)
                || "false".equalsIgnoreCase(trimmed)) {
            return new IdFileSetting(null, true);
        }
        return new IdFileSetting(Paths.get(trimmed), false);
    }

    /**
     * 本实例使用的本地身份文件：显式路径优先；否则按 {@code namespace+group} 分片到默认目录 （{@code
     * ${user.home}/.streammq/instance-id-<ns>_<group>}）。
     *
     * <p><b>为什么按应用分片（R4-A01）：</b>此前的全局单文件 {@code .streammq/instance-id} 被同一 OS 用户下 的<b>所有</b>
     * StreamMQ 应用共享——应用 B 启动会读到应用 A 的身份、被注册中心拒绝后又覆盖该文件， 导致 A 下次重启读到 B 的身份……形成身份互踩与组名漂移（广播漏投 +
     * 僵尸组堆积）。按 namespace+group 分片后，不同应用/消费者组天然隔离。
     *
     * @return 身份文件路径；禁用本地文件时为 null
     */
    private Path localIdFileFor(String namespace, String group) {
        if (explicitLocalIdFile != null) {
            return explicitLocalIdFile;
        }
        if (localIdFileDisabled) {
            return null;
        }
        String home = System.getProperty("user.home");
        if (home == null || home.isBlank()) {
            return null;
        }
        return Paths.get(home)
                .resolve(DEFAULT_LOCAL_ID_DIR)
                .resolve(DEFAULT_LOCAL_ID_FILE_PREFIX + scopeSegment(namespace, group));
    }

    /** 作用域片段：namespace 与 group 净化后拼接（不同应用/消费者组互不共享身份文件）。 */
    private static String scopeSegment(String namespace, String group) {
        String ns = BroadcastInstanceLease.sanitize(Objects.isNull(namespace) ? "" : namespace);
        String grp = BroadcastInstanceLease.sanitize(Objects.isNull(group) ? "" : group);
        String joined = ns.isEmpty() ? grp : ns + "_" + grp;
        return joined.isEmpty() ? "default" : joined;
    }

    /** 读取本地身份文件中的全部记录；不存在 / 不可读时返回空列表。 */
    private static List<LocalIdRecord> readLocalIdRecords(Path file) {
        if (file == null || !Files.isRegularFile(file)) {
            return List.of();
        }
        try {
            List<LocalIdRecord> records = new ArrayList<>();
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                LocalIdRecord record = parseLocalIdRecord(line);
                if (record != null) {
                    records.add(record);
                }
            }
            return records;
        } catch (IOException | RuntimeException ex) {
            LOG.debug("Failed to read local broadcast instance id file {}: {}", file, ex);
            return List.of();
        }
    }

    /** 解析一行记录（{@code id pid timestamp}）；兼容旧版"仅 id"单值文件（pid=0 表示归属未知）。 */
    private static LocalIdRecord parseLocalIdRecord(String line) {
        if (line == null) {
            return null;
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
            return null;
        }
        String[] parts = trimmed.split("\\s+");
        String id = trimToNull(parts[0]);
        if (id == null) {
            return null;
        }
        long pid = 0L;
        if (parts.length >= 2) {
            try {
                pid = Long.parseLong(parts[1]);
            } catch (NumberFormatException ignored) {
                pid = 0L;
            }
        }
        long timestamp = 0L;
        if (parts.length >= 3) {
            try {
                timestamp = Long.parseLong(parts[2]);
            } catch (NumberFormatException ignored) {
                timestamp = 0L;
            }
        }
        return new LocalIdRecord(id, pid, timestamp);
    }

    /**
     * 选择可复用的身份记录：优先本进程记录，其次是<b>已退出进程</b>的记录（同机重启复用同一身份，保住 PEL）， 最后是归属未知的旧版单值记录。
     *
     * <p>若记录归属的 pid 仍存活且不是本进程，说明该身份正被同机另一个进程使用——不复用（否则两个进程 会争抢同一身份，注册中心的同主机守卫会拒绝其一，最终双双漂移）。
     */
    private static LocalIdRecord pickReusable(List<LocalIdRecord> records, long myPid) {
        LocalIdRecord unknownOwner = null;
        for (LocalIdRecord record : records) {
            if (myPid > 0 && record.pid() == myPid) {
                return record;
            }
            if (record.pid() <= 0) {
                if (unknownOwner == null) {
                    unknownOwner = record;
                }
                continue;
            }
            if (!isProcessAlive(record.pid())) {
                return record;
            }
        }
        return unknownOwner;
    }

    /** 进程是否存活；无法判断时按「不存活」处理（保守：宁可分配新身份也不与活进程争抢）。 */
    private static boolean isProcessAlive(long pid) {
        if (pid <= 0) {
            return false;
        }
        try {
            return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
        } catch (RuntimeException ex) {
            return false;
        }
    }

    /**
     * 写入/更新本进程的身份记录（原子替换）：替换同 pid / 同 id 的旧记录，剪除超过保留期的记录。
     *
     * <p>不删除其它存活进程的记录——它们是那些进程跨重启复用身份的凭据。
     */
    private static synchronized void upsertLocalIdRecord(Path file, String id, long pid, long now) {
        if (file == null) {
            return;
        }
        // 同机多进程 + 同机多消费者可能写同一个身份文件（显式配置 single-file 时），
        // read-modify-write 必须串行化：JVM 内用 static synchronized，跨进程用文件锁。
        Path lockFile = file.resolveSibling(file.getFileName() + ".lock");
        try (FileChannel channel =
                        FileChannel.open(
                                lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            List<LocalIdRecord> records = new ArrayList<>(readLocalIdRecords(file));
            records.removeIf(r -> (pid > 0 && r.pid() == pid) || r.instanceId().equals(id));
            records.removeIf(r -> now - r.timestamp() > LOCAL_RECORD_RETENTION_MS);
            records.add(new LocalIdRecord(id, pid, now));
            writeLocalIdRecords(file, records);
        } catch (IOException | RuntimeException ex) {
            LOG.debug("Failed to persist local broadcast instance id to {}: {}", file, ex);
        }
    }

    /** 原子写入全部记录（写临时文件后 rename）；失败向上抛出由调用方记录。 */
    private static void writeLocalIdRecords(Path file, List<LocalIdRecord> records)
            throws IOException {
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        StringBuilder content = new StringBuilder();
        for (LocalIdRecord record : records) {
            content.append(record.instanceId())
                    .append(' ')
                    .append(record.pid())
                    .append(' ')
                    .append(record.timestamp())
                    .append('\n');
        }
        // 临时文件名必须进程唯一：固定 "<name>.tmp" 时同机多进程并发写会互相覆盖临时文件，
        // 一方 rename 后另一方 rename 失败/内容错乱 —— 身份记录被踩会导致身份漂移、组名变更、
        // 位点（PEL）丢失，与"多记录防覆盖"的设计直接相悖。
        Path tmp =
                file.resolveSibling(
                        file.getFileName() + "." + resolvePid() + "-" + UUID.randomUUID() + ".tmp");
        try {
            Files.writeString(tmp, content.toString(), StandardCharsets.UTF_8);
            try {
                Files.move(
                        tmp,
                        file,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException notAtomic) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // 清理失败无影响：rename 成功后临时文件已不存在
            }
        }
    }

    /** 解析宿主标识：主机名，回退到 IP，再回退到固定串。 */
    static String resolveHost() {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            if (trimToNull(hostName) != null) {
                return BroadcastInstanceLease.sanitize(hostName.trim());
            }
        } catch (Exception ignored) {
            // 网络不可用时继续回退
        }
        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            if (trimToNull(ip) != null) {
                return BroadcastInstanceLease.sanitize(ip.trim());
            }
        } catch (Exception ignored) {
            // 继续回退
        }
        return "unknown-host";
    }

    /** 解析进程标识；不可获取时返回 -1。 */
    static long resolvePid() {
        try {
            return ProcessHandle.current().pid();
        } catch (Exception ignored) {
            return -1L;
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * 本地身份文件设置：显式路径，或"显式禁用"标记。
     *
     * @param path 显式文件路径（null 表示未显式配置）
     * @param disabled 是否禁用本地文件
     */
    private record IdFileSetting(Path path, boolean disabled) {}

    /**
     * 本地身份文件中的一条记录。
     *
     * @param instanceId 实例身份
     * @param pid 持有该身份的进程号（0 表示归属未知，兼容旧版单值文件）
     * @param timestamp 记录写入时间（毫秒）
     */
    private record LocalIdRecord(String instanceId, long pid, long timestamp) {}

    /**
     * 本地身份向注册中心声明的结局（R6-B1）。
     *
     * <p>"拒绝"与"不可达"必须分开：前者是注册中心给出的<b>裁决</b>（该身份确实被别的活实例占用）， 后者只是<b>没有裁决</b>（对方不响应）。把不可达当成拒绝会让 Redis
     * 停机时的身份退化为随机值， 与"本地文件保证停机期间身份不漂移"的设计承诺相悖。
     */
    private enum ClaimOutcome {
        /** 注册中心确认占用（或未配置注册中心，本地值即权威）。 */
        CONFIRMED,
        /** 注册中心明确拒绝：槽位被其它主机的活实例占用，绝不可复用。 */
        REFUSED,
        /** 注册中心不可达 / 无响应：无法裁决，调用方按"信任本地持久身份"处理。 */
        UNREACHABLE
    }

    /**
     * 解析结果。
     *
     * @param instanceId 实例身份（非空）
     * @param source 来源
     */
    public record Resolution(String instanceId, BroadcastInstanceSource source) {

        /** 紧凑构造：校验非空。 */
        public Resolution {
            Objects.requireNonNull(instanceId, "instanceId");
            Objects.requireNonNull(source, "source");
        }

        /**
         * 身份是否跨重启稳定。
         *
         * @return true 表示广播消费者组名不会因重启而改变
         */
        public boolean isStable() {
            return source.isStable();
        }
    }
}
