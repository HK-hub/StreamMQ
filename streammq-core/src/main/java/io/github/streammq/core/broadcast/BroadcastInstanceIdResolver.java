/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.broadcast;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.UUID;
import lombok.NonNull;
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
 *   <li><b>本地持久文件</b>——默认 {@code ${user.home}/.streammq/instance-id}。对齐 RocketMQ {@code
 *       LocalFileOffsetStore}：身份落在本地盘，重启零 Redis 往返即可复用。
 *   <li><b>注册中心回收</b>——本地文件丢失（K8s emptyDir 重建、镜像重置）时，向 Redis 注册中心按 {@code host} 匹配回收同主机的历史槽位，保住 PEL
 *       与消费位点。
 *   <li><b>注册中心分配</b>——全新实例由注册中心单调递增分配，并立即写入本地文件。
 *   <li><b>随机降级</b>——注册中心不可用时退化为随机 UUID（等价于 0.1.1 行为，会漂移，仅作最后兜底）。
 * </ol>
 *
 * <h2>为什么必须"本地文件 + 注册中心"双写</h2>
 *
 * <p>只有注册中心：每次启动至少一次 Redis 往返，且 Redis 不可用时广播消费直接失去位点。 只有本地文件：容器空盘重建即漂移。
 * 两者结合后——<b>快路径零往返、慢路径可恢复</b>，覆盖了从物理机到 K8s 的全部部署形态。
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

    /** 本地身份文件默认路径（相对 {@code user.home}） */
    public static final String DEFAULT_LOCAL_ID_FILE = ".streammq/instance-id";

    /** 显式禁用本地文件的哨兵值 */
    private static final String DISABLED = "none";

    private final BroadcastInstanceRegistry registry;
    private final Path localIdFile;
    private final long leaseTimeoutMillis;
    private final long reclaimGraceMillis;

    /**
     * 构造解析器。
     *
     * @param registry 注册中心，可为 null（表示禁用注册中心，只在配置 / 本地文件 / 随机值之间解析）
     * @param localIdFilePath 本地身份文件路径，可为 null 表示使用默认路径
     * @param leaseTimeoutMillis 租约超时（毫秒）
     * @param reclaimGraceMillis 回收宽限期（毫秒）
     */
    public BroadcastInstanceIdResolver(
            BroadcastInstanceRegistry registry,
            Path localIdFilePath,
            long leaseTimeoutMillis,
            long reclaimGraceMillis) {
        this.registry = registry;
        this.localIdFile = resolveLocalIdFile(localIdFilePath);
        this.leaseTimeoutMillis = leaseTimeoutMillis;
        this.reclaimGraceMillis = reclaimGraceMillis;
    }

    // ===================== 公开 API =====================

    /**
     * 解析（或分配）本实例的持久化广播身份。
     *
     * <p>本方法<b>永不抛异常</b>：任何一级失败都向下一级降级，保证消费容器总能启动。
     *
     * @param namespace 命名空间
     * @param topic 主题
     * @param group 消费者组
     * @param configuredId 显式配置的身份，可为 null / 空
     * @return 解析结果，永不为 null
     */
    public Resolution resolve(
            @NonNull String namespace,
            @NonNull String topic,
            @NonNull String group,
            String configuredId) {
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

        // ② 本地持久文件
        String fromFile = readLocalIdFile();
        if (fromFile != null) {
            if (claimQuietly(ns, topic, group, host, pid, fromFile, now)) {
                return new Resolution(fromFile, BroadcastInstanceSource.LOCAL_FILE);
            }
            // 注册中心拒绝（槽位被其它主机占用）→ 继续走回收/分配，不复用该值
            LOG.warn(
                    "Local broadcast instance id {} rejected by registry (slot taken by another"
                            + " host); allocating a fresh identity",
                    fromFile);
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
                    writeLocalIdFile(id);
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
     * @param namespace 命名空间
     * @param group 消费者组
     * @param instanceId 实例身份
     */
    public void release(
            @NonNull String namespace, @NonNull String group, @NonNull String instanceId) {
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
     * 返回底层注册中心（可能为 null）。
     *
     * @return 注册中心
     */
    public BroadcastInstanceRegistry registry() {
        return registry;
    }

    // ===================== 内部实现 =====================

    /** 向注册中心声明占用指定身份；失败静默（不阻塞启动）。返回是否成功占用。 */
    private boolean claimQuietly(
            String namespace,
            String topic,
            String group,
            String host,
            long pid,
            String preferredId,
            long now) {
        if (registry == null) {
            // 无注册中心时，配置值/本地文件值本身即权威，直接接受
            return true;
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
            return lease != null && preferredId.equals(lease.instanceId());
        } catch (RuntimeException ex) {
            LOG.debug("Broadcast instance claim failed for {}: {}", preferredId, ex.toString());
            return false;
        }
    }

    /**
     * 解析本地身份文件路径。
     *
     * <p>优先级：显式入参 &gt; 系统属性 {@code streammq.instance.id-file} &gt; 默认 {@code
     * ${user.home}/.streammq/instance-id}。系统属性取值为 {@code none}（或 {@code false}）时禁用本地文件。
     */
    private static Path resolveLocalIdFile(Path explicit) {
        if (explicit != null) {
            return explicit;
        }
        String fromProperty = System.getProperty(LOCAL_ID_FILE_PROPERTY);
        if (fromProperty != null) {
            String trimmed = fromProperty.trim();
            if (trimmed.isEmpty()
                    || DISABLED.equalsIgnoreCase(trimmed)
                    || "false".equalsIgnoreCase(trimmed)) {
                return null;
            }
            return Paths.get(trimmed);
        }
        String home = System.getProperty("user.home");
        if (home == null || home.isBlank()) {
            return null;
        }
        return Paths.get(home).resolve(DEFAULT_LOCAL_ID_FILE);
    }

    /** 读取本地身份文件；不存在 / 不可读 / 内容非法时返回 null。 */
    private String readLocalIdFile() {
        Path file = localIdFile;
        if (file == null || !Files.isRegularFile(file)) {
            return null;
        }
        try {
            String content = Files.readString(file, StandardCharsets.UTF_8).trim();
            return trimToNull(content);
        } catch (IOException | RuntimeException ex) {
            LOG.debug("Failed to read local broadcast instance id file {}: {}", file, ex);
            return null;
        }
    }

    /** 原子写入本地身份文件（写临时文件后 rename）；失败静默。 */
    private void writeLocalIdFile(String id) {
        Path file = localIdFile;
        if (file == null) {
            return;
        }
        try {
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp, id, StandardCharsets.UTF_8);
            try {
                Files.move(
                        tmp,
                        file,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException notAtomic) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | RuntimeException ex) {
            LOG.debug("Failed to persist local broadcast instance id to {}: {}", file, ex);
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
