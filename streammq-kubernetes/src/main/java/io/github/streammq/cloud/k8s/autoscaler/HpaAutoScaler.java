/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.autoscaler;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.github.streammq.cloud.k8s.HpaMetricsProvider;
import io.github.streammq.cloud.k8s.operator.StreamMQCluster;
import io.github.streammq.cloud.k8s.operator.StreamMQK8sDefaults;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * HPA auto-scaling controller.
 *
 * <p>Periodically collects consumer lag and rate metrics from {@link HpaMetricsProvider} and makes
 * scaling decisions for {@link StreamMQCluster} resources with auto-scale enabled.
 *
 * <p>Scaling decision logic:
 *
 * <ul>
 *   <li>If average consumer lag &gt; targetLag * scaleUpThreshold% and replicas &lt; maxReplicas
 *       &rarr; scale up
 *   <li>If average consumer lag &lt; targetLag * scaleDownThreshold% and replicas &gt; minReplicas
 *       &rarr; scale down
 *   <li>Scale-up/down is followed by a cooldown period to prevent thrashing
 *   <li>Stabilization window: requires consistent readings over multiple cycles
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class HpaAutoScaler implements InitializingBean, DisposableBean {

    /** 扩缩方向：无操作 */
    private static final String DIRECTION_NONE = "none";

    /** 扩缩方向：扩容 */
    private static final String DIRECTION_UP = "up";

    /** 扩缩方向：缩容 */
    private static final String DIRECTION_DOWN = "down";

    /** HTTP 409：resourceVersion 乐观锁冲突 */
    private static final int HTTP_CONFLICT = 409;

    /** spec.replicas 回写时 409 冲突的最大重试次数（每次重试重新读取最新 resourceVersion） */
    private static final int REPLICAS_PATCH_MAX_RETRIES = 3;

    /** 限频告警最小间隔（毫秒）：避免每轮扫描刷屏 */
    private static final long WARN_THROTTLE_MS = 300_000L;

    @Autowired(required = false)
    private KubernetesClient kubernetesClient;

    /**
     * HPA 指标提供者（可选注入）。
     *
     * <p>自动装配会注册默认实现；若用户显式关闭或自定义装配缺失，此处为 null—— 扫描循环以 WARN 日志降级跳过而非启动失败（belt+braces）。
     */
    @Autowired(required = false)
    private HpaMetricsProvider metricsProvider;

    /** 是否全命名空间扫描（默认 true；与 StreamMQClusterController 的 watch 语义保持一致） */
    private volatile boolean watchAllNamespaces = true;

    /** 收敛模式下的扫描命名空间列表（仅当 {@code watchAllNamespaces=false} 时生效） */
    private volatile List<String> watchNamespaces = List.of();

    /**
     * 积压探针（可选，来自 {@code streammq-diagnostics}）。
     *
     * <p><b>K6 生产者侧：</b>kubernetes 模块依赖面只有 core + fabric8，无法直接读 Redis；存在探针 Bean 时每轮扫描前用真实 {@code
     * XLEN/XPENDING} 数据刷新 lag 指标， 探针缺席时指标需由用户自定义生产者（{@link
     * HpaMetricsProvider}）写入——两条路径都不会静默失效（跳过决策一定伴随限频 WARN）。
     */
    @Autowired(required = false)
    private ObjectProvider<BacklogProbe> backlogProbeProvider;

    /** 同步间隔（秒） */
    private long syncIntervalSeconds = StreamMQK8sDefaults.DEFAULT_RECONCILE_INTERVAL_SECONDS;

    /** 默认目标积压 */
    private long defaultTargetLag = StreamMQK8sDefaults.AUTOSCALE_TARGET_LAG;

    /** 默认扩容阈值百分比 */
    private int scaleUpThreshold = StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_THRESHOLD;

    /** 默认缩容阈值百分比 */
    private int scaleDownThreshold = StreamMQK8sDefaults.AUTOSCALE_SCALE_DOWN_THRESHOLD;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, THREAD_HPA_SCALER);
                        t.setDaemon(true);
                        return t;
                    });

    /** HPA 调度线程名 */
    private static final String THREAD_HPA_SCALER = "streammq-hpa-scaler";

    private ScheduledFuture<?> scanFuture;

    private final ConcurrentHashMap<String, Long> lastScaleTime = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, String> lastScaleDirection = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, StabilizationWindow> stabilizationWindows =
            new ConcurrentHashMap<>();

    /** 「未声明 autoScale.topic/consumerGroup」告警限频（key → 上次告警毫秒） */
    private final ConcurrentHashMap<String, Long> lastMissingTopicWarn = new ConcurrentHashMap<>();

    /** 「该 topic/group 无指标」告警限频（key → 上次告警毫秒） */
    private final ConcurrentHashMap<String, Long> lastNoMetricsWarn = new ConcurrentHashMap<>();

    /** 「扫描范围为空」告警限频（key → 上次告警毫秒） */
    private final ConcurrentHashMap<String, Long> lastScopeWarn = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public void afterPropertiesSet() {
        start();
    }

    @Override
    public void destroy() {
        stop();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("HpaAutoScaler already started");
            return;
        }
        if (kubernetesClient == null) {
            log.warn("KubernetesClient not available, HpaAutoScaler disabled");
            running.set(false);
            return;
        }
        log.info("Starting HpaAutoScaler with syncIntervalSeconds={}", syncIntervalSeconds);
        scanFuture =
                scheduler.scheduleAtFixedRate(
                        this::scanOnce, 0, syncIntervalSeconds, TimeUnit.SECONDS);
        log.info("HpaAutoScaler started");
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping HpaAutoScaler...");
        if (scanFuture != null) {
            scanFuture.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(
                    StreamMQK8sDefaults.OPERATOR_AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        log.info("HpaAutoScaler stopped");
    }

    /**
     * 执行一次扫描与扩缩决策。
     *
     * <p>由调度线程周期调用；同时作为单次扫描入口供测试直接调用（断言扫描范围、指标维度与回写行为）。
     */
    public void scanOnce() {
        if (kubernetesClient == null) {
            // KubernetesClient 不可用（非 K8s 环境）时不执行扫描
            return;
        }
        if (metricsProvider == null) {
            // 指标提供者缺失时无法做出扩缩决策：降级跳过并提示，而非抛 NPE
            log.warn(
                    "HpaMetricsProvider not available, skipping HPA scaling decisions"
                            + " (register a HpaMetricsProvider bean to enable autoscaling)");
            return;
        }
        try {
            var clusters = listHpaEnabledClusters();
            // K6：先用积压探针把真实 lag 灌入 HpaMetricsProvider，再做扩缩决策
            refreshMetricsFromProbe(clusters);
            Set<String> liveKeys = new HashSet<>();
            for (var cluster : clusters) {
                liveKeys.add(clusterKey(cluster));
                try {
                    processCluster(cluster);
                } catch (Exception e) {
                    log.error(
                            "Failed to process cluster {}/{}: {}",
                            cluster.getMetadata().getNamespace(),
                            cluster.getMetadata().getName(),
                            e.getMessage(),
                            e);
                }
            }
            // K9：清理已消失 CR 的每-CR 状态，避免 map 随 CR 生命周期只增不减
            pruneStaleState(liveKeys);
        } catch (Exception e) {
            log.error("HPA scan failed: {}", e.getMessage(), e);
        }
    }

    /**
     * 列出扫描范围内的 HPA 启用集群。
     *
     * <p>K9：扫描范围与 {@code StreamMQClusterController} 的 watch 语义一致——默认全命名空间（需 ClusterRole RBAC），
     * {@code operator.watch-all-namespaces=false} 时收敛到 {@code operator.watch-namespaces} 列表， 不再无条件
     * {@code inAnyNamespace()} 全集群 list。
     */
    private List<StreamMQCluster> listHpaEnabledClusters() {
        var operation = kubernetesClient.resources(StreamMQCluster.class);
        List<StreamMQCluster> clusters = new ArrayList<>();
        if (watchAllNamespaces) {
            clusters.addAll(operation.inAnyNamespace().list().getItems());
        } else if (watchNamespaces.isEmpty()) {
            warnThrottled(
                    lastScopeWarn,
                    "scope",
                    "HPA scan scope is empty (operator.watch-all-namespaces=false without"
                            + " operator.watch-namespaces); no cluster will be scanned");
            return List.of();
        } else {
            for (String ns : watchNamespaces) {
                clusters.addAll(operation.inNamespace(ns).list().getItems());
            }
        }
        return clusters.stream().filter(this::isHpaEnabled).toList();
    }

    /**
     * 以 {@link BacklogProbe} 的真实积压数据刷新 {@link HpaMetricsProvider} 的 lag 指标（K6 生产者侧）。
     *
     * <p>只对该 CR 显式声明且与消费侧一致的 {@code autoScale.topic / autoScale.consumerGroup} 采样 {@code
     * pendingCount}（XPENDING 未确认消息数作为消费积压）；未声明 topic/group 的 CR 由决策路径输出限频 WARN。探针抛出异常时按 CR 隔离，不影响其它
     * CR 与本轮决策。
     *
     * @param clusters 本轮扫描范围内的 CR 列表
     */
    private void refreshMetricsFromProbe(List<StreamMQCluster> clusters) {
        BacklogProbe probe =
                backlogProbeProvider == null ? null : backlogProbeProvider.getIfAvailable();
        if (probe == null) {
            return;
        }
        for (var cluster : clusters) {
            var autoScale = cluster.getSpec() == null ? null : cluster.getSpec().getAutoScale();
            if (autoScale == null) {
                continue;
            }
            String topic = autoScale.getTopic();
            String group = autoScale.getConsumerGroup();
            if (topic == null || topic.isBlank() || group == null || group.isBlank()) {
                continue;
            }
            try {
                var result = probe.probe(topic, group);
                if (result != null) {
                    // 两类积压用不同信号表达，只看 XPENDING 会漏判关键场景：
                    //  · 消费者跟不上（读/处理慢）→ XPENDING 增长
                    //  · 消费者进程全挂 → XPENDING≈0（没人读就没有未确认）而 XLEN 持续增长
                    // 后者若不区分，HPA 在最需要扩容时判定"无积压"永不扩容。
                    long effectiveLag =
                            result.consumerCount() == 0
                                    ? result.streamSize()
                                    : result.pendingCount();
                    metricsProvider.recordLag(topic, group, effectiveLag);
                }
            } catch (Exception e) {
                log.warn(
                        "BacklogProbe failed for topic={}, group={}: {}",
                        topic,
                        group,
                        e.getMessage());
            }
        }
    }

    /** 清理已不存在 CR 的扩缩状态与告警限频记录（K9）。 */
    private void pruneStaleState(Set<String> liveKeys) {
        lastScaleTime.keySet().removeIf(key -> !liveKeys.contains(key));
        lastScaleDirection.keySet().removeIf(key -> !liveKeys.contains(key));
        stabilizationWindows.keySet().removeIf(key -> !liveKeys.contains(key));
        lastMissingTopicWarn.keySet().removeIf(key -> !liveKeys.contains(key));
        lastNoMetricsWarn.keySet().removeIf(key -> !liveKeys.contains(key));
    }

    /** 限频告警：同一 key 在 {@link #WARN_THROTTLE_MS} 内只输出一条 WARN。 */
    private void warnThrottled(
            ConcurrentHashMap<String, Long> warnState, String key, String message) {
        long now = System.currentTimeMillis();
        Long lastWarn = warnState.put(key, now);
        if (lastWarn == null || now - lastWarn >= WARN_THROTTLE_MS) {
            log.warn(message);
        }
    }

    private static String clusterKey(StreamMQCluster cluster) {
        return cluster.getMetadata().getNamespace() + "/" + cluster.getMetadata().getName();
    }

    private void processCluster(StreamMQCluster cluster) {
        String ns = cluster.getMetadata().getNamespace();
        String name = cluster.getMetadata().getName();
        String key = ns + "/" + name;

        var autoScale = cluster.getSpec().getAutoScale();
        if (autoScale == null || !Boolean.TRUE.equals(autoScale.getEnabled())) {
            return;
        }

        int minReplicas =
                autoScale.getMinReplicas() != null
                        ? autoScale.getMinReplicas()
                        : StreamMQK8sDefaults.AUTOSCALE_MIN_REPLICAS;
        int maxReplicas =
                autoScale.getMaxReplicas() != null
                        ? autoScale.getMaxReplicas()
                        : StreamMQK8sDefaults.AUTOSCALE_MAX_REPLICAS;
        int targetLag =
                autoScale.getTargetLag() != null
                        ? autoScale.getTargetLag()
                        : (int) defaultTargetLag;
        int scaleUpPct =
                autoScale.getScaleUpThreshold() != null
                        ? autoScale.getScaleUpThreshold()
                        : scaleUpThreshold;
        int scaleDownPct =
                autoScale.getScaleDownThreshold() != null
                        ? autoScale.getScaleDownThreshold()
                        : scaleDownThreshold;
        int scaleUpCooldown =
                autoScale.getScaleUpCooldownSeconds() != null
                        ? autoScale.getScaleUpCooldownSeconds()
                        : StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_COOLDOWN_SECONDS;
        int scaleDownCooldown =
                autoScale.getScaleDownCooldownSeconds() != null
                        ? autoScale.getScaleDownCooldownSeconds()
                        : StreamMQK8sDefaults.AUTOSCALE_SCALE_DOWN_COOLDOWN_SECONDS;
        int stabilizationSecs =
                autoScale.getStabilizationWindowSeconds() != null
                        ? autoScale.getStabilizationWindowSeconds()
                        : StreamMQK8sDefaults.AUTOSCALE_STABILIZATION_WINDOW_SECONDS;

        int currentReplicas = getCurrentReplicas(cluster);
        if (currentReplicas <= 0) {
            currentReplicas =
                    cluster.getSpec().getReplicas() != null
                            ? cluster.getSpec().getReplicas()
                            : StreamMQK8sDefaults.DEFAULT_REPLICAS;
        }

        // K6：指标维度必须与真实消费侧一致——topic/consumerGroup 由 CR spec.autoScale 显式声明
        // （取值即消费者部署中 @StreamMQConsumer 的 topic/consumerGroup），框架不存在
        // 「CR 名 + -cg」这类命名约定；未声明时 fail-closed 跳过并限频告警。
        String topic = autoScale.getTopic();
        String group = autoScale.getConsumerGroup();
        if (topic == null || topic.isBlank() || group == null || group.isBlank()) {
            warnThrottled(
                    lastMissingTopicWarn,
                    key,
                    "Cluster "
                            + key
                            + " enables autoScale but spec.autoScale.topic/consumerGroup is not"
                            + " declared; HPA cannot locate metrics. Declare the same"
                            + " topic/consumerGroup as @StreamMQConsumer in the consumer"
                            + " deployment. Skipping scaling decision (fail-closed).");
            return;
        }

        long currentLag = metricsProvider.getConsumerLag(topic, group);
        double currentRate = metricsProvider.getConsumeRate(topic, group);

        // FAIL-CLOSED：无任何真实指标数据时绝不缩容。此前空指标 → avgLag=0 → 命中
        // 缩容分支 → 把繁忙消费者压到 minReplicas，是严重事故源。
        if (currentLag <= 0 && currentRate <= 0) {
            warnThrottled(
                    lastNoMetricsWarn,
                    key,
                    "No metrics available for cluster "
                            + key
                            + " (topic="
                            + topic
                            + ", group="
                            + group
                            + "), skipping scaling decision (fail-closed). Ensure a producer"
                            + " records lag/rate for this topic+group (e.g. BacklogProbe-based"
                            + " collector or a custom HpaMetricsProvider).");
            return;
        }

        double targetLagVal = targetLag;
        double avgLag = currentLag;
        int desiredReplicas = currentReplicas;
        String direction = DIRECTION_NONE;

        if (avgLag > targetLagVal * scaleUpPct / 100.0) {
            double ratio = avgLag / Math.max(targetLagVal, 1);
            desiredReplicas = Math.min(maxReplicas, (int) Math.ceil(currentReplicas * ratio));
            direction = DIRECTION_UP;
        } else if (avgLag > 0 && avgLag < targetLagVal * scaleDownPct / 100.0) {
            double ratio = avgLag / Math.max(targetLagVal, 1);
            desiredReplicas = Math.max(minReplicas, (int) Math.floor(currentReplicas * ratio));
            direction = DIRECTION_DOWN;
        }

        if (DIRECTION_NONE.equals(direction)) {
            stabilizationWindows.remove(key);
            return;
        }

        if (!checkCooldown(key, direction, scaleUpCooldown, scaleDownCooldown)) {
            return;
        }
        if (!checkStabilizationWindow(key, direction, desiredReplicas, stabilizationSecs)) {
            return;
        }

        int newReplicas = Math.max(minReplicas, Math.min(maxReplicas, desiredReplicas));
        if (newReplicas != currentReplicas) {
            if (executeScaling(cluster, newReplicas)) {
                lastScaleTime.put(key, System.currentTimeMillis());
                lastScaleDirection.put(key, direction);
                log.info(
                        "Scaled cluster {}/{} from {} to {} replicas (direction: {}, lag: {}, rate:"
                                + " {})",
                        ns,
                        name,
                        currentReplicas,
                        newReplicas,
                        direction,
                        currentLag,
                        currentRate);
            }
        }
    }

    private boolean checkCooldown(
            String key, String direction, int scaleUpCooldown, int scaleDownCooldown) {
        Long lastTime = lastScaleTime.get(key);
        String lastDir = lastScaleDirection.get(key);
        if (lastTime != null && lastDir != null && lastDir.equals(direction)) {
            long cooldownMs =
                    DIRECTION_UP.equals(direction)
                            ? scaleUpCooldown * 1000L
                            : scaleDownCooldown * 1000L;
            long elapsed = System.currentTimeMillis() - lastTime;
            if (elapsed < cooldownMs) {
                log.debug(
                        "Scaling {} for {} in cooldown ({}ms remaining)",
                        direction,
                        key,
                        cooldownMs - elapsed);
                return false;
            }
        }
        return true;
    }

    private boolean checkStabilizationWindow(
            String key, String direction, int desiredReplicas, int stabilizationSecs) {
        StabilizationWindow window =
                stabilizationWindows.computeIfAbsent(
                        key, k -> new StabilizationWindow(stabilizationSecs));
        window.record(direction, desiredReplicas);
        if (!window.isStable()) {
            log.debug(
                    "Stabilization window not satisfied for {} (direction: {}, desired: {})",
                    key,
                    direction,
                    desiredReplicas);
            return false;
        }
        return true;
    }

    @SuppressWarnings("deprecation")
    private boolean executeScaling(StreamMQCluster cluster, int replicas) {
        String ns = cluster.getMetadata().getNamespace();
        String name = cluster.getMetadata().getName();
        try {
            kubernetesClient.apps().deployments().inNamespace(ns).withName(name).scale(replicas);
        } catch (Exception e) {
            log.error(
                    "Failed to scale Deployment {}/{} to {} replicas: {}",
                    ns,
                    name,
                    replicas,
                    e.getMessage(),
                    e);
            return false;
        }
        // 同步持久化 spec.replicas 到 CR：否则 reconcile 在 resync 周期会按旧 spec 把
        // Deployment 缩回去，两个控制器互相拉抖。写入后 reconcile 与 HPA 目标一致。
        return persistReplicas(ns, name, replicas);
    }

    /**
     * 以 JSON merge patch + resourceVersion 乐观锁回写 {@code spec.replicas}。
     *
     * <p><b>K7：为什么不是 replace</b>——旧实现把 informer 快照整对象 replace 回去，会用陈旧快照覆盖用户并发修改的 spec
     * 字段（镜像、backend、resources 等）且无冲突检测。现在：
     *
     * <ol>
     *   <li>每次尝试先 GET 最新 CR，取当前 resourceVersion；
     *   <li>merge patch 携带该 resourceVersion，只写 {@code spec.replicas} 一个字段，其余字段不动；
     *   <li>若期间 CR 被改动，APIServer 返回 409，重新读取最新版本后重试（最多 {@link #REPLICAS_PATCH_MAX_RETRIES}
     *       次），绝不静默覆盖其它字段。
     * </ol>
     *
     * @param ns 命名空间
     * @param name CR 名
     * @param replicas 目标副本数
     * @return true 表示 spec.replicas 已持久化
     */
    private boolean persistReplicas(String ns, String name, int replicas) {
        for (int attempt = 1; attempt <= REPLICAS_PATCH_MAX_RETRIES; attempt++) {
            var current =
                    kubernetesClient
                            .resources(StreamMQCluster.class)
                            .inNamespace(ns)
                            .withName(name)
                            .get();
            if (current == null || current.getMetadata() == null) {
                log.error("Cannot persist spec.replicas for {}/{}: CR not found", ns, name);
                return false;
            }
            String resourceVersion = current.getMetadata().getResourceVersion();
            // resourceVersion 为空（极端场景）时退化为无锁 merge patch，至少不覆盖其它字段
            String patch =
                    resourceVersion == null
                            ? "{\"spec\":{\"replicas\":" + replicas + "}}"
                            : "{\"metadata\":{\"resourceVersion\":\""
                                    + resourceVersion
                                    + "\"},\"spec\":{\"replicas\":"
                                    + replicas
                                    + "}}";
            try {
                kubernetesClient
                        .resources(StreamMQCluster.class)
                        .inNamespace(ns)
                        .withName(name)
                        .patch(PatchContext.of(PatchType.JSON_MERGE), patch);
                return true;
            } catch (KubernetesClientException e) {
                if (e.getCode() != HTTP_CONFLICT) {
                    log.error(
                            "Failed to patch spec.replicas for {}/{}: {}",
                            ns,
                            name,
                            e.getMessage());
                    return false;
                }
                log.info(
                        "spec.replicas patch for {}/{} hit 409 (resourceVersion={}) on attempt"
                                + " {}/{}; re-reading latest version and retrying",
                        ns,
                        name,
                        resourceVersion,
                        attempt,
                        REPLICAS_PATCH_MAX_RETRIES);
            }
        }
        log.error(
                "Failed to patch spec.replicas for {}/{} after {} optimistic-lock retries",
                ns,
                name,
                REPLICAS_PATCH_MAX_RETRIES);
        return false;
    }

    private int getCurrentReplicas(StreamMQCluster cluster) {
        if (cluster.getStatus() != null
                && cluster.getStatus().getReplicas() != null
                && cluster.getStatus().getReplicas() > 0) {
            return cluster.getStatus().getReplicas();
        }
        if (cluster.getSpec().getReplicas() != null && cluster.getSpec().getReplicas() > 0) {
            return cluster.getSpec().getReplicas();
        }
        return StreamMQK8sDefaults.DEFAULT_REPLICAS;
    }

    private boolean isHpaEnabled(StreamMQCluster cluster) {
        var autoScale = cluster.getSpec().getAutoScale();
        return autoScale != null && Boolean.TRUE.equals(autoScale.getEnabled());
    }

    public void setSyncIntervalSeconds(long syncIntervalSeconds) {
        this.syncIntervalSeconds = syncIntervalSeconds;
    }

    public void setDefaultTargetLag(long defaultTargetLag) {
        this.defaultTargetLag = defaultTargetLag;
    }

    public void setScaleUpThreshold(int scaleUpThreshold) {
        this.scaleUpThreshold = scaleUpThreshold;
    }

    public void setScaleDownThreshold(int scaleDownThreshold) {
        this.scaleDownThreshold = scaleDownThreshold;
    }

    /**
     * 设置是否全命名空间扫描。
     *
     * <p>默认 true（需 ClusterRole 级 RBAC）；与 {@code operator.watch-all-namespaces} 保持同一开关（K9）。
     *
     * @param watchAllNamespaces true 表示全命名空间扫描
     */
    public void setWatchAllNamespaces(boolean watchAllNamespaces) {
        this.watchAllNamespaces = watchAllNamespaces;
    }

    /**
     * 设置收敛模式的扫描命名空间列表。
     *
     * @param namespaces 命名空间列表；为空时扫描范围为空（仅告警，不扩缩）
     */
    public void setWatchNamespaces(List<String> namespaces) {
        this.watchNamespaces = namespaces == null ? List.of() : List.copyOf(namespaces);
    }

    /**
     * 设置 KubernetesClient。
     *
     * <p>生产环境由 Spring 按类型注入；显式 setter 供测试装配（fabric8 mock server）使用。
     *
     * @param kubernetesClient fabric8 客户端
     */
    public void setKubernetesClient(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    /**
     * 设置 HPA 指标提供者。
     *
     * <p>生产环境由 Spring 注入；显式 setter 供测试装配使用。
     *
     * @param metricsProvider 指标提供者
     */
    public void setMetricsProvider(HpaMetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    /**
     * 设置积压探针提供者。
     *
     * <p>生产环境由 Spring 注入（{@code ObjectProvider} 以容忍探针缺席）；显式 setter 供测试装配使用。
     *
     * @param backlogProbeProvider 积压探针提供者
     */
    public void setBacklogProbeProvider(ObjectProvider<BacklogProbe> backlogProbeProvider) {
        this.backlogProbeProvider = backlogProbeProvider;
    }

    /** Stabilization window tracks recent scaling decisions. */
    private static class StabilizationWindow {
        private final long windowMs;
        private final List<Decision> decisions = new ArrayList<>();

        StabilizationWindow(int windowSeconds) {
            this.windowMs = windowSeconds * 1000L;
        }

        synchronized void record(String direction, int desiredReplicas) {
            decisions.add(new Decision(System.currentTimeMillis(), direction, desiredReplicas));
            prune();
        }

        synchronized boolean isStable() {
            prune();
            if (decisions.isEmpty()) {
                return false;
            }
            String firstDirection = decisions.get(0).direction;
            for (var d : decisions) {
                if (!d.direction.equals(firstDirection)) {
                    return false;
                }
            }
            return decisions.size() >= 2;
        }

        private void prune() {
            long now = System.currentTimeMillis();
            decisions.removeIf(d -> now - d.timestamp > windowMs);
        }

        private record Decision(long timestamp, String direction, int desiredReplicas) {}
    }
}
