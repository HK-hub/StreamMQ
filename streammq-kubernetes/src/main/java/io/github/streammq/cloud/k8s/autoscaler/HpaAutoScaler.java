/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.autoscaler;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.github.streammq.cloud.k8s.HpaMetricsProvider;
import io.github.streammq.cloud.k8s.operator.StreamMQCluster;
import io.github.streammq.cloud.k8s.operator.StreamMQK8sDefaults;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
@Component
public class HpaAutoScaler implements InitializingBean, DisposableBean {

    /** 消费者组后缀约定 */
    private static final String GROUP_SUFFIX = "-cg";

    /** 扩缩方向：无操作 */
    private static final String DIRECTION_NONE = "none";

    /** 扩缩方向：扩容 */
    private static final String DIRECTION_UP = "up";

    /** 扩缩方向：缩容 */
    private static final String DIRECTION_DOWN = "down";

    @Autowired(required = false)
    private KubernetesClient kubernetesClient;

    /**
     * HPA 指标提供者（可选注入）。
     *
     * <p>自动装配会注册默认实现；若用户显式关闭或自定义装配缺失，此处为 null—— 扫描循环以 WARN 日志降级跳过而非启动失败（belt+braces）。
     */
    @Autowired(required = false)
    private HpaMetricsProvider metricsProvider;

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
                        this::scanAndScale, 0, syncIntervalSeconds, TimeUnit.SECONDS);
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

    private void scanAndScale() {
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
            var clusters =
                    kubernetesClient
                            .resources(StreamMQCluster.class)
                            .inAnyNamespace()
                            .list()
                            .getItems()
                            .stream()
                            .filter(this::isHpaEnabled)
                            .toList();

            for (var cluster : clusters) {
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
        } catch (Exception e) {
            log.error("HPA scan failed: {}", e.getMessage(), e);
        }
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

        String topic = cluster.getMetadata().getName();
        String group = topic + GROUP_SUFFIX;

        long currentLag = metricsProvider.getConsumerLag(topic, group);
        double currentRate = metricsProvider.getConsumeRate(topic, group);

        // FAIL-CLOSED：无任何真实指标数据时绝不缩容。此前空指标 → avgLag=0 → 命中
        // 缩容分支 → 把繁忙消费者压到 minReplicas，是严重事故源。
        if (currentLag <= 0 && currentRate <= 0) {
            log.debug(
                    "No metrics available for {}/{}, skipping scaling decision (fail-closed)",
                    ns,
                    name);
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
            // 同步持久化 spec.replicas 到 CR：否则 reconcile 在 resync 周期会按旧 spec 把
            // Deployment 缩回去，两个控制器互相拉抖。写入后 reconcile 与 HPA 目标一致。
            StreamMQCluster toUpdate = new StreamMQCluster();
            toUpdate.setApiVersion(cluster.getApiVersion());
            toUpdate.setKind(cluster.getKind());
            toUpdate.setMetadata(cluster.getMetadata());
            toUpdate.setSpec(cluster.getSpec());
            toUpdate.getSpec().setReplicas(replicas);
            kubernetesClient
                    .resources(StreamMQCluster.class)
                    .inNamespace(ns)
                    .withName(name)
                    .replace(toUpdate);
            return true;
        } catch (Exception e) {
            log.error(
                    "Failed to scale Deployment {}/{} to {} replicas: {}",
                    ns,
                    name,
                    replicas,
                    e.getMessage());
            return false;
        }
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
