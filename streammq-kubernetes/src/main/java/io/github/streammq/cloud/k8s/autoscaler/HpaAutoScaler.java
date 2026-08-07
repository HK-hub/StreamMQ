package io.github.streammq.cloud.k8s.autoscaler;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.github.streammq.cloud.k8s.HpaMetricsProvider;
import io.github.streammq.cloud.k8s.operator.StreamMQCluster;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HPA auto-scaling controller.
 *
 * <p>Periodically collects consumer lag and rate metrics from
 * {@link HpaMetricsProvider} and makes scaling decisions for
 * {@link StreamMQCluster} resources with auto-scale enabled.
 *
 * <p>Scaling decision logic:
 * <ul>
 *   <li>If average consumer lag &gt; targetLag * scaleUpThreshold% and replicas &lt; maxReplicas
 *       &rarr; scale up</li>
 *   <li>If average consumer lag &lt; targetLag * scaleDownThreshold% and replicas &gt; minReplicas
 *       &rarr; scale down</li>
 *   <li>Scale-up/down is followed by a cooldown period to prevent thrashing</li>
 *   <li>Stabilization window: requires consistent readings over multiple cycles</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
@Slf4j
@Component
public class HpaAutoScaler implements InitializingBean, DisposableBean {

    @Autowired
    private KubernetesClient kubernetesClient;

    @Autowired
    private HpaMetricsProvider metricsProvider;

    private long syncIntervalSeconds = 30;

    private long defaultTargetLag = 100;

    private int scaleUpThreshold = 80;

    private int scaleDownThreshold = 20;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "streammq-hpa-scaler");
        t.setDaemon(true);
        return t;
    });

    private ScheduledFuture<?> scanFuture;

    private final ConcurrentHashMap<String, Long> lastScaleTime = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, String> lastScaleDirection = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, StabilizationWindow> stabilizationWindows = new ConcurrentHashMap<>();

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
        log.info("Starting HpaAutoScaler with syncIntervalSeconds={}", syncIntervalSeconds);
        scanFuture = scheduler.scheduleAtFixedRate(this::scanAndScale, 0, syncIntervalSeconds, TimeUnit.SECONDS);
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
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        log.info("HpaAutoScaler stopped");
    }

    private void scanAndScale() {
        try {
            var clusters = kubernetesClient.resources(StreamMQCluster.class)
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
                    log.error("Failed to process cluster {}/{}: {}",
                        cluster.getMetadata().getNamespace(), cluster.getMetadata().getName(),
                        e.getMessage(), e);
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

        int minReplicas = autoScale.getMinReplicas() != null ? autoScale.getMinReplicas() : 1;
        int maxReplicas = autoScale.getMaxReplicas() != null ? autoScale.getMaxReplicas() : 10;
        int targetLag = autoScale.getTargetLag() != null ? autoScale.getTargetLag() : (int) defaultTargetLag;
        int scaleUpPct = autoScale.getScaleUpThreshold() != null ? autoScale.getScaleUpThreshold() : scaleUpThreshold;
        int scaleDownPct = autoScale.getScaleDownThreshold() != null
            ? autoScale.getScaleDownThreshold() : scaleDownThreshold;
        int scaleUpCooldown = autoScale.getScaleUpCooldownSeconds() != null
            ? autoScale.getScaleUpCooldownSeconds() : 60;
        int scaleDownCooldown = autoScale.getScaleDownCooldownSeconds() != null
            ? autoScale.getScaleDownCooldownSeconds() : 300;
        int stabilizationSecs = autoScale.getStabilizationWindowSeconds() != null
            ? autoScale.getStabilizationWindowSeconds() : 300;

        int currentReplicas = getCurrentReplicas(cluster);
        if (currentReplicas <= 0) {
            currentReplicas = cluster.getSpec().getReplicas() != null ? cluster.getSpec().getReplicas() : 3;
        }

        String topic = cluster.getMetadata().getName();
        String group = cluster.getMetadata().getName() + "-cg";

        long currentLag = metricsProvider.getConsumerLag(topic, group);
        double currentRate = metricsProvider.getConsumeRate(topic, group);

        double targetLagVal = targetLag;
        double avgLag = currentLag;
        int desiredReplicas = currentReplicas;
        String direction = "none";

        if (avgLag > targetLagVal * scaleUpPct / 100.0) {
            double ratio = avgLag / Math.max(targetLagVal, 1);
            desiredReplicas = Math.min(maxReplicas, (int) Math.ceil(currentReplicas * ratio));
            direction = "up";
        } else if (avgLag < targetLagVal * scaleDownPct / 100.0) {
            double ratio = avgLag / Math.max(targetLagVal, 1);
            desiredReplicas = Math.max(minReplicas, (int) Math.floor(currentReplicas * ratio));
            direction = "down";
        }

        if ("none".equals(direction)) {
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
                log.info("Scaled cluster {}/{} from {} to {} replicas (direction: {}, lag: {}, rate: {})",
                    ns, name, currentReplicas, newReplicas, direction, currentLag, currentRate);
            }
        }
    }

    private boolean checkCooldown(String key, String direction, int scaleUpCooldown, int scaleDownCooldown) {
        Long lastTime = lastScaleTime.get(key);
        String lastDir = lastScaleDirection.get(key);
        if (lastTime != null && lastDir != null && lastDir.equals(direction)) {
            long cooldownMs = "up".equals(direction) ? scaleUpCooldown * 1000L : scaleDownCooldown * 1000L;
            long elapsed = System.currentTimeMillis() - lastTime;
            if (elapsed < cooldownMs) {
                log.debug("Scaling {} for {} in cooldown ({}ms remaining)", direction, key,
                    cooldownMs - elapsed);
                return false;
            }
        }
        return true;
    }

    private boolean checkStabilizationWindow(String key, String direction, int desiredReplicas,
                                              int stabilizationSecs) {
        StabilizationWindow window = stabilizationWindows.computeIfAbsent(key,
            k -> new StabilizationWindow(stabilizationSecs));
        window.record(direction, desiredReplicas);
        if (!window.isStable()) {
            log.debug("Stabilization window not satisfied for {} (direction: {}, desired: {})",
                key, direction, desiredReplicas);
            return false;
        }
        return true;
    }

    private boolean executeScaling(StreamMQCluster cluster, int replicas) {
        String ns = cluster.getMetadata().getNamespace();
        String name = cluster.getMetadata().getName();
        try {
            kubernetesClient.apps().deployments().inNamespace(ns)
                .withName(name).scale(replicas);
            cluster.getSpec().setReplicas(replicas);
            return true;
        } catch (Exception e) {
            log.error("Failed to scale Deployment {}/{} to {} replicas: {}", ns, name, replicas, e.getMessage());
            return false;
        }
    }

    private int getCurrentReplicas(StreamMQCluster cluster) {
        if (cluster.getStatus() != null && cluster.getStatus().getReplicas() != null
            && cluster.getStatus().getReplicas() > 0) {
            return cluster.getStatus().getReplicas();
        }
        if (cluster.getSpec().getReplicas() != null && cluster.getSpec().getReplicas() > 0) {
            return cluster.getSpec().getReplicas();
        }
        return 3;
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
     * Stabilization window tracks recent scaling decisions.
     */
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
