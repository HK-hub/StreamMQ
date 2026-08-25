/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.github.streammq.cloud.k8s.NoopConfigRefresher;
import io.github.streammq.cloud.k8s.StreamMQConfigRefresher;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * ConfigMap-based configuration hot-reload implementation.
 *
 * <p>Watches Kubernetes ConfigMaps for changes and applies updated configuration to running {@link
 * StreamMQListenerContainer} instances without pod restart.
 *
 * <p>Supports:
 *
 * <ul>
 *   <li>Multi-namespace, multi-ConfigMap watching
 *   <li>Label-based ConfigMap filtering
 *   <li>Automatic retry on parse failures
 *   <li>Periodic full-sync to prevent event loss
 * </ul>
 *
 * <p>ConfigMap data keys:
 *
 * <ul>
 *   <li>{@code maxReconsumeTimes} - max retry count
 *   <li>{@code retryIntervals} - comma-separated retry intervals in ms
 *   <li>{@code consumerThreadMin} - min consumer thread pool size
 *   <li>{@code consumerThreadMax} - max consumer thread pool size
 *   <li>{@code retryScanMs} - retry scan interval in ms
 *   <li>{@code delayScanMs} - delayed message scan interval in ms
 *   <li>{@code pullBatchSize} - pull batch size
 *   <li>{@code consumeTimeoutMs} - consume timeout in ms
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class ConfigMapConfigRefresher
        implements StreamMQConfigRefresher,
                Runnable,
                org.springframework.context.SmartLifecycle,
                AutoCloseable {

    @Autowired(required = false)
    private KubernetesClient kubernetesClient;

    /** 用户委托刷新器（由自动装配注入，缺省为 Noop） */
    private final StreamMQConfigRefresher customRefresher;

    /** watch 命名空间列表（可注入覆盖默认值） */
    private List<String> watchNamespaces;

    public ConfigMapConfigRefresher() {
        this.customRefresher = new NoopConfigRefresher();
    }

    public ConfigMapConfigRefresher(StreamMQConfigRefresher delegate) {
        this.customRefresher =
                java.util.Objects.requireNonNullElseGet(delegate, NoopConfigRefresher::new);
    }

    public void setWatchNamespaces(List<String> namespaces) {
        this.watchNamespaces = namespaces;
    }

    private SharedInformerFactory informerFactory;

    private final List<ConfigMapWatchConfig> watchConfigs = new ArrayList<>();

    private final Map<String, SharedIndexInformer<ConfigMap>> informers = new ConcurrentHashMap<>();

    private final Map<String, String> processedVersions = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ScheduledExecutorService syncScheduler;

    private ScheduledFuture<?> syncFuture;

    /** ConfigMap 标签 key */
    private static final String STREAMMQ_CONFIG_LABEL = "streammq.io/config";

    /** 同步线程名 */
    private static final String THREAD_CONFIGMAP_SYNC = "streammq-configmap-sync";

    /** 全量同步间隔（秒） */
    private static final int SYNC_INTERVAL_SECONDS = 30;

    /** 默认 watch 命名空间 */
    private static final String DEFAULT_WATCH_NAMESPACE = "default";

    /** 默认 watch ConfigMap 名称 */
    public static final String DEFAULT_WATCH_CONFIG_MAP_NAME = "streammq-consumer-config";

    /** 默认 watch 刷新间隔（毫秒） */
    private static final long DEFAULT_WATCH_REFRESH_INTERVAL_MS = 5_000L;

    // ==================== ConfigMap data key 契约 ====================
    /** data key：最大重试次数 */
    public static final String CM_KEY_MAX_RECONSUME_TIMES = "maxReconsumeTimes";

    /** data key：重试间隔数组（逗号分隔毫秒值） */
    public static final String CM_KEY_RETRY_INTERVALS = "retryIntervals";

    /** data key：最小消费线程数 */
    public static final String CM_KEY_CONSUMER_THREAD_MIN = "consumerThreadMin";

    /** data key：最大消费线程数 */
    public static final String CM_KEY_CONSUMER_THREAD_MAX = "consumerThreadMax";

    /** data key：重试扫描间隔（毫秒） */
    public static final String CM_KEY_RETRY_SCAN_MS = "retryScanMs";

    /** data key：延时扫描间隔（毫秒） */
    public static final String CM_KEY_DELAY_SCAN_MS = "delayScanMs";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("ConfigMapConfigRefresher already started");
            return;
        }
        if (kubernetesClient == null) {
            log.warn("KubernetesClient not available, ConfigMapConfigRefresher disabled");
            running.set(false);
            return;
        }
        if (watchConfigs.isEmpty()) {
            addDefaultWatchConfig(watchNamespaces);
        }
        log.info("Starting ConfigMapConfigRefresher with {} watch configs", watchConfigs.size());

        informerFactory = kubernetesClient.informers();
        for (var watchConfig : watchConfigs) {
            if (!watchConfig.isEnabled()) {
                log.debug(
                        "Skipping disabled watch config: {}/{}",
                        watchConfig.getNamespace(),
                        watchConfig.getName());
                continue;
            }
            registerInformer(watchConfig);
        }
        informerFactory.startAllRegisteredInformers();

        syncScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, THREAD_CONFIGMAP_SYNC);
                            t.setDaemon(true);
                            return t;
                        });
        syncFuture =
                syncScheduler.scheduleAtFixedRate(
                        this, SYNC_INTERVAL_SECONDS, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
        log.info("ConfigMapConfigRefresher started");
    }

    @Override
    public void run() {
        try {
            for (var watchConfig : watchConfigs) {
                if (!watchConfig.isEnabled()) {
                    continue;
                }
                String informerKey = watchConfig.getNamespace() + "/" + watchConfig.getName();
                var informer = informers.get(informerKey);
                if (informer != null) {
                    var configMaps = informer.getIndexer().list();
                    for (var cm : configMaps) {
                        processConfigMap(cm, watchConfig);
                    }
                }
            }
        } catch (Exception e) {
            log.error("ConfigMap sync loop failed: {}", e.getMessage(), e);
        }
    }

    private void registerInformer(ConfigMapWatchConfig watchConfig) {
        String informerKey = watchConfig.getNamespace() + "/" + watchConfig.getName();
        var informer =
                informerFactory.sharedIndexInformerFor(
                        ConfigMap.class, watchConfig.getRefreshIntervalMs());

        informer.addEventHandler(
                new ResourceEventHandler<ConfigMap>() {
                    @Override
                    public void onAdd(ConfigMap configMap) {
                        if (matches(configMap, watchConfig)) {
                            log.info(
                                    "ConfigMap added: {}/{} (version: {})",
                                    configMap.getMetadata().getNamespace(),
                                    configMap.getMetadata().getName(),
                                    configMap.getMetadata().getResourceVersion());
                            processConfigMap(configMap, watchConfig);
                        }
                    }

                    @Override
                    public void onUpdate(ConfigMap oldConfigMap, ConfigMap newConfigMap) {
                        if (matches(newConfigMap, watchConfig)) {
                            String newVersion = newConfigMap.getMetadata().getResourceVersion();
                            String oldVersion = processedVersions.get(informerKey);
                            if (!newVersion.equals(oldVersion)) {
                                log.info(
                                        "ConfigMap updated: {}/{} (version: {} -> {})",
                                        newConfigMap.getMetadata().getNamespace(),
                                        newConfigMap.getMetadata().getName(),
                                        oldVersion,
                                        newVersion);
                                processConfigMap(newConfigMap, watchConfig);
                            }
                        }
                    }

                    @Override
                    public void onDelete(ConfigMap configMap, boolean deletedFinalStateUnknown) {
                        log.info(
                                "ConfigMap deleted: {}/{}",
                                configMap.getMetadata().getNamespace(),
                                configMap.getMetadata().getName());
                        processedVersions.remove(informerKey);
                    }
                });

        informers.put(informerKey, informer);
    }

    private boolean matches(ConfigMap configMap, ConfigMapWatchConfig watchConfig) {
        if (!watchConfig.getNamespace().equals(configMap.getMetadata().getNamespace())) {
            return false;
        }
        if (watchConfig.getName() != null
                && !watchConfig.getName().isEmpty()
                && !watchConfig.getName().equals(configMap.getMetadata().getName())) {
            return false;
        }
        var labels = configMap.getMetadata().getLabels();
        if (labels != null && STREAMMQ_CONFIG_LABEL.equals(labels.get(STREAMMQ_CONFIG_LABEL))) {
            return true;
        }
        return watchConfig.getName() != null
                && !watchConfig.getName().isEmpty()
                && watchConfig.getName().equals(configMap.getMetadata().getName());
    }

    private void processConfigMap(ConfigMap configMap, ConfigMapWatchConfig watchConfig) {
        String informerKey = watchConfig.getNamespace() + "/" + watchConfig.getName();
        String version = configMap.getMetadata().getResourceVersion();
        Map<String, String> data = configMap.getData();
        if (data == null || data.isEmpty()) {
            log.debug(
                    "ConfigMap {}/{} has no data, skipping",
                    configMap.getMetadata().getNamespace(),
                    configMap.getMetadata().getName());
            return;
        }
        try {
            boolean changed = applyConfigMapData(data);
            if (changed) {
                processedVersions.put(informerKey, version);
                log.info(
                        "Applied config from ConfigMap {}/{} (version: {})",
                        configMap.getMetadata().getNamespace(),
                        configMap.getMetadata().getName(),
                        version);
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to apply ConfigMap {}/{} data: {}",
                    configMap.getMetadata().getNamespace(),
                    configMap.getMetadata().getName(),
                    e.getMessage());
        }
    }

    public boolean applyConfigMapData(Map<String, String> data) {
        if (data == null || data.isEmpty()) {
            return false;
        }
        boolean changed = false;

        String maxReconsumeTimesStr = data.get(CM_KEY_MAX_RECONSUME_TIMES);
        String retryIntervalsStr = data.get(CM_KEY_RETRY_INTERVALS);
        if (maxReconsumeTimesStr != null) {
            try {
                int maxReconsumeTimes = Integer.parseInt(maxReconsumeTimesStr.trim());
                long[] retryIntervals = parseRetryIntervals(retryIntervalsStr);
                refreshRetryPolicy(maxReconsumeTimes, retryIntervals);
                changed = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid maxReconsumeTimes: {}", maxReconsumeTimesStr);
            }
        }

        String consumerThreadMinStr = data.get(CM_KEY_CONSUMER_THREAD_MIN);
        String consumerThreadMaxStr = data.get(CM_KEY_CONSUMER_THREAD_MAX);
        if (consumerThreadMinStr != null && consumerThreadMaxStr != null) {
            try {
                int min = Integer.parseInt(consumerThreadMinStr.trim());
                int max = Integer.parseInt(consumerThreadMaxStr.trim());
                refreshConsumerThreads(min, max);
                changed = true;
            } catch (NumberFormatException e) {
                log.warn(
                        "Invalid consumerThreadMin/Max: {}/{}",
                        consumerThreadMinStr,
                        consumerThreadMaxStr);
            }
        }

        String retryScanMsStr = data.get(CM_KEY_RETRY_SCAN_MS);
        String delayScanMsStr = data.get(CM_KEY_DELAY_SCAN_MS);
        if (retryScanMsStr != null || delayScanMsStr != null) {
            try {
                long retryScanMs =
                        retryScanMsStr != null
                                ? Long.parseLong(retryScanMsStr.trim())
                                : io.github.streammq.core.StreamMQConstants
                                        .DEFAULT_PEL_CLAIM_SCAN_INTERVAL_MS;
                long delayScanMs =
                        delayScanMsStr != null
                                ? Long.parseLong(delayScanMsStr.trim())
                                : io.github.streammq.core.StreamMQConstants
                                        .DEFAULT_SCAN_INTERVAL_MS;
                refreshScanInterval(retryScanMs, delayScanMs);
                changed = true;
            } catch (NumberFormatException e) {
                log.warn("Invalid retryScanMs/delayScanMs: {}/{}", retryScanMsStr, delayScanMsStr);
            }
        }

        return changed;
    }

    private long[] parseRetryIntervals(String intervalsStr) {
        if (intervalsStr == null || intervalsStr.trim().isEmpty()) {
            return new long[0];
        }
        String[] parts = intervalsStr.split(",");
        long[] intervals = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            intervals[i] = Long.parseLong(parts[i].trim());
        }
        return intervals;
    }

    @Override
    public void refreshRetryPolicy(int maxReconsumeTimes, long[] retryIntervals) {
        log.info(
                "Refreshing retry policy: maxReconsumeTimes={}, intervals.length={}",
                maxReconsumeTimes,
                retryIntervals.length);
        if (customRefresher != null) {
            customRefresher.refreshRetryPolicy(maxReconsumeTimes, retryIntervals);
        }
    }

    @Override
    public void refreshConsumerThreads(int min, int max) {
        log.info("Refreshing consumer threads: min={}, max={}", min, max);
        if (customRefresher != null) {
            customRefresher.refreshConsumerThreads(min, max);
        }
    }

    @Override
    public void refreshScanInterval(long retryScanMs, long delayScanMs) {
        log.info(
                "Refreshing scan intervals: retryScanMs={}, delayScanMs={}",
                retryScanMs,
                delayScanMs);
        if (customRefresher != null) {
            customRefresher.refreshScanInterval(retryScanMs, delayScanMs);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping ConfigMapConfigRefresher...");
        if (syncFuture != null) {
            syncFuture.cancel(false);
        }
        if (syncScheduler != null) {
            syncScheduler.shutdown();
            try {
                if (!syncScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    syncScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                syncScheduler.shutdownNow();
            }
        }
        if (informerFactory != null) {
            informerFactory.stopAllRegisteredInformers();
        }
        informers.clear();
        processedVersions.clear();
        log.info("ConfigMapConfigRefresher stopped");
    }

    @Override
    public void close() {
        stop();
    }

    private void addDefaultWatchConfig(List<String> namespaces) {
        if (namespaces != null && !namespaces.isEmpty()) {
            for (String ns : namespaces) {
                var cfg = new ConfigMapWatchConfig();
                cfg.setNamespace(ns);
                cfg.setName(DEFAULT_WATCH_CONFIG_MAP_NAME);
                cfg.setRefreshIntervalMs(DEFAULT_WATCH_REFRESH_INTERVAL_MS);
                cfg.setEnabled(true);
                watchConfigs.add(cfg);
            }
            return;
        }
        var defaultConfig = new ConfigMapWatchConfig();
        defaultConfig.setNamespace(DEFAULT_WATCH_NAMESPACE);
        defaultConfig.setName(DEFAULT_WATCH_CONFIG_MAP_NAME);
        defaultConfig.setRefreshIntervalMs(DEFAULT_WATCH_REFRESH_INTERVAL_MS);
        defaultConfig.setEnabled(true);
        watchConfigs.add(defaultConfig);
    }

    public void addWatchConfig(ConfigMapWatchConfig config) {
        if (config != null) {
            watchConfigs.add(config);
        }
    }

    public List<ConfigMapWatchConfig> getWatchConfigs() {
        return new ArrayList<>(watchConfigs);
    }

    /** ConfigMap watch configuration. */
    public static class ConfigMapWatchConfig {
        private String namespace = DEFAULT_WATCH_NAMESPACE;
        private String name = DEFAULT_WATCH_CONFIG_MAP_NAME;
        private long refreshIntervalMs = DEFAULT_WATCH_REFRESH_INTERVAL_MS;
        private boolean enabled = true;

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getRefreshIntervalMs() {
            return refreshIntervalMs;
        }

        public void setRefreshIntervalMs(long refreshIntervalMs) {
            this.refreshIntervalMs = refreshIntervalMs;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    // ==================== SmartLifecycle ====================

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
