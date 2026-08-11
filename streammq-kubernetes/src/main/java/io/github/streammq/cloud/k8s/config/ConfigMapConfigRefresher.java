package io.github.streammq.cloud.k8s.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
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
 * @since 2.0.0
 */
@Slf4j
public class ConfigMapConfigRefresher implements StreamMQConfigRefresher, Runnable, AutoCloseable {

  @Autowired(required = false)
  private KubernetesClient kubernetesClient;

  @Autowired(required = false)
  private StreamMQConfigRefresher customRefresher;

  private SharedInformerFactory informerFactory;

  private final List<ConfigMapWatchConfig> watchConfigs = new ArrayList<>();

  private final Map<String, SharedIndexInformer<ConfigMap>> informers = new ConcurrentHashMap<>();

  private final Map<String, String> processedVersions = new ConcurrentHashMap<>();

  private final AtomicBoolean running = new AtomicBoolean(false);

  private ScheduledExecutorService syncScheduler;

  private ScheduledFuture<?> syncFuture;

  private static final String STREAMMQ_CONFIG_LABEL = "streammq.io/config";

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
      addDefaultWatchConfig();
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
              Thread t = new Thread(r, "streammq-configmap-sync");
              t.setDaemon(true);
              return t;
            });
    syncFuture = syncScheduler.scheduleAtFixedRate(this, 30, 30, TimeUnit.SECONDS);
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
        informerFactory.sharedIndexInformerFor(ConfigMap.class, watchConfig.getRefreshIntervalMs());

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

    String maxReconsumeTimesStr = data.get("maxReconsumeTimes");
    String retryIntervalsStr = data.get("retryIntervals");
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

    String consumerThreadMinStr = data.get("consumerThreadMin");
    String consumerThreadMaxStr = data.get("consumerThreadMax");
    if (consumerThreadMinStr != null && consumerThreadMaxStr != null) {
      try {
        int min = Integer.parseInt(consumerThreadMinStr.trim());
        int max = Integer.parseInt(consumerThreadMaxStr.trim());
        refreshConsumerThreads(min, max);
        changed = true;
      } catch (NumberFormatException e) {
        log.warn(
            "Invalid consumerThreadMin/Max: {}/{}", consumerThreadMinStr, consumerThreadMaxStr);
      }
    }

    String retryScanMsStr = data.get("retryScanMs");
    String delayScanMsStr = data.get("delayScanMs");
    if (retryScanMsStr != null || delayScanMsStr != null) {
      try {
        long retryScanMs = retryScanMsStr != null ? Long.parseLong(retryScanMsStr.trim()) : 5000L;
        long delayScanMs = delayScanMsStr != null ? Long.parseLong(delayScanMsStr.trim()) : 1000L;
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
    log.info("Refreshing scan intervals: retryScanMs={}, delayScanMs={}", retryScanMs, delayScanMs);
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

  private void addDefaultWatchConfig() {
    var defaultConfig = new ConfigMapWatchConfig();
    defaultConfig.setNamespace("default");
    defaultConfig.setName("streammq-consumer-config");
    defaultConfig.setRefreshIntervalMs(5000);
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
    private String namespace = "default";
    private String name = "streammq-consumer-config";
    private long refreshIntervalMs = 5000;
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
}
