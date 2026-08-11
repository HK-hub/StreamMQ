package io.github.streammq.cloud.k8s.operator;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.github.streammq.cloud.k8s.StreamMQConfigRefresher;
import java.time.Instant;
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
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * StreamMQCluster CRD Operator controller.
 *
 * <p>Core responsibilities:
 *
 * <ul>
 *   <li>Watch {@code StreamMQCluster} CRD create/update/delete events via {@link
 *       SharedIndexInformer}
 *   <li>Reconcile desired state (Spec) with actual state using level-based reconciliation
 *   <li>Manage replica count, resource limits, config hot-reload, auto-scaling policy
 *   <li>Maintain cluster Status: phase, replicas, readyReplicas, conditions
 *   <li>Create and manage child Deployment resources with OwnerReference
 * </ul>
 *
 * <p>Reconciliation loop:
 *
 * <ol>
 *   <li>On ADD/UPDATE: compute desired Deployment, create or patch
 *   <li>On DELETE: garbage-collect child resources via OwnerReference cascade
 *   <li>Periodic full-sync: re-reconcile all clusters to handle missed events
 * </ol>
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
@Slf4j
public class StreamMQClusterController
        implements InitializingBean, DisposableBean, Runnable, AutoCloseable {

    @Autowired private KubernetesClient kubernetesClient;

    @Autowired(required = false)
    private StreamMQConfigRefresher configRefresher;

    private SharedInformerFactory informerFactory;

    private SharedIndexInformer<StreamMQCluster> clusterInformer;

    private final Map<String, ClusterContext> clusterContexts = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "streammq-operator-scheduler");
                        t.setDaemon(true);
                        return t;
                    });

    private ScheduledFuture<?> reconcileFuture;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private long reconcileIntervalSeconds = 30;

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
            log.warn("StreamMQClusterController already started");
            return;
        }
        if (kubernetesClient == null) {
            log.warn("KubernetesClient not available, StreamMQClusterController disabled");
            return;
        }
        log.info("Starting StreamMQClusterController...");

        informerFactory = kubernetesClient.informers();
        clusterInformer = informerFactory.sharedIndexInformerFor(StreamMQCluster.class, 30 * 1000L);

        clusterInformer.addEventHandler(
                new ResourceEventHandler<StreamMQCluster>() {
                    @Override
                    public void onAdd(StreamMQCluster cluster) {
                        log.info(
                                "StreamMQCluster added: {}/{}",
                                cluster.getMetadata().getNamespace(),
                                cluster.getMetadata().getName());
                        reconcile(cluster);
                    }

                    @Override
                    public void onUpdate(StreamMQCluster oldCluster, StreamMQCluster newCluster) {
                        log.info(
                                "StreamMQCluster updated: {}/{}",
                                newCluster.getMetadata().getNamespace(),
                                newCluster.getMetadata().getName());
                        reconcile(newCluster);
                    }

                    @Override
                    public void onDelete(
                            StreamMQCluster cluster, boolean deletedFinalStateUnknown) {
                        log.info(
                                "StreamMQCluster deleted: {}/{}",
                                cluster.getMetadata().getNamespace(),
                                cluster.getMetadata().getName());
                        String key = clusterKey(cluster);
                        clusterContexts.remove(key);
                    }
                });

        informerFactory.startAllRegisteredInformers();
        reconcileFuture =
                scheduler.scheduleAtFixedRate(
                        this, reconcileIntervalSeconds, reconcileIntervalSeconds, TimeUnit.SECONDS);
        log.info(
                "StreamMQClusterController started (reconcile interval: {}s)",
                reconcileIntervalSeconds);
    }

    @Override
    public void run() {
        try {
            if (clusterInformer == null) {
                return;
            }
            var clusters = clusterInformer.getIndexer().list();
            for (var cluster : clusters) {
                try {
                    reconcile(cluster);
                } catch (Exception e) {
                    log.error(
                            "Failed to reconcile cluster {}/{}: {}",
                            cluster.getMetadata().getNamespace(),
                            cluster.getMetadata().getName(),
                            e.getMessage(),
                            e);
                }
            }
        } catch (Exception e) {
            log.error("Reconciliation loop failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Reconcile a single StreamMQCluster resource: compute desired state, update status,
     * create/patch child Deployment.
     */
    private void reconcile(StreamMQCluster cluster) {
        String ns = cluster.getMetadata().getNamespace();
        String name = cluster.getMetadata().getName();
        String key = clusterKey(cluster);

        var spec = cluster.getSpec();
        if (spec == null) {
            updateStatus(cluster, "Failed", "Spec is null");
            return;
        }

        int replicas = spec.getReplicas() != null ? spec.getReplicas() : 3;

        ensureDeployment(cluster, ns, name, replicas, spec);

        refreshConfigIfNeeded(cluster, spec);

        updateStatus(cluster, "Running", null);
        clusterContexts.put(key, new ClusterContext(cluster, System.currentTimeMillis()));
    }

    @SuppressWarnings("deprecation")
    private void ensureDeployment(
            StreamMQCluster cluster,
            String ns,
            String name,
            int replicas,
            StreamMQCluster.Spec spec) {
        var existingDeploy =
                kubernetesClient.apps().deployments().inNamespace(ns).withName(name).get();

        if (existingDeploy == null) {
            Deployment deployment = buildDeployment(cluster, ns, name, replicas, spec);
            kubernetesClient.apps().deployments().inNamespace(ns).create(deployment);
            log.info("Created Deployment {}/{} with {} replicas", ns, name, replicas);
        } else {
            int currentReplicas = existingDeploy.getSpec().getReplicas();
            if (currentReplicas != replicas) {
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(ns)
                        .withName(name)
                        .scale(replicas);
                log.info(
                        "Scaled Deployment {}/{} from {} to {} replicas",
                        ns,
                        name,
                        currentReplicas,
                        replicas);
            }

            var currentLabels = existingDeploy.getSpec().getTemplate().getMetadata().getLabels();
            var desiredLabels = buildLabels(name);
            if (!desiredLabels.equals(currentLabels)) {
                var updated =
                        new DeploymentBuilder(existingDeploy)
                                .editSpec()
                                .editTemplate()
                                .editMetadata()
                                .withLabels(desiredLabels)
                                .endMetadata()
                                .endTemplate()
                                .endSpec()
                                .build();
                kubernetesClient.apps().deployments().inNamespace(ns).withName(name).patch(updated);
                log.info("Updated Deployment labels for {}/{}", ns, name);
            }

            if (spec.getResources() != null) {
                updateContainerResources(existingDeploy, ns, name, spec.getResources());
            }
        }
    }

    private Deployment buildDeployment(
            StreamMQCluster cluster,
            String ns,
            String name,
            int replicas,
            StreamMQCluster.Spec spec) {
        OwnerReference ownerRef =
                new OwnerReferenceBuilder()
                        .withApiVersion(cluster.getApiVersion())
                        .withKind(cluster.getKind())
                        .withName(cluster.getMetadata().getName())
                        .withUid(cluster.getMetadata().getUid())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build();

        var envVars = collectEnvVars(name, ns, spec);

        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(ns)
                .withLabels(buildLabels(name))
                .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewSelector()
                .addToMatchLabels("app", name)
                .endSelector()
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(buildLabels(name))
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName("streammq-consumer")
                .withImage("streammq/streammq-consumer:latest")
                .withImagePullPolicy("IfNotPresent")
                .withEnv(buildEnvVarList(envVars))
                .withPorts(buildPortList())
                .withResources(buildResources(spec.getResources()))
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    private List<EnvVar> collectEnvVars(String name, String ns, StreamMQCluster.Spec spec) {
        var vars = new ArrayList<EnvVar>();
        vars.add(new EnvVar("STREAMMQ_CLUSTER_NAME", name));
        vars.add(new EnvVar("STREAMMQ_NAMESPACE", ns));

        if (spec.getBackend() != null && spec.getBackend().getRedis() != null) {
            var redis = spec.getBackend().getRedis();
            vars.add(
                    new EnvVar(
                            "STREAMMQ_REDIS_ADDRESS",
                            redis.getAddress() != null ? redis.getAddress() : ""));
            vars.add(
                    new EnvVar(
                            "STREAMMQ_REDIS_NAMESPACE",
                            redis.getNamespace() != null ? redis.getNamespace() : ""));
            if (redis.getPassword() != null) {
                vars.add(new EnvVar("STREAMMQ_REDIS_PASSWORD", redis.getPassword()));
            }
        }

        if (spec.getConfig() != null && spec.getConfig().getTracing() != null) {
            var tracing = spec.getConfig().getTracing();
            vars.add(new EnvVar("STREAMMQ_TRACING_ENABLED", String.valueOf(tracing.getEnabled())));
            vars.add(
                    new EnvVar(
                            "STREAMMQ_TRACING_EXPORTER",
                            tracing.getExporter() != null ? tracing.getExporter() : ""));
            vars.add(
                    new EnvVar(
                            "STREAMMQ_TRACING_ENDPOINT",
                            tracing.getEndpoint() != null ? tracing.getEndpoint() : ""));
        }

        return vars;
    }

    private record EnvVar(String name, String value) {}

    private List<io.fabric8.kubernetes.api.model.EnvVar> buildEnvVarList(List<EnvVar> envVars) {
        return envVars.stream()
                .map(
                        e ->
                                new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                                        .withName(e.name())
                                        .withValue(e.value())
                                        .build())
                .toList();
    }

    private List<io.fabric8.kubernetes.api.model.ContainerPort> buildPortList() {
        return List.of(
                new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
                        .withContainerPort(8080)
                        .withName("http")
                        .build(),
                new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
                        .withContainerPort(9090)
                        .withName("metrics")
                        .build());
    }

    private io.fabric8.kubernetes.api.model.ResourceRequirements buildResources(
            StreamMQCluster.Resources resources) {
        if (resources == null) {
            return new io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder().build();
        }
        return new io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder()
                .addToRequests(
                        "cpu",
                        new io.fabric8.kubernetes.api.model.Quantity(resources.getCpuRequest()))
                .addToRequests(
                        "memory",
                        new io.fabric8.kubernetes.api.model.Quantity(resources.getMemoryRequest()))
                .addToLimits(
                        "cpu",
                        new io.fabric8.kubernetes.api.model.Quantity(resources.getCpuLimit()))
                .addToLimits(
                        "memory",
                        new io.fabric8.kubernetes.api.model.Quantity(resources.getMemoryLimit()))
                .build();
    }

    private Map<String, String> buildLabels(String name) {
        return Map.of(
                "app", name,
                "app.kubernetes.io/name", "streammq",
                "app.kubernetes.io/component", "consumer",
                "app.kubernetes.io/managed-by", "streammq-operator");
    }

    private void updateContainerResources(
            Deployment deployment, String ns, String name, StreamMQCluster.Resources resources) {
        var containers = deployment.getSpec().getTemplate().getSpec().getContainers();
        if (containers.isEmpty()) {
            return;
        }
        var existingResources = containers.get(0).getResources();
        var cpuRequest = new io.fabric8.kubernetes.api.model.Quantity(resources.getCpuRequest());
        var memoryRequest =
                new io.fabric8.kubernetes.api.model.Quantity(resources.getMemoryRequest());
        var cpuLimit = new io.fabric8.kubernetes.api.model.Quantity(resources.getCpuLimit());
        var memoryLimit = new io.fabric8.kubernetes.api.model.Quantity(resources.getMemoryLimit());
        if (existingResources == null
                || !cpuRequest.equals(existingResources.getRequests().get("cpu"))
                || !memoryRequest.equals(existingResources.getRequests().get("memory"))
                || !cpuLimit.equals(existingResources.getLimits().get("cpu"))
                || !memoryLimit.equals(existingResources.getLimits().get("memory"))) {
            var updated =
                    new DeploymentBuilder(deployment)
                            .editSpec()
                            .editTemplate()
                            .editSpec()
                            .editContainer(0)
                            .withNewResources()
                            .addToRequests("cpu", cpuRequest)
                            .addToRequests("memory", memoryRequest)
                            .addToLimits("cpu", cpuLimit)
                            .addToLimits("memory", memoryLimit)
                            .endResources()
                            .endContainer()
                            .endSpec()
                            .endTemplate()
                            .endSpec()
                            .build();
            kubernetesClient.apps().deployments().inNamespace(ns).withName(name).patch(updated);
            log.info("Updated container resources for {}/{}", ns, name);
        }
    }

    private void refreshConfigIfNeeded(StreamMQCluster cluster, StreamMQCluster.Spec spec) {
        if (configRefresher == null || spec.getConfig() == null) {
            return;
        }
        var config = spec.getConfig();
        if (config.getRetry() != null) {
            var retry = config.getRetry();
            int maxTimes = retry.getMaxReconsumeTimes() != null ? retry.getMaxReconsumeTimes() : 16;
            long[] intervals =
                    retry.getRetryIntervals() != null
                            ? java.util.Arrays.stream(retry.getRetryIntervals())
                                    .mapToLong(Long::longValue)
                                    .toArray()
                            : new long[0];
            configRefresher.refreshRetryPolicy(maxTimes, intervals);
        }
        if (config.getDelay() != null) {
            var delay = config.getDelay();
            long scanMs = delay.getScanIntervalMs() != null ? delay.getScanIntervalMs() : 1000L;
            configRefresher.refreshScanInterval(scanMs, scanMs);
        }
    }

    @SuppressWarnings("deprecation")
    private void updateStatus(StreamMQCluster cluster, String phase, String message) {
        var status = cluster.getStatus();
        if (status == null) {
            status = new StreamMQCluster.Status();
            cluster.setStatus(status);
        }
        status.setPhase(phase);
        status.setLastUpdateTime(Instant.now().toString());

        if (cluster.getSpec() != null) {
            status.setReplicas(
                    cluster.getSpec().getReplicas() != null ? cluster.getSpec().getReplicas() : 3);
        }

        try {
            kubernetesClient
                    .resources(StreamMQCluster.class)
                    .inNamespace(cluster.getMetadata().getNamespace())
                    .withName(cluster.getMetadata().getName())
                    .updateStatus(cluster);
        } catch (Exception e) {
            log.warn(
                    "Failed to update status for {}/{}: {}",
                    cluster.getMetadata().getNamespace(),
                    cluster.getMetadata().getName(),
                    e.getMessage());
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping StreamMQClusterController...");
        if (reconcileFuture != null) {
            reconcileFuture.cancel(false);
        }
        if (informerFactory != null) {
            informerFactory.stopAllRegisteredInformers();
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
        clusterContexts.clear();
        log.info("StreamMQClusterController stopped");
    }

    @Override
    public void close() {
        stop();
    }

    private static String clusterKey(StreamMQCluster cluster) {
        return cluster.getMetadata().getNamespace() + "/" + cluster.getMetadata().getName();
    }

    /** Reconciliation context for a single cluster. */
    private static class ClusterContext {
        final StreamMQCluster cluster;
        final long lastReconcileTime;

        ClusterContext(StreamMQCluster cluster, long lastReconcileTime) {
            this.cluster = cluster;
            this.lastReconcileTime = lastReconcileTime;
        }
    }
}
