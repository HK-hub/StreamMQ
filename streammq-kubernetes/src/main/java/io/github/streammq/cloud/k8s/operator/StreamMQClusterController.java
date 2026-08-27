/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
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
import io.github.streammq.core.StreamMQConstants;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * @since 0.1.0
 */
@Slf4j
public class StreamMQClusterController
        implements InitializingBean, DisposableBean, Runnable, AutoCloseable {

    @Autowired(required = false)
    private KubernetesClient kubernetesClient;

    /**
     * 配置刷新器提供者（懒解析）。
     *
     * <p><b>为何使用 {@link ObjectProvider} 而非直接注入：</b>{@code configMapConfigRefresher} Bean
     * 的工厂方法在创建期会主动执行一次 {@code StreamMQConfigRefresher} 类型查找；若控制器在此处直接注入该类型， 控制器的依赖装配会强制提前创建 {@code
     * configMapConfigRefresher}， 而后者创建期的同类型查找又会撞上 「自身正在创建中」，形成 {@code configMapConfigRefresher ↔
     * streamMQClusterController} 启动循环。 延迟到调和阶段（lifecycle 运行期）再解析即可干净地打破循环。
     */
    @Autowired(required = false)
    private org.springframework.beans.factory.ObjectProvider<StreamMQConfigRefresher>
            configRefresherProvider;

    private SharedInformerFactory informerFactory;

    /** 已注册的 Cluster Informer（全命名空间模式为单元素；收敛模式按命名空间各一） */
    private final List<SharedIndexInformer<StreamMQCluster>> clusterInformers = new ArrayList<>();

    /** 是否监听全部命名空间（默认 true，需要 ClusterRole 级 RBAC） */
    private volatile boolean watchAllNamespaces = true;

    /** 收敛模式下的监听命名空间列表（仅当 {@code watchAllNamespaces=false} 时生效） */
    private volatile List<String> watchNamespaces = List.of();

    private final Map<String, ClusterContext> clusterContexts = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, StreamMQK8sDefaults.THREAD_OPERATOR_SCHEDULER);
                        t.setDaemon(true);
                        return t;
                    });

    private ScheduledFuture<?> reconcileFuture;

    private final AtomicBoolean running = new AtomicBoolean(false);

    /** 调和间隔（秒） */
    private long reconcileIntervalSeconds = StreamMQK8sDefaults.DEFAULT_RECONCILE_INTERVAL_SECONDS;

    /**
     * 设置调和间隔（秒）。
     *
     * @param seconds 间隔秒数，必须 &gt; 0
     */
    public void setReconcileIntervalSeconds(long seconds) {
        if (seconds > 0) {
            this.reconcileIntervalSeconds = seconds;
        }
    }

    /**
     * 设置是否监听全部命名空间。
     *
     * <p>默认 true（需要 ClusterRole 级 RBAC）；设为 false 时改用 {@link #setWatchNamespaces(List)} 提供的命名空间列表。
     *
     * @param watchAllNamespaces true 表示全命名空间监听
     */
    public void setWatchAllNamespaces(boolean watchAllNamespaces) {
        this.watchAllNamespaces = watchAllNamespaces;
    }

    /**
     * 设置收敛模式的监听命名空间列表。
     *
     * @param namespaces 命名空间列表；为空时收敛模式退化为不监听任何资源
     */
    public void setWatchNamespaces(List<String> namespaces) {
        this.watchNamespaces = namespaces == null ? List.of() : List.copyOf(namespaces);
    }

    @Override
    public void afterPropertiesSet() {
        start();
    }

    @Override
    public void destroy() {
        stop();
    }

    @SuppressWarnings("deprecation")
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
        var eventHandler = buildEventHandler();

        if (watchAllNamespaces) {
            // 全命名空间模式（默认）：单 informer，部署需 ClusterRole watch 权限
            SharedIndexInformer<StreamMQCluster> informer =
                    informerFactory.sharedIndexInformerFor(
                            StreamMQCluster.class,
                            StreamMQK8sDefaults.DEFAULT_RESYNC_PERIOD_MILLIS);
            informer.addEventHandler(eventHandler);
            clusterInformers.add(informer);
        } else {
            // 收敛模式：按配置的命名空间逐个注册 informer，RBAC 只需对应命名空间权限
            for (String ns : watchNamespaces) {
                SharedIndexInformer<StreamMQCluster> informer =
                        informerFactory
                                .inNamespace(ns)
                                .sharedIndexInformerFor(
                                        StreamMQCluster.class,
                                        StreamMQK8sDefaults.DEFAULT_RESYNC_PERIOD_MILLIS);
                informer.addEventHandler(eventHandler);
                clusterInformers.add(informer);
            }
            log.info("StreamMQClusterController watching namespaces: {}", watchNamespaces);
        }

        informerFactory.startAllRegisteredInformers();
        reconcileFuture =
                scheduler.scheduleAtFixedRate(
                        this, reconcileIntervalSeconds, reconcileIntervalSeconds, TimeUnit.SECONDS);
        log.info(
                "StreamMQClusterController started (reconcile interval: {}s)",
                reconcileIntervalSeconds);
    }

    /** 构建共享的 CR 事件处理器（ADD/UPDATE 触发调和，DELETE 清理上下文）。 */
    private ResourceEventHandler<StreamMQCluster> buildEventHandler() {
        return new ResourceEventHandler<StreamMQCluster>() {
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
            public void onDelete(StreamMQCluster cluster, boolean deletedFinalStateUnknown) {
                log.info(
                        "StreamMQCluster deleted: {}/{}",
                        cluster.getMetadata().getNamespace(),
                        cluster.getMetadata().getName());
                String key = clusterKey(cluster);
                clusterContexts.remove(key);
            }
        };
    }

    @Override
    public void run() {
        try {
            if (clusterInformers.isEmpty()) {
                return;
            }
            for (SharedIndexInformer<StreamMQCluster> informer : clusterInformers) {
                var clusters = informer.getIndexer().list();
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
            updateStatus(cluster, StreamMQK8sDefaults.PHASE_FAILED, "Spec is null");
            return;
        }
        // 镜像必填校验：内置默认镜像名不可拉取，缺省时直接 Failed 并给出可操作提示
        if (spec.getImage() == null || spec.getImage().isBlank()) {
            updateStatus(
                    cluster,
                    StreamMQK8sDefaults.PHASE_FAILED,
                    "spec.image is required (e.g. your-registry/streammq-consumer:1.0.0)");
            return;
        }

        int replicas =
                spec.getReplicas() != null
                        ? spec.getReplicas()
                        : StreamMQK8sDefaults.DEFAULT_REPLICAS;

        try {
            ensureDeployment(cluster, ns, name, replicas, spec);
        } catch (Exception e) {
            log.error("Failed to ensure Deployment for {}/{}: {}", ns, name, e.getMessage(), e);
            updateStatus(cluster, StreamMQK8sDefaults.PHASE_FAILED, e.getMessage());
            return;
        }

        refreshConfigIfNeeded(cluster, spec);

        // phase 由 Deployment 实际就绪副本数推导（对齐 CRD enum），而非无条件宣称 Running
        updateStatusWithReadiness(cluster, ns, name, replicas);
        clusterContexts.put(key, new ClusterContext(cluster, System.currentTimeMillis()));
    }

    /** 读取 Deployment readyReplicas 推导 phase：Pending / NotReady / Ready。 */
    private void updateStatusWithReadiness(
            StreamMQCluster cluster, String ns, String name, int desiredReplicas) {
        var deploy = kubernetesClient.apps().deployments().inNamespace(ns).withName(name).get();
        String phase;
        String message = null;
        if (deploy == null || deploy.getStatus() == null) {
            phase = StreamMQK8sDefaults.PHASE_PENDING;
        } else {
            Integer ready =
                    deploy.getStatus().getReadyReplicas() != null
                            ? deploy.getStatus().getReadyReplicas()
                            : 0;
            status_readyReplicas.set(ready);
            if (ready >= desiredReplicas && desiredReplicas > 0) {
                phase = StreamMQK8sDefaults.PHASE_READY;
            } else if (ready > 0) {
                phase = StreamMQK8sDefaults.PHASE_UPDATING;
                message = "ready=" + ready + ", desired=" + desiredReplicas;
            } else {
                phase = StreamMQK8sDefaults.PHASE_NOT_READY;
            }
        }
        updateStatus(cluster, phase, message);
    }

    /** 最近一次 reconcile 观测到的就绪副本数（写入 status.readyReplicas）。 */
    private final java.util.concurrent.atomic.AtomicInteger status_readyReplicas =
            new java.util.concurrent.atomic.AtomicInteger(0);

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

            // 镜像 / 环境变量漂移检测：CR 为唯一事实来源，存量 Deployment 与 Spec 不一致时收敛
            updateContainerImageAndEnv(existingDeploy, ns, name, spec);
        }
    }

    /**
     * 对比存量 Deployment 首个容器与 CR Spec 的镜像及环境变量，存在漂移时以客户端 edit+patch 方式收敛。
     *
     * <p>此前仅处理副本数 / 标签 / 资源，用户修改 CR 的 {@code spec.image} 或 backend 配置后， 存量 Deployment
     * 不会更新，表现为「改了配置不生效」。
     */
    private void updateContainerImageAndEnv(
            Deployment deployment, String ns, String name, StreamMQCluster.Spec spec) {
        var containers = deployment.getSpec().getTemplate().getSpec().getContainers();
        if (containers.isEmpty()) {
            return;
        }
        var container = containers.get(0);
        boolean imageDrift =
                spec.getImage() != null && !spec.getImage().equals(container.getImage());
        List<io.fabric8.kubernetes.api.model.EnvVar> desiredEnv = collectEnvVars(name, ns, spec);
        boolean envDrift = !desiredEnv.equals(container.getEnv());
        if (!imageDrift && !envDrift) {
            return;
        }
        log.info(
                "Detected container drift for {}/{} (imageDrift={}, envDrift={}), patching",
                ns,
                name,
                imageDrift,
                envDrift);
        var updated =
                new DeploymentBuilder(deployment)
                        .editSpec()
                        .editTemplate()
                        .editSpec()
                        .editContainer(0)
                        .withImage(imageDrift ? spec.getImage() : container.getImage())
                        .withEnv(envDrift ? desiredEnv : container.getEnv())
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec()
                        .build();
        kubernetesClient.apps().deployments().inNamespace(ns).withName(name).patch(updated);
        log.info("Updated container image/env for {}/{}", ns, name);
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
                .withName(StreamMQK8sDefaults.CONTAINER_NAME)
                .withImage(spec.getImage())
                .withImagePullPolicy(StreamMQK8sDefaults.PULL_POLICY_IF_NOT_PRESENT)
                .withEnv(envVars)
                .withPorts(buildPortList())
                .withResources(buildResources(spec.getResources()))
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    private List<io.fabric8.kubernetes.api.model.EnvVar> collectEnvVars(
            String name, String ns, StreamMQCluster.Spec spec) {
        var vars = new ArrayList<io.fabric8.kubernetes.api.model.EnvVar>();
        var b =
                new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                        .withName(StreamMQK8sDefaults.ENV_CLUSTER_NAME)
                        .withValue(name);
        vars.add(b.build());
        vars.add(
                new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                        .withName(StreamMQK8sDefaults.ENV_NAMESPACE)
                        .withValue(ns)
                        .build());

        if (spec.getBackend() != null && spec.getBackend().getRedis() != null) {
            var redis = spec.getBackend().getRedis();
            vars.add(
                    new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                            .withName(StreamMQK8sDefaults.ENV_REDIS_ADDRESS)
                            .withValue(redis.getAddress() != null ? redis.getAddress() : "")
                            .build());
            vars.add(
                    new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                            .withName(StreamMQK8sDefaults.ENV_REDIS_NAMESPACE)
                            .withValue(redis.getNamespace() != null ? redis.getNamespace() : "")
                            .build());
            if (redis.getPasswordSecretRef() != null) {
                // 推荐：从 Secret 注入密码，CR 与 Deployment 规格中不出现明文
                var ref = redis.getPasswordSecretRef();
                io.fabric8.kubernetes.api.model.EnvVarSource source =
                        new io.fabric8.kubernetes.api.model.EnvVarSourceBuilder()
                                .withNewSecretKeyRef()
                                .withName(ref.getName())
                                .withKey(ref.getKey())
                                .endSecretKeyRef()
                                .build();
                vars.add(
                        new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                                .withName(StreamMQK8sDefaults.ENV_REDIS_PASSWORD)
                                .withValueFrom(source)
                                .build());
            } else if (redis.getPassword() != null) {
                log.warn(
                        "Redis inline password is deprecated for cluster {}/{};"
                                + " use spec.backend.redis.passwordSecretRef",
                        ns,
                        name);
                vars.add(
                        new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                                .withName(StreamMQK8sDefaults.ENV_REDIS_PASSWORD)
                                .withValue(redis.getPassword())
                                .build());
            }
        }

        if (spec.getConfig() != null && spec.getConfig().getTracing() != null) {
            var tracing = spec.getConfig().getTracing();
            vars.add(
                    new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                            .withName(StreamMQK8sDefaults.ENV_TRACING_ENABLED)
                            .withValue(String.valueOf(tracing.getEnabled()))
                            .build());
            vars.add(
                    new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                            .withName(StreamMQK8sDefaults.ENV_TRACING_EXPORTER)
                            .withValue(tracing.getExporter() != null ? tracing.getExporter() : "")
                            .build());
            vars.add(
                    new io.fabric8.kubernetes.api.model.EnvVarBuilder()
                            .withName(StreamMQK8sDefaults.ENV_TRACING_ENDPOINT)
                            .withValue(tracing.getEndpoint() != null ? tracing.getEndpoint() : "")
                            .build());
        }

        return vars;
    }

    private List<io.fabric8.kubernetes.api.model.ContainerPort> buildPortList() {
        return List.of(
                new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
                        .withContainerPort(StreamMQK8sDefaults.PORT_HTTP)
                        .withName(StreamMQK8sDefaults.PORT_NAME_HTTP)
                        .build(),
                new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
                        .withContainerPort(StreamMQK8sDefaults.PORT_METRICS)
                        .withName(StreamMQK8sDefaults.PORT_NAME_METRICS)
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
                StreamMQK8sDefaults.LABEL_APP,
                name,
                StreamMQK8sDefaults.LABEL_APP_K8S_NAME,
                StreamMQK8sDefaults.LABEL_VALUE_APP_NAME,
                StreamMQK8sDefaults.LABEL_APP_K8S_COMPONENT,
                StreamMQK8sDefaults.LABEL_VALUE_COMPONENT_CONSUMER,
                StreamMQK8sDefaults.LABEL_APP_K8S_MANAGED_BY,
                StreamMQK8sDefaults.LABEL_VALUE_MANAGED_BY);
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
        StreamMQConfigRefresher configRefresher =
                Objects.isNull(configRefresherProvider)
                        ? null
                        : configRefresherProvider.getIfAvailable();
        if (configRefresher == null || spec.getConfig() == null) {
            return;
        }
        var config = spec.getConfig();
        if (config.getRetry() != null) {
            var retry = config.getRetry();
            int maxTimes =
                    retry.getMaxReconsumeTimes() != null
                            ? retry.getMaxReconsumeTimes()
                            : StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;
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
            long scanMs =
                    delay.getScanIntervalMs() != null
                            ? delay.getScanIntervalMs()
                            : StreamMQConstants.DEFAULT_SCAN_INTERVAL_MS;
            configRefresher.refreshScanInterval(scanMs, scanMs);
        }
    }

    @SuppressWarnings("deprecation")
    private void updateStatus(StreamMQCluster cluster, String phase, String message) {
        // 不修改 informer 缓存的共享对象：构造仅含元数据的副本写入状态，
        // 避免污染本地缓存导致后续 reconcile 基于脏数据决策
        StreamMQCluster shell = new StreamMQCluster();
        shell.setApiVersion(cluster.getApiVersion());
        shell.setKind(cluster.getKind());
        shell.setMetadata(cluster.getMetadata());

        var status = new StreamMQCluster.Status();
        status.setPhase(phase);
        status.setLastUpdateTime(Instant.now().toString());
        status.setReadyReplicas(status_readyReplicas.get());
        if (cluster.getSpec() != null) {
            status.setReplicas(
                    cluster.getSpec().getReplicas() != null
                            ? cluster.getSpec().getReplicas()
                            : StreamMQK8sDefaults.DEFAULT_REPLICAS);
        }
        if (message != null) {
            status.setMessage(message);
        }
        shell.setStatus(status);

        try {
            kubernetesClient
                    .resources(StreamMQCluster.class)
                    .inNamespace(cluster.getMetadata().getNamespace())
                    .withName(cluster.getMetadata().getName())
                    .updateStatus(shell);
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
            if (!scheduler.awaitTermination(
                    StreamMQK8sDefaults.OPERATOR_AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
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
