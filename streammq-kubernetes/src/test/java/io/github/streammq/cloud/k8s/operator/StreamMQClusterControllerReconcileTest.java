/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.operator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link StreamMQClusterController#reconcile(StreamMQCluster)} 调和契约测试（fabric8 Mock Server，
 * expectation 模式）。
 *
 * <p><b>锁定的红队发现：K4（状态无条件写入 → 自激死循环）与 K5（readyReplicas 使用实例级共享计数器 → 跨 CR 串值）</b>
 *
 * <p><b>为何使用 expectation 模式而非 CRUD 模式：</b>当前构建把 Jackson 统一升级到 2.21.4（父 pom 的 {@code
 * jackson.version}，且 {@code jackson-annotations} 由 BOM 映射到 2.21），而 fabric8 6.13.1 的 CRUD dispatcher
 * 依赖 {@code GenericKubernetesResource} 的 {@code additionalProperties} 序列化—— 在 2.21.4 下抛 {@code
 * JsonMappingException: ... because "keySerializer" is null}，CRUD 模式的 POST/PUT 全部 500。 本类改用
 * expectation 模式（响应体是测试侧构造的 {@code StreamMQCluster}/{@code Deployment} POJO，序列化正常）， 因此可以对「真实 HTTP
 * 请求」做断言：写了几次 status、写出的 JSON 字段是什么，而不是断言 mock 的调用次数。
 *
 * <p><b>失败即红说明：</b>
 *
 * <ul>
 *   <li>用例一（K4）：旧实现在每次调和末尾无条件 {@code updateStatus()}，且 {@code lastUpdateTime} 恒为新时间戳， 第二次调和必然再发一次
 *       {@code PUT .../status}——{@code statusWrites(second).isZero()} 立即红（informer onUpdate
 *       自激死循环的充要条件）；
 *   <li>用例二（K4）：旧实现无法区分「语义未变」与「语义已变」，整体断言链在旧代码上由用例一先红；
 *   <li>用例三/四（K5）：旧实现 Pending/Failed 分支不写 {@code readyReplicas}（字段保持 0 也依赖默认值），且不写 {@code
 *       observedGeneration}——{@code readyReplicas=0} 与 {@code observedGeneration=1} 断言会红；
 *   <li>用例五（K5）：旧实现用实例级共享计数器回填 readyReplicas，第二个 CR 会读到第一个 CR 的观测值（3 而非 1）， {@code readyReplicas=1}
 *       断言会红。
 * </ul>
 *
 * @author StreamMQ Contributors
 */
@DisplayName("StreamMQClusterController 调和契约测试（K4/K5）")
class StreamMQClusterControllerReconcileTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String NS_A = "ns-a";
    private static final String IMAGE = "registry.example.com/streammq-consumer:1.0.0";

    private KubernetesMockServer server;
    private KubernetesClient client;
    private StreamMQClusterController controller;

    @BeforeAll
    static void silenceMockServerLogs() {
        Logger.getLogger("okhttp3.mockwebserver.MockWebServer").setLevel(Level.WARNING);
    }

    @BeforeEach
    void setUp() {
        server = new KubernetesMockServer(false);
        server.init();
        client = server.createClient();
        controller = new StreamMQClusterController();
        controller.setKubernetesClient(client);
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.destroy();
    }

    @Test
    @DisplayName("K4 - 状态语义未变时第二次调和不得再次写 status；语义变化时必须写")
    void statusIsWrittenOnlyWhenSemanticsChange() throws Exception {
        String crPath = crPath(NS_A, "demo");
        // 第一次调和基于「status 为空」的快照；informer 随后投递的观察到写回后的对象
        StreamMQCluster initial = cluster(NS_A, "demo", 3);
        StreamMQCluster observed = cluster(NS_A, "demo", 3);
        observed.setStatus(status("Ready", 3, 3, 1L, "2026-01-01T00:00:00Z"));
        // 语义变化：readyReplicas 由 3 变 1（phase 随之 Updating）
        StreamMQCluster changed = cluster(NS_A, "demo", 3);
        changed.setStatus(status("Updating", 3, 1, 1L, "2026-01-01T00:00:00Z"));

        server.expect()
                .get()
                .withPath(deploymentPath(NS_A, "demo"))
                .andReturn(200, readyDeployment(NS_A, "demo", 3, 3))
                .always();
        server.expect().put().withPath(crPath + "/status").andReturn(200, observed).always();

        controller.reconcile(initial);
        List<Req> first = drainRequests();
        assertThat(statusWrites(first)).as("status 由空变为 Ready 时必须写").isEqualTo(1);
        JsonNode firstBody = statusBody(first);

        controller.reconcile(observed);
        List<Req> second = drainRequests();
        assertThat(statusWrites(second))
                .as("K4：语义未变时不得再写 status（否则 updateStatus→informer onUpdate→再调和形成死循环）")
                .isZero();

        controller.reconcile(changed);
        List<Req> third = drainRequests();
        assertThat(statusWrites(third)).as("语义变化时必须写").isEqualTo(1);
        JsonNode thirdBody = statusBody(third);

        // status 写入必须携带完整语义字段
        assertThat(firstBody.path("phase").asText()).isEqualTo(StreamMQK8sDefaults.PHASE_READY);
        assertThat(firstBody.path("replicas").asInt()).isEqualTo(3);
        assertThat(firstBody.path("readyReplicas").asInt()).isEqualTo(3);
        assertThat(firstBody.path("observedGeneration").asLong()).isEqualTo(1L);
        assertThat(firstBody.path("lastUpdateTime").asText()).isNotBlank();
        // 语义变化时 lastUpdateTime 必须刷新（旧实现每次刷新 → 恒写；新实现只在变化时刷新）
        assertThat(thirdBody.path("lastUpdateTime").asText())
                .isNotBlank()
                .isNotEqualTo(firstBody.path("lastUpdateTime").asText());
    }

    @Test
    @DisplayName("K5 - spec 为 null 时写 Failed 且 readyReplicas=0、observedGeneration 必填")
    void nullSpecWritesFailedWithZeroReadyReplicas() throws Exception {
        String crPath = crPath(NS_A, "demo");
        StreamMQCluster noSpec = new StreamMQCluster();
        // informer 投递的 CR 一定带 resourceVersion：缺失时 fabric8 的 updateStatus 会先 GET 再 PUT
        noSpec.setMetadata(
                new ObjectMetaBuilder()
                        .withName("demo")
                        .withNamespace(NS_A)
                        .withGeneration(1L)
                        .withResourceVersion("1")
                        .build());
        server.expect().put().withPath(crPath + "/status").andReturn(200, noSpec).always();

        controller.reconcile(noSpec);

        List<Req> requests = drainRequests();
        assertThat(statusWrites(requests)).isEqualTo(1);
        JsonNode body = statusBody(requests);
        assertThat(body.path("phase").asText()).isEqualTo(StreamMQK8sDefaults.PHASE_FAILED);
        assertThat(body.path("readyReplicas").asInt())
                .as("K5：spec=null 分支必须显式写 readyReplicas=0，否则保留其它 CR 的旧观测值")
                .isZero();
        assertThat(body.path("observedGeneration").asLong()).isEqualTo(1L);
        assertThat(body.path("message").asText()).isEqualTo("Spec is null");
    }

    @Test
    @DisplayName("K5 - spec.image 缺失时写 Failed 并给出可操作提示，readyReplicas=0")
    void missingImageWritesFailedWithZeroReadyReplicas() throws Exception {
        String crPath = crPath(NS_A, "demo");
        StreamMQCluster noImage = cluster(NS_A, "demo", 3);
        noImage.getSpec().setImage(null);
        server.expect().put().withPath(crPath + "/status").andReturn(200, noImage).always();

        controller.reconcile(noImage);

        List<Req> requests = drainRequests();
        assertThat(statusWrites(requests)).isEqualTo(1);
        JsonNode body = statusBody(requests);
        assertThat(body.path("phase").asText()).isEqualTo(StreamMQK8sDefaults.PHASE_FAILED);
        assertThat(body.path("message").asText()).contains("spec.image is required");
        assertThat(body.path("readyReplicas").asInt()).isZero();
        assertThat(body.path("observedGeneration").asLong()).isEqualTo(1L);
    }

    @Test
    @DisplayName("K5 - readyReplicas 来自各 CR 自己的 Deployment（多 CR 不串值）")
    void readyReplicasIsDerivedPerCluster() throws Exception {
        String pathA = crPath(NS_A, "cluster-a");
        String pathB = crPath(NS_A, "cluster-b");
        StreamMQCluster clusterA = cluster(NS_A, "cluster-a", 3);
        StreamMQCluster clusterB = cluster(NS_A, "cluster-b", 3);
        server.expect()
                .get()
                .withPath(deploymentPath(NS_A, "cluster-a"))
                .andReturn(200, readyDeployment(NS_A, "cluster-a", 3, 3))
                .always();
        server.expect()
                .get()
                .withPath(deploymentPath(NS_A, "cluster-b"))
                .andReturn(200, readyDeployment(NS_A, "cluster-b", 3, 1))
                .always();
        server.expect().put().withPath(pathA + "/status").andReturn(200, clusterA).always();
        server.expect().put().withPath(pathB + "/status").andReturn(200, clusterB).always();

        controller.reconcile(clusterA);
        JsonNode bodyA = statusBody(drainRequests());
        controller.reconcile(clusterB);
        JsonNode bodyB = statusBody(drainRequests());

        assertThat(bodyA.path("phase").asText()).isEqualTo(StreamMQK8sDefaults.PHASE_READY);
        assertThat(bodyA.path("readyReplicas").asInt()).isEqualTo(3);
        assertThat(bodyB.path("phase").asText()).isEqualTo(StreamMQK8sDefaults.PHASE_UPDATING);
        assertThat(bodyB.path("readyReplicas").asInt())
                .as("K5：cluster-b 的 readyReplicas 必须是 1，而不是 cluster-a 观测到的 3（共享计数器串值）")
                .isEqualTo(1);
        assertThat(bodyB.path("message").asText()).isEqualTo("ready=1, desired=3");
    }

    // ==================== fixtures ====================

    private static String crPath(String namespace, String name) {
        return "/apis/streammq.io/v1/namespaces/" + namespace + "/streammqclusters/" + name;
    }

    private static String deploymentPath(String namespace, String name) {
        return "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + name;
    }

    private static StreamMQCluster cluster(String namespace, String name, int replicas) {
        StreamMQCluster cluster = new StreamMQCluster();
        cluster.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withUid("uid-" + name)
                        .withGeneration(1L)
                        .withResourceVersion("1")
                        .build());
        StreamMQCluster.Spec spec = new StreamMQCluster.Spec();
        spec.setImage(IMAGE);
        spec.setReplicas(replicas);
        cluster.setSpec(spec);
        return cluster;
    }

    private static StreamMQCluster.Status status(
            String phase,
            int replicas,
            int readyReplicas,
            Long observedGeneration,
            String lastUpdateTime) {
        StreamMQCluster.Status status = new StreamMQCluster.Status();
        status.setPhase(phase);
        status.setReplicas(replicas);
        status.setReadyReplicas(readyReplicas);
        status.setObservedGeneration(observedGeneration);
        status.setLastUpdateTime(lastUpdateTime);
        return status;
    }

    /** 构造与控制器期望完全一致的 Deployment，避免 labels/env/image 漂移导致额外的 PATCH 请求。 */
    private static Deployment readyDeployment(
            String namespace, String name, int replicas, int readyReplicas) {
        Map<String, String> labels = expectedLabels(name);
        List<EnvVar> env =
                List.of(
                        new EnvVarBuilder()
                                .withName(StreamMQK8sDefaults.ENV_CLUSTER_NAME)
                                .withValue(name)
                                .build(),
                        new EnvVarBuilder()
                                .withName(StreamMQK8sDefaults.ENV_NAMESPACE)
                                .withValue(namespace)
                                .build());
        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(labels)
                .withResourceVersion("1")
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewSelector()
                .addToMatchLabels(StreamMQK8sDefaults.LABEL_APP, name)
                .endSelector()
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withContainers(
                        new ContainerBuilder()
                                .withName(StreamMQK8sDefaults.CONTAINER_NAME)
                                .withImage(IMAGE)
                                .withEnv(env)
                                .withPorts(
                                        new ContainerPortBuilder()
                                                .withContainerPort(StreamMQK8sDefaults.PORT_HTTP)
                                                .withName(StreamMQK8sDefaults.PORT_NAME_HTTP)
                                                .build(),
                                        new ContainerPortBuilder()
                                                .withContainerPort(StreamMQK8sDefaults.PORT_METRICS)
                                                .withName(StreamMQK8sDefaults.PORT_NAME_METRICS)
                                                .build())
                                .build())
                .endSpec()
                .endTemplate()
                .endSpec()
                .withStatus(
                        new DeploymentStatusBuilder()
                                .withReplicas(replicas)
                                .withReadyReplicas(readyReplicas)
                                .withAvailableReplicas(readyReplicas)
                                .build())
                .build();
    }

    private static Map<String, String> expectedLabels(String name) {
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

    // ==================== request recording ====================

    /** 记录到的 HTTP 请求（含响应体无关的请求行与请求体）。 */
    private record Req(String method, String path, String body) {}

    private List<Req> drainRequests() throws InterruptedException {
        List<Req> requests = new ArrayList<>();
        RecordedRequest request;
        while ((request = server.takeRequest(200, TimeUnit.MILLISECONDS)) != null) {
            requests.add(
                    new Req(request.getMethod(), request.getPath(), request.getBody().readUtf8()));
        }
        return requests;
    }

    private static long statusWrites(List<Req> requests) {
        return requests.stream()
                .filter(r -> "PUT".equals(r.method()) && r.path().endsWith("/status"))
                .count();
    }

    private static JsonNode statusBody(List<Req> requests) throws Exception {
        Req write =
                requests.stream()
                        .filter(r -> "PUT".equals(r.method()) && r.path().endsWith("/status"))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "expected a PUT .../status request, got: "
                                                        + requests));
        JsonNode status = MAPPER.readTree(write.body()).path("status");
        assertThat(status.isObject()).as("PUT .../status 请求体必须包含 status 字段").isTrue();
        return status;
    }
}
