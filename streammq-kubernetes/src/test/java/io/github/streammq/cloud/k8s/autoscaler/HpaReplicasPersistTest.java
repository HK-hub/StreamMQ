/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.autoscaler;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v1.ScaleBuilder;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.github.streammq.cloud.k8s.HpaMetricsProvider;
import io.github.streammq.cloud.k8s.operator.StreamMQCluster;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link HpaAutoScaler} 的 {@code persistReplicas} 回写契约测试。
 *
 * <p><b>锁定的红队发现：K7（用 informer 快照整对象 replace 回写 spec.replicas，覆盖用户并发修改的其它 spec 字段且无冲突检测）</b>
 *
 * <p><b>失败即红说明：</b>
 *
 * <ul>
 *   <li>用例一（线级断言）：旧实现把整对象 replace 回去，CR 路径上会出现携带 {@code spec.image} 等完整 spec 的请求体； {@code spec 只含
 *       replicas}、{@code 请求体不含 image/status} 与 {@code content-type=application/merge-patch+json}
 *       断言全部红；
 *   <li>用例二（409 重试上限）：旧实现没有 GET-最新版本 + 409 重试，服务器恒返回 409 时只有 1 次 PATCH（或直接把异常 抛给调用方），{@code PATCH
 *       恰好 3 次} 与「每次重试携带刚读到的最新 resourceVersion」断言红；
 *   <li>用例三（冲突后收敛）：前两次 409、第三次成功时旧实现已提前失败，{@code persistReplicas 返回 true} 断言红；
 *   <li>用例四（非 409 不重试）：旧实现（若引入重试）对任何错误都重试会放大请求；{@code 恰好 1 次 PATCH} 断言红。
 * </ul>
 *
 * <p>依赖假设：全部用例使用 expectation 模式 mock server。409/403 由 mock server 真实返回（客户端真实抛出 {@code
 * KubernetesClientException}，{@code getCode()} 即 HTTP 状态），比 Mockito 更贴近生产； 私有 {@code
 * persistReplicas} 通过反射直达（生产入口 {@code scanOnce} 需要完整「稳定窗口 + 冷却」链路）。
 *
 * @author StreamMQ Contributors
 */
@DisplayName("HpaAutoScaler spec.replicas 回写契约测试（K7）")
class HpaReplicasPersistTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String NS = "ns-a";
    private static final String NAME = "demo";
    private static final String CR_PATH =
            "/apis/streammq.io/v1/namespaces/" + NS + "/streammqclusters/" + NAME;
    private static final String LIST_PATH =
            "/apis/streammq.io/v1/namespaces/" + NS + "/streammqclusters";
    private static final String SCALE_PATH =
            "/apis/apps/v1/namespaces/" + NS + "/deployments/" + NAME + "/scale";
    private static final String DEPLOY_PATH =
            "/apis/apps/v1/namespaces/" + NS + "/deployments/" + NAME;

    private KubernetesMockServer server;
    private KubernetesClient client;

    @BeforeEach
    void setUp() {
        server = new KubernetesMockServer(false);
        server.init();
        client = server.createClient();
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.destroy();
    }

    @Test
    @DisplayName("K7 - 回写只含 spec.replicas + metadata.resourceVersion（merge patch，不动其它字段）")
    void persistReplicasSendsMinimalMergePatch() throws Exception {
        StreamMQCluster cluster = hpaCluster(2, 5);
        server.expect().get().withPath(LIST_PATH).andReturn(200, clusterList(cluster)).always();
        server.expect().get().withPath(CR_PATH).andReturn(200, cluster).always();
        server.expect().get().withPath(DEPLOY_PATH).andReturn(200, deployment()).always();
        server.expect().get().withPath(SCALE_PATH).andReturn(200, scale(2)).always();
        server.expect().put().withPath(SCALE_PATH).andReturn(200, scale(5)).always();
        server.expect().patch().withPath(CR_PATH).andReturn(200, cluster).always();

        HpaMetricsProvider metrics = new HpaMetricsProvider();
        metrics.recordLag("orders", "cg-1", 900L);
        HpaAutoScaler scaler = new HpaAutoScaler();
        scaler.setKubernetesClient(client);
        scaler.setMetricsProvider(metrics);
        scaler.setWatchAllNamespaces(false);
        scaler.setWatchNamespaces(List.of(NS));

        scaler.scanOnce();
        scaler.scanOnce();

        List<Req> requests = drainRequests();
        List<Req> crWrites =
                requests.stream()
                        .filter(r -> r.path().equals(CR_PATH) && !"GET".equals(r.method()))
                        .toList();
        assertThat(crWrites).as("K7：回写必须是唯一的 CR 写请求，且不得是整对象 replace（PUT）").hasSize(1);
        Req write = crWrites.get(0);
        assertThat(write.method()).isEqualTo("PATCH");
        assertThat(write.contentType()).contains("merge-patch");

        JsonNode body = MAPPER.readTree(write.body());
        assertThat(fieldNames(body))
                .as("K7：请求体只能有 metadata + spec 两个顶层键")
                .containsExactlyInAnyOrder("metadata", "spec");
        assertThat(fieldNames(body.path("metadata")))
                .as("K7：metadata 只携带乐观锁 resourceVersion")
                .containsExactly("resourceVersion");
        assertThat(body.path("metadata").path("resourceVersion").asText()).isEqualTo("11");
        assertThat(fieldNames(body.path("spec")))
                .as("K7：spec 只写 replicas，用户并发修改的 image/backend 等字段不得被覆盖")
                .containsExactly("replicas");
        assertThat(body.path("spec").path("replicas").asInt()).isEqualTo(5);
        assertThat(write.body())
                .as("K7：回写请求体不得包含 image/status（旧实现整对象 replace 的指纹）")
                .doesNotContain("image")
                .doesNotContain("status");
    }

    @Test
    @DisplayName("K7 - 409 冲突重试上限 3 次，每次都重新读取最新 resourceVersion，最终返回 false")
    void persistReplicasRetriesWithFreshResourceVersionAndFailsAfterLimit() throws Exception {
        // 用有状态的 mock 模拟真实乐观锁：GET 返回当前版本，409 的 PATCH 把版本推进一格，
        // 因此每次重试携带的 resourceVersion 必须是「重新 GET 之后」的新值（缓存旧值无法通过）。
        AtomicInteger currentVersion = new AtomicInteger(1);
        server.expect()
                .get()
                .withPath(CR_PATH)
                .andReply(
                        200, request -> crWithResourceVersion(String.valueOf(currentVersion.get())))
                .always();
        server.expect()
                .patch()
                .withPath(CR_PATH)
                .andReply(
                        409,
                        request -> {
                            currentVersion.incrementAndGet();
                            return conflictStatus();
                        })
                .always();

        HpaAutoScaler scaler = new HpaAutoScaler();
        scaler.setKubernetesClient(client);

        Object result = invokePersistReplicas(scaler, NS, NAME, 7);

        assertThat(result).as("K7：重试耗尽必须返回失败语义（调用方不得以为已持久化）").isEqualTo(Boolean.FALSE);
        List<Req> writes = patches(drainRequests());
        assertThat(writes).as("K7：409 重试上限为 3（旧实现无重试，只有 1 次）").hasSize(3);
        assertThat(writes)
                .extracting(Req::body)
                .as("K7：每次重试都必须携带刚读取到的最新 resourceVersion")
                .containsExactly(
                        "{\"metadata\":{\"resourceVersion\":\"1\"},\"spec\":{\"replicas\":7}}",
                        "{\"metadata\":{\"resourceVersion\":\"2\"},\"spec\":{\"replicas\":7}}",
                        "{\"metadata\":{\"resourceVersion\":\"3\"},\"spec\":{\"replicas\":7}}");
    }

    @Test
    @DisplayName("K7 - 前两次 409、第三次成功时返回 true（冲突后仍能收敛）")
    void persistReplicasSucceedsAfterConflictRetry() throws Exception {
        AtomicInteger currentVersion = new AtomicInteger(4);
        server.expect()
                .get()
                .withPath(CR_PATH)
                .andReply(
                        200, request -> crWithResourceVersion(String.valueOf(currentVersion.get())))
                .always();
        server.expect()
                .patch()
                .withPath(CR_PATH)
                .andReply(
                        409,
                        request -> {
                            currentVersion.incrementAndGet();
                            return conflictStatus();
                        })
                .once();
        server.expect()
                .patch()
                .withPath(CR_PATH)
                .andReply(
                        409,
                        request -> {
                            currentVersion.incrementAndGet();
                            return conflictStatus();
                        })
                .once();
        server.expect()
                .patch()
                .withPath(CR_PATH)
                .andReply(
                        200, request -> crWithResourceVersion(String.valueOf(currentVersion.get())))
                .always();

        HpaAutoScaler scaler = new HpaAutoScaler();
        scaler.setKubernetesClient(client);

        Object result = invokePersistReplicas(scaler, NS, NAME, 3);

        assertThat(result).isEqualTo(Boolean.TRUE);
        assertThat(patches(drainRequests()))
                .extracting(Req::body)
                .as("K7：冲突后必须用最新版本收敛（4 → 5 → 6）")
                .containsExactly(
                        "{\"metadata\":{\"resourceVersion\":\"4\"},\"spec\":{\"replicas\":3}}",
                        "{\"metadata\":{\"resourceVersion\":\"5\"},\"spec\":{\"replicas\":3}}",
                        "{\"metadata\":{\"resourceVersion\":\"6\"},\"spec\":{\"replicas\":3}}");
    }

    @Test
    @DisplayName("K7 - 非 409 错误不重试，立即返回 false（不吞错也不放大请求）")
    void persistReplicasDoesNotRetryOnNonConflict() throws Exception {
        server.expect()
                .get()
                .withPath(CR_PATH)
                .andReply(200, request -> crWithResourceVersion("9"))
                .always();
        server.expect().patch().withPath(CR_PATH).andReturn(403, forbiddenStatus()).always();

        HpaAutoScaler scaler = new HpaAutoScaler();
        scaler.setKubernetesClient(client);

        Object result = invokePersistReplicas(scaler, NS, NAME, 4);

        assertThat(result).isEqualTo(Boolean.FALSE);
        assertThat(patches(drainRequests())).hasSize(1);
    }

    // ==================== fixtures ====================

    private static Object invokePersistReplicas(
            HpaAutoScaler scaler, String namespace, String name, int replicas)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method method =
                HpaAutoScaler.class.getDeclaredMethod(
                        "persistReplicas", String.class, String.class, int.class);
        method.setAccessible(true);
        return method.invoke(scaler, namespace, name, replicas);
    }

    private static StreamMQCluster hpaCluster(int replicas, int maxReplicas) {
        StreamMQCluster cluster = new StreamMQCluster();
        cluster.setMetadata(
                new ObjectMetaBuilder()
                        .withName(NAME)
                        .withNamespace(NS)
                        .withUid("uid-" + NAME)
                        .withGeneration(1L)
                        .withResourceVersion("11")
                        .build());
        StreamMQCluster.Spec spec = new StreamMQCluster.Spec();
        spec.setImage("registry.example.com/streammq-consumer:1.0.0");
        spec.setReplicas(replicas);
        StreamMQCluster.AutoScale autoScale = new StreamMQCluster.AutoScale();
        autoScale.setEnabled(true);
        autoScale.setTopic("orders");
        autoScale.setConsumerGroup("cg-1");
        autoScale.setMinReplicas(1);
        autoScale.setMaxReplicas(maxReplicas);
        autoScale.setTargetLag(100);
        autoScale.setScaleUpThreshold(80);
        autoScale.setScaleDownThreshold(20);
        autoScale.setScaleUpCooldownSeconds(0);
        autoScale.setScaleDownCooldownSeconds(0);
        spec.setAutoScale(autoScale);
        cluster.setSpec(spec);
        return cluster;
    }

    private static StreamMQCluster crWithResourceVersion(String resourceVersion) {
        StreamMQCluster cluster = new StreamMQCluster();
        cluster.setMetadata(
                new ObjectMetaBuilder()
                        .withName(NAME)
                        .withNamespace(NS)
                        .withResourceVersion(resourceVersion)
                        .build());
        return cluster;
    }

    private static CustomResourceList<StreamMQCluster> clusterList(StreamMQCluster... clusters) {
        CustomResourceList<StreamMQCluster> list = new CustomResourceList<>();
        list.getItems().addAll(List.of(clusters));
        return list;
    }

    private static Status conflictStatus() {
        return new StatusBuilder()
                .withCode(409)
                .withReason("Conflict")
                .withMessage("Operation cannot be fulfilled: resourceVersion conflict")
                .build();
    }

    private static Status forbiddenStatus() {
        return new StatusBuilder()
                .withCode(403)
                .withReason("Forbidden")
                .withMessage("streammqclusters.streammq.io is forbidden")
                .build();
    }

    private static io.fabric8.kubernetes.api.model.apps.Deployment deployment() {
        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NS)
                .endMetadata()
                .withNewSpec()
                .withReplicas(2)
                .endSpec()
                .build();
    }

    private static io.fabric8.kubernetes.api.model.autoscaling.v1.Scale scale(int replicas) {
        return new ScaleBuilder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NS)
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .endSpec()
                .build();
    }

    private static List<Req> patches(List<Req> requests) {
        return requests.stream().filter(r -> "PATCH".equals(r.method())).toList();
    }

    private record Req(String method, String path, String contentType, String body) {}

    private static List<String> fieldNames(JsonNode node) {
        List<String> names = new ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }

    private List<Req> drainRequests() throws InterruptedException {
        List<Req> requests = new ArrayList<>();
        RecordedRequest request;
        while ((request = server.takeRequest(200, TimeUnit.MILLISECONDS)) != null) {
            requests.add(
                    new Req(
                            request.getMethod(),
                            request.getPath(),
                            request.getHeader("Content-Type"),
                            request.getBody().readUtf8()));
        }
        return requests;
    }
}
