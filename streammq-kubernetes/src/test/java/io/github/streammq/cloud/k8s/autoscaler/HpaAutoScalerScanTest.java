/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.autoscaler;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.github.streammq.cloud.k8s.HpaMetricsProvider;
import io.github.streammq.cloud.k8s.operator.StreamMQCluster;
import io.github.streammq.diagnostics.spi.BacklogProbe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

/**
 * {@link HpaAutoScaler#scanOnce()} 扫描契约测试：积压探针生产者（K6）与扫描范围收敛 / 状态清理（K9）。
 *
 * <p><b>锁定的红队发现：K6（HPA 指标无生产者，跳过决策静默失效）与 K9（扫描恒为全集群 inAnyNamespace，且每-CR 状态 map 只增不减）</b>
 *
 * <p><b>失败即红说明：</b>
 *
 * <ul>
 *   <li>用例一（K6）：撤销「扫描前用 {@link BacklogProbe} 灌入真实 lag」后，{@code HpaMetricsProvider} 里没有任何指标， {@code
 *       getConsumerLag("orders", "cg-1") > 0} 立即红（这正是「未声明生产者 → HPA 恒不扩缩」的逃逸路径）；
 *   <li>用例二（K6）：撤销「未声明 topic/consumerGroup 时 fail-closed」后，代码会退化为按猜测维度取指标， 本用例断言「不写任何指标 + 输出
 *       WARN」会红；
 *   <li>用例三（K9）：撤销扫描范围收敛后 {@code listHpaEnabledClusters()} 回到 {@code inAnyNamespace()}，ns-b 的 CR
 *       也被扫描并写入指标， {@code getConsumerLag("other-topic", "cg-b") == 0} 与「无全集群路径请求」断言同时红；
 *   <li>用例四（K9）：撤销 {@code pruneStaleState} 后告警限频记录不会随 CR 消失而清理，CR 重新出现时被旧的 300s 限频压制， 第二次 WARN 不出现
 *       → {@code warnCount == 2} 断言红；
 *   <li>用例五（K9）：扫描范围为空时必须限频告警且不发任何 list 请求（旧实现会全集群 list）。
 * </ul>
 *
 * <p>依赖假设：list 响应由测试侧构造的 {@code CustomResourceList<StreamMQCluster>} POJO 提供（expectation 模式）。 不使用
 * CRUD 模式，因为 fabric8 6.13.1 的 CRUD dispatcher 需要 {@code GenericKubernetesResource} 序列化，而本仓库将
 * Jackson 统一为 2.21.4 后该路径抛 {@code JsonMappingException}。
 *
 * @author StreamMQ Contributors
 */
@DisplayName("HpaAutoScaler 扫描契约测试（K6/K9）")
class HpaAutoScalerScanTest {

    private static final String CLUSTER_LIST_PATH = "/apis/streammq.io/v1/streammqclusters";

    private KubernetesMockServer server;
    private KubernetesClient client;
    private HpaAutoScaler scaler;
    private HpaMetricsProvider metrics;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger scalerLogger;

    @BeforeEach
    void setUp() {
        server = new KubernetesMockServer(false);
        server.init();
        client = server.createClient();
        metrics = new HpaMetricsProvider();
        scaler = new HpaAutoScaler();
        scaler.setKubernetesClient(client);
        scaler.setMetricsProvider(metrics);
        scalerLogger = (Logger) LoggerFactory.getLogger(HpaAutoScaler.class);
        scalerLogger.setLevel(Level.DEBUG);
        logAppender = new ListAppender<>();
        logAppender.start();
        scalerLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        scalerLogger.detachAppender(logAppender);
        client.close();
        server.destroy();
    }

    @Test
    @DisplayName("K6 - 扫描前以 BacklogProbe 的真实 pendingCount 灌入 lag 指标（topic/consumerGroup 按 CR 声明）")
    void scanOnceFeedsLagMetricFromBacklogProbe() throws Exception {
        StreamMQCluster cluster = cluster("ns-a", "demo", "orders", "cg-1");
        expectClusterList(CLUSTER_LIST_PATH, clusterList(cluster));
        List<String> probeCalls = new ArrayList<>();
        scaler.setBacklogProbeProvider(
                fixedProbe(
                        (topic, group) -> {
                            probeCalls.add(topic + ":" + group);
                            return new BacklogProbe.Result(10_000L, 42L);
                        }));

        scaler.scanOnce();

        assertThat(probeCalls).as("探针必须按 CR 显式声明的 topic/group 采样").containsExactly("orders:cg-1");
        assertThat(metrics.getConsumerLag("orders", "cg-1"))
                .as("K6：探针读到的 pendingCount 必须写入 HpaMetricsProvider，HPA 才有数据可读")
                .isEqualTo(42L);
    }

    @Test
    @DisplayName("K6 - 未声明 autoScale.topic/consumerGroup 时不写指标并输出限频 WARN（fail-closed）")
    void scanOnceWithoutDeclaredTopicWritesNoMetricAndWarns() {
        StreamMQCluster cluster = cluster("ns-a", "demo", null, null);
        expectClusterList(CLUSTER_LIST_PATH, clusterList(cluster));
        scaler.setBacklogProbeProvider(
                fixedProbe((topic, group) -> new BacklogProbe.Result(10_000L, 42L)));

        scaler.scanOnce();

        assertThat(metrics.getConsumerMetrics())
                .as("K6：维度未声明时必须跳过（框架不存在由 CR 名派生的约定），不得写入任何指标")
                .isEmpty();
        assertThat(warnMessages())
                .as("跳过决策必须伴随可诊断的 WARN，不得静默失效")
                .anySatisfy(
                        message ->
                                assertThat(message)
                                        .contains("topic/consumerGroup is not")
                                        .contains("fail-closed"));
    }

    @Test
    @DisplayName("K9 - 收敛模式下只扫描配置的命名空间（不发起全集群 list）")
    void scanOnceConvergesToListedNamespaces() throws Exception {
        StreamMQCluster inScope = cluster("ns-a", "demo", "orders", "cg-1");
        StreamMQCluster outOfScope = cluster("ns-b", "other", "other-topic", "cg-b");
        expectClusterList(
                "/apis/streammq.io/v1/namespaces/ns-a/streammqclusters", clusterList(inScope));
        expectClusterList(
                "/apis/streammq.io/v1/namespaces/ns-b/streammqclusters", clusterList(outOfScope));
        scaler.setBacklogProbeProvider(
                fixedProbe((topic, group) -> new BacklogProbe.Result(5_000L, 42L)));
        scaler.setWatchAllNamespaces(false);
        scaler.setWatchNamespaces(List.of("ns-a"));

        scaler.scanOnce();

        assertThat(metrics.getConsumerLag("orders", "cg-1")).isEqualTo(42L);
        assertThat(metrics.getConsumerLag("other-topic", "cg-b"))
                .as("K9：ns-b 不在扫描范围内，其指标不得被刷新")
                .isZero();
        assertThat(clusterRequests())
                .as("K9：收敛模式不得对全集群路径发起 list（否则 RBAC 仍需 ClusterRole）")
                .isNotEmpty()
                .allSatisfy(path -> assertThat(path).contains("/namespaces/ns-a/"));
    }

    @Test
    @DisplayName("K9 - 扫描范围为空时告警且不发请求")
    void scanOnceWithEmptyScopeWarnsAndSkipsRequests() throws Exception {
        scaler.setWatchAllNamespaces(false);
        scaler.setWatchNamespaces(List.of());
        scaler.setBacklogProbeProvider(
                fixedProbe((topic, group) -> new BacklogProbe.Result(1L, 42L)));

        scaler.scanOnce();

        assertThat(clusterRequests()).as("K9：范围为空不得退化为全集群 list").isEmpty();
        assertThat(warnMessages())
                .anySatisfy(message -> assertThat(message).contains("HPA scan scope is empty"));
    }

    @Test
    @DisplayName("K9 - CR 消失后清理每-CR 状态（告警限频记录随 CR 生命周期回收）")
    void scanOncePrunesStateOfRemovedClusters() throws Exception {
        StreamMQCluster cluster = cluster("ns-a", "demo", null, null);
        // 第一轮：CR 存在 → WARN；第二轮：CR 消失 → 状态被清理；第三轮：CR 回来 → 限频记录已被回收，必须再次 WARN（300s 限频内）
        server.expect()
                .get()
                .withPath(CLUSTER_LIST_PATH)
                .andReturn(200, clusterList(cluster))
                .once();
        server.expect().get().withPath(CLUSTER_LIST_PATH).andReturn(200, clusterList()).once();
        server.expect()
                .get()
                .withPath(CLUSTER_LIST_PATH)
                .andReturn(200, clusterList(cluster))
                .always();

        scaler.scanOnce();
        assertThat(warnCount("topic/consumerGroup is not")).isEqualTo(1);
        scaler.scanOnce();
        assertThat(warnCount("topic/consumerGroup is not"))
                .as("同一 CR 在 300s 限频窗口内不得重复告警")
                .isEqualTo(1);
        scaler.scanOnce();
        assertThat(warnCount("topic/consumerGroup is not"))
                .as("K9：CR 消失时其限频记录必须被 prune，否则 CR 重建后被旧记录永久压制")
                .isEqualTo(2);
    }

    // ==================== fixtures ====================

    private static StreamMQCluster cluster(
            String namespace, String name, String topic, String consumerGroup) {
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
        spec.setImage("registry.example.com/streammq-consumer:1.0.0");
        spec.setReplicas(2);
        StreamMQCluster.AutoScale autoScale = new StreamMQCluster.AutoScale();
        autoScale.setEnabled(true);
        autoScale.setTopic(topic);
        autoScale.setConsumerGroup(consumerGroup);
        autoScale.setMinReplicas(1);
        autoScale.setMaxReplicas(5);
        autoScale.setTargetLag(100);
        autoScale.setScaleUpThreshold(80);
        autoScale.setScaleDownThreshold(20);
        spec.setAutoScale(autoScale);
        cluster.setSpec(spec);
        return cluster;
    }

    private static CustomResourceList<StreamMQCluster> clusterList(StreamMQCluster... clusters) {
        CustomResourceList<StreamMQCluster> list = new CustomResourceList<>();
        list.getItems().addAll(List.of(clusters));
        return list;
    }

    private void expectClusterList(String path, CustomResourceList<StreamMQCluster> response) {
        server.expect().get().withPath(path).andReturn(200, response).always();
    }

    /** 包装探针为 {@link ObjectProvider} 桩（生产代码用 ObjectProvider 容忍探针缺席）。 */
    private static ObjectProvider<BacklogProbe> fixedProbe(BacklogProbe probe) {
        return new FixedProbeProvider(probe);
    }

    private List<String> clusterRequests() throws InterruptedException {
        List<String> paths = new ArrayList<>();
        RecordedRequest request;
        while ((request = server.takeRequest(200, TimeUnit.MILLISECONDS)) != null) {
            if (request.getPath().contains("streammqclusters")) {
                paths.add(request.getPath());
            }
        }
        return paths;
    }

    private List<String> warnMessages() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private long warnCount(String fragment) {
        return warnMessages().stream().filter(message -> message.contains(fragment)).count();
    }

    /** 固定返回同一探针的 {@link ObjectProvider} 桩（避免 Mockito 泛型裸类型转换带来的 unchecked 告警）。 */
    private static final class FixedProbeProvider implements ObjectProvider<BacklogProbe> {

        private final BacklogProbe probe;

        FixedProbeProvider(BacklogProbe probe) {
            this.probe = probe;
        }

        @Override
        public BacklogProbe getObject() {
            return probe;
        }

        @Override
        public BacklogProbe getObject(Object... args) {
            return probe;
        }

        @Override
        public BacklogProbe getIfAvailable() {
            return probe;
        }

        @Override
        public BacklogProbe getIfUnique() {
            return probe;
        }

        @Override
        public Iterator<BacklogProbe> iterator() {
            return List.of(probe).iterator();
        }
    }
}
