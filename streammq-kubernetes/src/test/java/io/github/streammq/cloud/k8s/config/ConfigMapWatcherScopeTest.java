/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.config;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapListBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link ConfigMapConfigRefresher} 的命名空间收敛（K3）与版本键契约（K10）测试。
 *
 * <p><b>锁定的红队发现：K3（informer 未加 {@code inNamespace(ns)} → 实际 list/watch 整个集群，缺少 ClusterRole 时 403）与
 * K10 （版本键用 watch 配置的 ns/name → label 模式下多 CM 互相覆盖；{@code addWatchConfig} 在 {@code start()}
 * 后静默无效）</b>
 *
 * <p><b>失败即红说明：</b>
 *
 * <ul>
 *   <li>用例一（K3，mock server 线级断言）：撤销 {@code informerFactory.inNamespace(ns)} 后 fabric8 退回 {@code
 *       inAnyNamespace()}，录到的 ConfigMap 请求路径变成 {@code /api/v1/configmaps?watch=true...}（无 {@code
 *       /namespaces/}），「每个 CM 请求路径都包含配置的命名空间」与「不得出现全集群路径」断言红；
 *   <li>用例二（K10）：撤销「{@code start()} 后 addWatchConfig 记 WARN 并忽略」后，配置被静默追加（永远不注册 informer）， {@code
 *       watchConfigs.size()} 不变与 WARN 断言红；
 *   <li>用例三（K10，反射直达 {@code processConfigMap}）：撤销「按实际 ConfigMap 的 ns/name 记版本键」后，两个 CM 落到同一个键
 *       （watch 配置的 ns/name），{@code containsOnlyKeys("ns-a/cm-x", "ns-a/cm-y")} 断言红——这正是「label 模式下
 *       更新其它 CM 被判为已处理」的逃逸路径。
 * </ul>
 *
 * <p>依赖假设：用例一用 expectation 模式 mock server 记录 informer 的真实 HTTP 请求路径（不断言响应语义）； 用例三用反射调用包内私有 {@code
 * processConfigMap} / 读取私有 {@code processedVersions}（驱动 informer 事件需要精确的 watch 流，成本高且脆弱）。
 *
 * @author StreamMQ Contributors
 */
@DisplayName("ConfigMap 刷新器命名空间收敛与版本键测试（K3/K10）")
class ConfigMapWatcherScopeTest {

    private KubernetesMockServer server;
    private KubernetesClient client;
    private ConfigMapConfigRefresher refresher;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger refresherLogger;

    @BeforeAll
    static void silenceMockServerLogs() {
        java.util.logging.Logger.getLogger("okhttp3.mockwebserver.MockWebServer")
                .setLevel(java.util.logging.Level.WARNING);
    }

    @BeforeEach
    void setUp() {
        server = new KubernetesMockServer(false);
        server.init();
        client = server.createClient();
        refresher = new ConfigMapConfigRefresher();
        refresher.setKubernetesClient(client);
        refresherLogger = (Logger) LoggerFactory.getLogger(ConfigMapConfigRefresher.class);
        refresherLogger.setLevel(Level.DEBUG);
        logAppender = new ListAppender<>();
        logAppender.start();
        refresherLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        refresher.stop();
        refresherLogger.detachAppender(logAppender);
        client.close();
        server.destroy();
    }

    @Test
    @DisplayName("K3 - informer 按配置命名空间逐个注册：CM 请求路径必须带命名空间，不得全集群 list/watch")
    void informersAreRegisteredPerNamespace() throws Exception {
        server.expect()
                .get()
                .withPath("/api/v1/namespaces/ns-a/configmaps?resourceVersion=0")
                .andReturn(200, new ConfigMapListBuilder().build())
                .always();
        server.expect()
                .get()
                .withPath("/api/v1/namespaces/ns-b/configmaps?resourceVersion=0")
                .andReturn(200, new ConfigMapListBuilder().build())
                .always();
        refresher.setWatchNamespaces(List.of("ns-a", "ns-b"));

        refresher.start();
        List<String> paths = awaitConfigMapRequests(2, 8_000L);

        assertThat(refresher.getWatchConfigs())
                .extracting(ConfigMapConfigRefresher.ConfigMapWatchConfig::getNamespace)
                .containsExactly("ns-a", "ns-b");
        assertThat(paths)
                .as("两个命名空间都必须各自发起 list/watch")
                .anySatisfy(path -> assertThat(path).contains("/namespaces/ns-a/"))
                .anySatisfy(path -> assertThat(path).contains("/namespaces/ns-b/"));
        assertThat(paths)
                .as("K3：收敛模式下不得对全集群路径（/api/v1/configmaps）发起请求，否则仍需 ClusterRole")
                .allSatisfy(path -> assertThat(path).contains("/namespaces/"))
                .noneSatisfy(path -> assertThat(path).startsWith("/api/v1/configmaps"));
    }

    @Test
    @DisplayName("K10 - addWatchConfig 只允许在 start() 之前追加；start() 之后记 WARN 并忽略")
    void addWatchConfigAfterStartIsRejectedWithWarn() {
        ConfigMapConfigRefresher.ConfigMapWatchConfig before =
                watchConfig("ns-before", ConfigMapConfigRefresher.DEFAULT_WATCH_CONFIG_MAP_NAME);
        refresher.addWatchConfig(before);
        assertThat(refresher.getWatchConfigs()).hasSize(1);
        assertThat(refresher.getWatchConfigs().get(0).getNamespace()).isEqualTo("ns-before");

        refresher.start();
        assertThat(refresher.isRunning()).isTrue();
        int sizeAfterStart = refresher.getWatchConfigs().size();

        refresher.addWatchConfig(watchConfig("ns-after", "cm-after"));

        assertThat(refresher.getWatchConfigs())
                .as("K10：start() 之后追加的 watch 配置不会被注册，必须拒绝而不是静默无效")
                .hasSize(sizeAfterStart);
        assertThat(warnMessages())
                .anySatisfy(
                        message ->
                                assertThat(message)
                                        .contains("addWatchConfig after start() is not supported"));
    }

    @Test
    @DisplayName("K10 - 版本键使用实际 ConfigMap 的 ns/name（label 模式下多 CM 不互相覆盖）")
    @SuppressWarnings("unchecked")
    void processedVersionKeyUsesActualConfigMapNamespaceAndName() throws Exception {
        Method process =
                ConfigMapConfigRefresher.class.getDeclaredMethod(
                        "processConfigMap", ConfigMap.class);
        process.setAccessible(true);
        Field versionsField = ConfigMapConfigRefresher.class.getDeclaredField("processedVersions");
        versionsField.setAccessible(true);

        ConfigMap cmX = labeledConfigMap("ns-a", "cm-x", "1", Map.of("maxReconsumeTimes", "5"));
        ConfigMap cmY = labeledConfigMap("ns-a", "cm-y", "2", Map.of("maxReconsumeTimes", "6"));
        ConfigMap noData = labeledConfigMap("ns-a", "cm-empty", "3", Map.of());

        process.invoke(refresher, cmX);
        process.invoke(refresher, cmY);
        process.invoke(refresher, noData);

        Map<String, String> processed = (Map<String, String>) versionsField.get(refresher);
        assertThat(processed)
                .as("K10：版本键必须是实际 CM 的 ns/name，否则同命名空间多个匹配 CM 会互相覆盖")
                .containsOnlyKeys("ns-a/cm-x", "ns-a/cm-y");
        assertThat(processed.get("ns-a/cm-x")).isEqualTo("1");
        assertThat(processed.get("ns-a/cm-y")).isEqualTo("2");
        assertThat(processed).as("无 data 的 CM 不得被记为已处理").doesNotContainKey("ns-a/cm-empty");
    }

    // ==================== fixtures ====================

    private static ConfigMapConfigRefresher.ConfigMapWatchConfig watchConfig(
            String namespace, String name) {
        ConfigMapConfigRefresher.ConfigMapWatchConfig config =
                new ConfigMapConfigRefresher.ConfigMapWatchConfig();
        config.setNamespace(namespace);
        config.setName(name);
        config.setEnabled(true);
        return config;
    }

    private static ConfigMap labeledConfigMap(
            String namespace, String name, String resourceVersion, Map<String, String> data) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withResourceVersion(resourceVersion)
                .withLabels(Map.of("streammq.io/config", "streammq.io/config"))
                .endMetadata()
                .withData(data)
                .build();
    }

    private List<String> awaitConfigMapRequests(int expectedAtLeast, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        List<String> paths = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            RecordedRequest request;
            while ((request = server.takeRequest(150, TimeUnit.MILLISECONDS)) != null) {
                if (request.getPath().contains("configmaps")) {
                    paths.add(request.getPath());
                }
            }
            if (paths.size() >= expectedAtLeast) {
                return paths;
            }
            Thread.sleep(100L);
        }
        return paths;
    }

    private List<String> warnMessages() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }
}
