/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * {@link StreamMQCluster} CRD 模型契约测试。
 *
 * <p><b>锁定的红队发现：K1（CRD 注解缺失 → 构造器抛异常）</b>
 *
 * <p><b>失败即红说明：</b>撤销 K1 修复（移除 {@code @Group}/{@code @Version}）后：
 *
 * <ul>
 *   <li>{@code new StreamMQCluster()} 立即抛 {@link IllegalArgumentException}——fabric8 {@link
 *       io.fabric8.kubernetes.client.CustomResource} 构造期强制校验注解，informer 反序列化、 {@code
 *       updateStatus()}、HPA 回写全部恒抛，本类所有用例直接红；
 *   <li>{@code getApiVersion()} 退化为 {@code "/"}（group/version 皆空）而非 {@code streammq.io/v1}；
 *   <li>CRD yaml 一致性断言（group/version/kind/plural）与模型常量脱钩后会红，防止两处漂移。
 * </ul>
 *
 * <p>另锁定 CRD yaml 中 {@code status} 子资源与 {@code status} 字段清单（K4/K5 的前置契约： 缺 {@code
 * subresources.status} 时真实集群上 {@code updateStatus()} 直接 404）以及 {@code
 * spec.autoScale.topic/consumerGroup}（K6：HPA 指标维度必须显式声明）。
 *
 * @author StreamMQ Contributors
 */
@DisplayName("StreamMQCluster CRD 模型契约测试")
class StreamMQClusterModelTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("K1 - 无参构造不抛异常，且 apiVersion/kind 由注解推导（informer 反序列化前置条件）")
    void noArgConstructorDoesNotThrowAndDerivesApiVersionAndKind() {
        // 失败即红：缺 @Group/@Version 时 CustomResource 构造器抛 IllegalArgumentException
        assertThatCode(StreamMQCluster::new).doesNotThrowAnyException();

        StreamMQCluster cluster = new StreamMQCluster();
        assertThat(cluster.getApiVersion())
                .as("apiVersion 必须由 @Group/@Version 推导")
                .isEqualTo("streammq.io/v1");
        assertThat(cluster.getKind()).isEqualTo("StreamMQCluster");
        assertThat(StreamMQCluster.GROUP).isEqualTo("streammq.io");
        assertThat(StreamMQCluster.VERSION).isEqualTo("v1");
        assertThat(cluster.getApiVersion())
                .isEqualTo(StreamMQCluster.GROUP + "/" + StreamMQCluster.VERSION);
    }

    @Test
    @DisplayName("K1 - Jackson 往返后字段保持，apiVersion/kind 正确（含 K6 的 autoScale.topic/consumerGroup）")
    void jacksonRoundTripKeepsFieldsIncludingAutoScaleTopicAndConsumerGroup() throws Exception {
        StreamMQCluster cluster = new StreamMQCluster();
        cluster.setMetadata(new ObjectMetaBuilder().withName("demo").withNamespace("ns-a").build());
        StreamMQCluster.Spec spec = new StreamMQCluster.Spec();
        spec.setImage("registry.example.com/streammq-consumer:1.0.0");
        spec.setReplicas(4);
        StreamMQCluster.AutoScale autoScale = new StreamMQCluster.AutoScale();
        autoScale.setEnabled(true);
        autoScale.setTopic("orders");
        autoScale.setConsumerGroup("order-consumer-group");
        spec.setAutoScale(autoScale);
        cluster.setSpec(spec);

        String json = Serialization.asJson(cluster);
        JsonNode tree = MAPPER.readTree(json);
        assertThat(tree.path("apiVersion").asText()).isEqualTo("streammq.io/v1");
        assertThat(tree.path("kind").asText()).isEqualTo("StreamMQCluster");
        assertThat(tree.path("spec").path("autoScale").path("topic").asText()).isEqualTo("orders");
        assertThat(tree.path("spec").path("autoScale").path("consumerGroup").asText())
                .isEqualTo("order-consumer-group");

        StreamMQCluster back = Serialization.unmarshal(json, StreamMQCluster.class);
        assertThat(back.getApiVersion()).isEqualTo("streammq.io/v1");
        assertThat(back.getKind()).isEqualTo("StreamMQCluster");
        assertThat(back.getMetadata().getName()).isEqualTo("demo");
        assertThat(back.getSpec().getImage())
                .isEqualTo("registry.example.com/streammq-consumer:1.0.0");
        assertThat(back.getSpec().getReplicas()).isEqualTo(4);
        assertThat(back.getSpec().getAutoScale().getTopic()).isEqualTo("orders");
        assertThat(back.getSpec().getAutoScale().getConsumerGroup())
                .isEqualTo("order-consumer-group");
        assertThat(back.getSpec().getAutoScale().getEnabled()).isTrue();
    }

    @Test
    @DisplayName("K1 - CRD yaml 的 group/version/kind/plural 与模型常量一致")
    @SuppressWarnings("unchecked")
    void crdYamlMatchesModelConstants() {
        Map<String, Object> crd = loadCrdYaml();
        assertThat(crd.get("apiVersion")).isEqualTo("apiextensions.k8s.io/v1");
        assertThat(crd.get("kind")).isEqualTo("CustomResourceDefinition");

        Map<String, Object> spec = (Map<String, Object>) crd.get("spec");
        assertThat(spec.get("group"))
                .as("CRD yaml group 必须等于 StreamMQCluster.GROUP")
                .isEqualTo(StreamMQCluster.GROUP);
        assertThat(spec.get("scope")).isEqualTo("Namespaced");

        Map<String, Object> names = (Map<String, Object>) spec.get("names");
        assertThat(names.get("kind")).isEqualTo(StreamMQCluster.class.getSimpleName());
        assertThat(names.get("plural")).isEqualTo("streammqclusters");
        assertThat(names.get("singular")).isEqualTo("streammqcluster");

        List<Map<String, Object>> versions = (List<Map<String, Object>>) spec.get("versions");
        assertThat(versions).hasSize(1);
        Map<String, Object> version = versions.get(0);
        assertThat(version.get("name"))
                .as("CRD yaml version 必须等于 StreamMQCluster.VERSION")
                .isEqualTo(StreamMQCluster.VERSION);
        assertThat(version.get("served")).isEqualTo(Boolean.TRUE);
        assertThat(version.get("storage")).isEqualTo(Boolean.TRUE);
        assertThat((Map<String, Object>) version.get("subresources"))
                .as("缺 status 子资源时真实集群的 updateStatus() 会 404（K4/K5 前置契约）")
                .containsKey("status");
    }

    @Test
    @DisplayName("K1 - CRD yaml 的 status 字段与 autoScale.topic/consumerGroup 与模型对齐")
    @SuppressWarnings("unchecked")
    void crdYamlStatusAndAutoScaleSchemaMatchModel() {
        Map<String, Object> crd = loadCrdYaml();
        Map<String, Object> spec = (Map<String, Object>) crd.get("spec");
        List<Map<String, Object>> versions = (List<Map<String, Object>>) spec.get("versions");
        Map<String, Object> openApiSchema =
                (Map<String, Object>)
                        ((Map<String, Object>) versions.get(0).get("schema"))
                                .get("openAPIV3Schema");
        Map<String, Object> rootProperties = (Map<String, Object>) openApiSchema.get("properties");

        Map<String, Object> statusProperties =
                (Map<String, Object>)
                        ((Map<String, Object>) rootProperties.get("status")).get("properties");
        assertThat(statusProperties)
                .containsKeys(
                        "phase",
                        "replicas",
                        "readyReplicas",
                        "lastUpdateTime",
                        "observedGeneration");
        Map<String, Object> phase = (Map<String, Object>) statusProperties.get("phase");
        assertThat((List<String>) phase.get("enum"))
                .containsExactlyInAnyOrder(
                        StreamMQK8sDefaults.PHASE_PENDING,
                        StreamMQK8sDefaults.PHASE_READY,
                        StreamMQK8sDefaults.PHASE_NOT_READY,
                        StreamMQK8sDefaults.PHASE_UPDATING,
                        StreamMQK8sDefaults.PHASE_FAILED);

        Map<String, Object> specProperties =
                (Map<String, Object>)
                        ((Map<String, Object>) rootProperties.get("spec")).get("properties");
        Map<String, Object> autoScaleProperties =
                (Map<String, Object>)
                        ((Map<String, Object>) specProperties.get("autoScale")).get("properties");
        assertThat(autoScaleProperties)
                .as("K6：HPA 指标维度必须在 CRD 中可声明")
                .containsKeys("topic", "consumerGroup");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadCrdYaml() {
        try (InputStream in = getClass().getResourceAsStream("/crd/streammq-cluster.yaml")) {
            assertThat(in).as("CRD yaml 必须在 classpath 上（src/main/resources/crd/）").isNotNull();
            Object loaded = new Yaml().load(new InputStreamReader(in, StandardCharsets.UTF_8));
            return (Map<String, Object>) loaded;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read CRD yaml", e);
        }
    }
}
