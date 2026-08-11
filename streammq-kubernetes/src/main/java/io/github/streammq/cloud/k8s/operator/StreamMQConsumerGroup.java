package io.github.streammq.cloud.k8s.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.client.CustomResource;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * StreamMQConsumerGroup CRD for managing consumer groups.
 *
 * <p>Declarative consumer group management through Kubernetes native resources:
 *
 * <ul>
 *   <li>Associate consumer groups with topics and clusters
 *   <li>Configure auto-scaling per consumer group
 *   <li>Manage consumer replicas and resource limits
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 2.0.0
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StreamMQConsumerGroup extends CustomResource {

    @JsonProperty("spec")
    private ConsumerGroupSpec spec;

    @JsonProperty("status")
    private ConsumerGroupStatus status;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConsumerGroupSpec {
        private String clusterRef;
        private String topic;
        private Integer replicas = 3;
        private AutoScale autoScale;
        private Resources resources;
        private ConsumerConfig config;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AutoScale {
        private Boolean enabled = false;
        private Integer minReplicas = 1;
        private Integer maxReplicas = 10;
        private Integer targetLag = 100;
        private Integer scaleUpThreshold = 80;
        private Integer scaleDownThreshold = 20;
        private Integer stabilizationWindowSeconds = 300;
        private Integer scaleUpCooldownSeconds = 60;
        private Integer scaleDownCooldownSeconds = 300;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Resources {
        private String cpuRequest = "500m";
        private String cpuLimit = "1000m";
        private String memoryRequest = "512Mi";
        private String memoryLimit = "1Gi";
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConsumerConfig {
        private Integer maxReconsumeTimes = 16;
        private Long[] retryIntervals;
        private Long retryScanMs = 5000L;
        private Long delayScanMs = 1000L;
        private Integer pullBatchSize = 32;
        private Long consumeTimeoutMs = 30000L;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConsumerGroupStatus {
        private String phase = "Pending";
        private Integer replicas = 0;
        private Integer readyReplicas = 0;
        private String lastUpdateTime;
        private Long observedGeneration;
    }
}
