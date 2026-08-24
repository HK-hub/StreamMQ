package io.github.streammq.cloud.k8s.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.client.CustomResource;
import io.github.streammq.core.StreamMQConstants;
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
 * @since 0.1.0
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
        private Integer replicas = StreamMQK8sDefaults.DEFAULT_REPLICAS;
        private AutoScale autoScale;
        private Resources resources;
        private ConsumerConfig config;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AutoScale {
        private Boolean enabled = false;
        private Integer minReplicas = StreamMQK8sDefaults.AUTOSCALE_MIN_REPLICAS;
        private Integer maxReplicas = StreamMQK8sDefaults.AUTOSCALE_MAX_REPLICAS;
        private Integer targetLag = StreamMQK8sDefaults.AUTOSCALE_TARGET_LAG;
        private Integer scaleUpThreshold = StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_THRESHOLD;
        private Integer scaleDownThreshold = StreamMQK8sDefaults.AUTOSCALE_SCALE_DOWN_THRESHOLD;
        private Integer stabilizationWindowSeconds =
                StreamMQK8sDefaults.AUTOSCALE_STABILIZATION_WINDOW_SECONDS;
        private Integer scaleUpCooldownSeconds =
                StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_COOLDOWN_SECONDS;
        private Integer scaleDownCooldownSeconds =
                StreamMQK8sDefaults.AUTOSCALE_SCALE_DOWN_COOLDOWN_SECONDS;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Resources {
        private String cpuRequest = StreamMQK8sDefaults.RESOURCE_CPU_REQUEST;
        private String cpuLimit = StreamMQK8sDefaults.RESOURCE_CPU_LIMIT;
        private String memoryRequest = StreamMQK8sDefaults.RESOURCE_MEMORY_REQUEST;
        private String memoryLimit = StreamMQK8sDefaults.RESOURCE_MEMORY_LIMIT;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConsumerConfig {
        private Integer maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;
        private Long[] retryIntervals;
        private Long retryScanMs = StreamMQConstants.DEFAULT_PEL_CLAIM_SCAN_INTERVAL_MS;
        private Long delayScanMs = StreamMQConstants.DEFAULT_SCAN_INTERVAL_MS;
        private Integer pullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;
        private Long consumeTimeoutMs = StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConsumerGroupStatus {
        private String phase = StreamMQK8sDefaults.PHASE_PENDING;
        private Integer replicas = 0;
        private Integer readyReplicas = 0;
        private String lastUpdateTime;
        private Long observedGeneration;
    }
}
