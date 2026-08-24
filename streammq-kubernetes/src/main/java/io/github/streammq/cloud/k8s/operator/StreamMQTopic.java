package io.github.streammq.cloud.k8s.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.client.CustomResource;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * StreamMQTopic CRD for managing topic lifecycle in StreamMQ.
 *
 * <p>Declarative topic management through Kubernetes native resources:
 *
 * <ul>
 *   <li>Create/update/delete topics with partition count and retention settings
 *   <li>Associate topics with StreamMQCluster through clusterRef
 *   <li>Supports max-length and time-based retention policies
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
public class StreamMQTopic extends CustomResource {

    @JsonProperty("spec")
    private StreamMQTopicSpec spec;

    @JsonProperty("status")
    private StreamMQTopicStatus status;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StreamMQTopicSpec {
        private String clusterRef;
        private Integer partitions = StreamMQK8sDefaults.DEFAULT_PARTITIONS;
        private Long maxLength = StreamMQK8sDefaults.DEFAULT_MAX_LENGTH;
        private Integer retentionHours = StreamMQK8sDefaults.DEFAULT_RETENTION_HOURS;
        private Integer replicas = 1;
        private String compression;
        private Boolean deduplicationEnabled = false;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StreamMQTopicStatus {
        private String phase = StreamMQK8sDefaults.PHASE_PENDING;
        private Integer partitions = 0;
        private Long currentSize = 0L;
        private String lastUpdateTime;
        private Long observedGeneration;
    }
}
