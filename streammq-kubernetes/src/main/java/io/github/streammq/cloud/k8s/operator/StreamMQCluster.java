package io.github.streammq.cloud.k8s.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.client.CustomResource;
import io.github.streammq.core.StreamMQConstants;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * StreamMQCluster CRD 自定义资源定义，用于管理 StreamMQ 集群配置。
 *
 * <p>通过 Kubernetes 自定义资源方式声明式管理 StreamMQ 集群配置，支持：
 *
 * <ul>
 *   <li>后端存储配置（Redis 地址、命名空间等）
 *   <li>消费重试、延时消费、链路追踪等核心功能开关
 *   <li>副本数、资源限制、自动扩缩容策略
 * </ul>
 *
 * <p>示例 YAML：
 *
 * <pre>{@code
 * apiVersion: streammq.io/v1
 * kind: StreamMQCluster
 * metadata:
 *   name: production-streammq
 * spec:
 *   backend:
 *     type: redis
 *     redis:
 *       address: "redis://redis-cluster:6379"
 *       namespace: "production"
 *   config:
 *     retry:
 *       max-reconsume-times: 16
 *     delay:
 *       enabled: true
 *     tracing:
 *       enabled: true
 *   replicas: 3
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StreamMQCluster extends CustomResource {

    /** StreamMQ 集群规格配置 */
    @JsonProperty("spec")
    private Spec spec;

    /** StreamMQ 集群状态（由 Operator 更新） */
    @JsonProperty("status")
    private Status status;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Spec {
        /** 后端存储配置 */
        private Backend backend;

        /** 功能配置 */
        private Config config;

        /** 消费者副本数 */
        private Integer replicas = StreamMQK8sDefaults.DEFAULT_REPLICAS;

        /** 资源限制与请求 */
        private Resources resources;

        /** 自动扩缩容配置 */
        private AutoScale autoScale;

        /** 额外标签 */
        private Map<String, String> labels;

        /** 额外注解 */
        private Map<String, String> annotations;

        // Getters and Setters
        public Backend getBackend() {
            return backend;
        }

        public void setBackend(Backend backend) {
            this.backend = backend;
        }

        public Config getConfig() {
            return config;
        }

        public void setConfig(Config config) {
            this.config = config;
        }

        public Integer getReplicas() {
            return replicas;
        }

        public void setReplicas(Integer replicas) {
            this.replicas = replicas;
        }

        public Resources getResources() {
            return resources;
        }

        public void setResources(Resources resources) {
            this.resources = resources;
        }

        public AutoScale getAutoScale() {
            return autoScale;
        }

        public void setAutoScale(AutoScale autoScale) {
            this.autoScale = autoScale;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        public void setLabels(Map<String, String> labels) {
            this.labels = labels;
        }

        public Map<String, String> getAnnotations() {
            return annotations;
        }

        public void setAnnotations(Map<String, String> annotations) {
            this.annotations = annotations;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Backend {
        /** 存储后端类型：redis */
        private String type = StreamMQK8sDefaults.BACKEND_TYPE_REDIS;

        /** Redis 连接配置 */
        private Redis redis;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Redis getRedis() {
            return redis;
        }

        public void setRedis(Redis redis) {
            this.redis = redis;
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Redis {
            /** Redis 连接地址（支持 sentinel://, redis://, rediss://） */
            private String address;

            /** 命名空间前缀 */
            private String namespace;

            /** 哨兵主节点名称（哨兵模式） */
            private String masterName;

            /** 连接密码（可选） */
            private String password;

            /** 连接池大小 */
            private Integer poolSize = StreamMQK8sDefaults.DEFAULT_POOL_SIZE;

            /** 连接超时（毫秒） */
            private Integer connectTimeoutMs = StreamMQK8sDefaults.DEFAULT_CONNECT_TIMEOUT_MS;

            /** 读取超时（毫秒） */
            private Integer readTimeoutMs = StreamMQK8sDefaults.DEFAULT_READ_TIMEOUT_MS;

            public String getAddress() {
                return address;
            }

            public void setAddress(String address) {
                this.address = address;
            }

            public String getNamespace() {
                return namespace;
            }

            public void setNamespace(String namespace) {
                this.namespace = namespace;
            }

            public String getMasterName() {
                return masterName;
            }

            public void setMasterName(String masterName) {
                this.masterName = masterName;
            }

            public String getPassword() {
                return password;
            }

            public void setPassword(String password) {
                this.password = password;
            }

            public Integer getPoolSize() {
                return poolSize;
            }

            public void setPoolSize(Integer poolSize) {
                this.poolSize = poolSize;
            }

            public Integer getConnectTimeoutMs() {
                return connectTimeoutMs;
            }

            public void setConnectTimeoutMs(Integer connectTimeoutMs) {
                this.connectTimeoutMs = connectTimeoutMs;
            }

            public Integer getReadTimeoutMs() {
                return readTimeoutMs;
            }

            public void setReadTimeoutMs(Integer readTimeoutMs) {
                this.readTimeoutMs = readTimeoutMs;
            }
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Config {
        /** 重试策略配置 */
        private Retry retry;

        /** 延时消费配置 */
        private Delay delay;

        /** 链路追踪配置 */
        private Tracing tracing;

        /** 消息压缩配置 */
        private Compression compression;

        public Retry getRetry() {
            return retry;
        }

        public void setRetry(Retry retry) {
            this.retry = retry;
        }

        public Delay getDelay() {
            return delay;
        }

        public void setDelay(Delay delay) {
            this.delay = delay;
        }

        public Tracing getTracing() {
            return tracing;
        }

        public void setTracing(Tracing tracing) {
            this.tracing = tracing;
        }

        public Compression getCompression() {
            return compression;
        }

        public void setCompression(Compression compression) {
            this.compression = compression;
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Retry {
            /** 最大重试次数 */
            private Integer maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

            /** 重试间隔毫秒数组（长度需与 maxReconsumeTimes 一致） */
            private Long[] retryIntervals;

            public Integer getMaxReconsumeTimes() {
                return maxReconsumeTimes;
            }

            public void setMaxReconsumeTimes(Integer maxReconsumeTimes) {
                this.maxReconsumeTimes = maxReconsumeTimes;
            }

            public Long[] getRetryIntervals() {
                return retryIntervals;
            }

            public void setRetryIntervals(Long[] retryIntervals) {
                this.retryIntervals = retryIntervals;
            }
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Delay {
            /** 是否启用延时消费（默认 true） */
            private Boolean enabled = true;

            /** 扫描间隔毫秒（默认 1000ms） */
            private Long scanIntervalMs = 1000L;

            public Boolean getEnabled() {
                return enabled;
            }

            public void setEnabled(Boolean enabled) {
                this.enabled = enabled;
            }

            public Long getScanIntervalMs() {
                return scanIntervalMs;
            }

            public void setScanIntervalMs(Long scanIntervalMs) {
                this.scanIntervalMs = scanIntervalMs;
            }
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Tracing {
            /** 是否启用链路追踪 */
            private Boolean enabled = false;

            /** 采样率 0.0-1.0 */
            private Double sampleRate = StreamMQK8sDefaults.DEFAULT_SAMPLE_RATE;

            /** 导出器类型：otlp, zipkin, jaeger */
            private String exporter = StreamMQK8sDefaults.EXPORTER_OTLP;

            /** 导出器端点地址 */
            private String endpoint;

            public Boolean getEnabled() {
                return enabled;
            }

            public void setEnabled(Boolean enabled) {
                this.enabled = enabled;
            }

            public Double getSampleRate() {
                return sampleRate;
            }

            public void setSampleRate(Double sampleRate) {
                this.sampleRate = sampleRate;
            }

            public String getExporter() {
                return exporter;
            }

            public void setExporter(String exporter) {
                this.exporter = exporter;
            }

            public String getEndpoint() {
                return endpoint;
            }

            public void setEndpoint(String endpoint) {
                this.endpoint = endpoint;
            }
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Compression {
            /** 是否启用压缩 */
            private Boolean enabled = false;

            /** 压缩算法：gzip, lz4, snappy, zstd */
            private String algorithm = StreamMQK8sDefaults.COMPRESSION_GZIP;

            /** 压缩阈值（字节），超过此大小触发压缩 */
            private Integer threshold = StreamMQK8sDefaults.DEFAULT_COMPRESS_THRESHOLD_BYTES;

            public Boolean getEnabled() {
                return enabled;
            }

            public void setEnabled(Boolean enabled) {
                this.enabled = enabled;
            }

            public String getAlgorithm() {
                return algorithm;
            }

            public void setAlgorithm(String algorithm) {
                this.algorithm = algorithm;
            }

            public Integer getThreshold() {
                return threshold;
            }

            public void setThreshold(Integer threshold) {
                this.threshold = threshold;
            }
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Resources {
        /** CPU 请求核数（如 "500m"） */
        private String cpuRequest = StreamMQK8sDefaults.RESOURCE_CPU_REQUEST;

        /** CPU 限制核数（如 "1000m"） */
        private String cpuLimit = StreamMQK8sDefaults.RESOURCE_CPU_LIMIT;

        /** 内存请求（如 "512Mi"） */
        private String memoryRequest = StreamMQK8sDefaults.RESOURCE_MEMORY_REQUEST;

        /** 内存限制（如 "1Gi"） */
        private String memoryLimit = StreamMQK8sDefaults.RESOURCE_MEMORY_LIMIT;

        public String getCpuRequest() {
            return cpuRequest;
        }

        public void setCpuRequest(String cpuRequest) {
            this.cpuRequest = cpuRequest;
        }

        public String getCpuLimit() {
            return cpuLimit;
        }

        public void setCpuLimit(String cpuLimit) {
            this.cpuLimit = cpuLimit;
        }

        public String getMemoryRequest() {
            return memoryRequest;
        }

        public void setMemoryRequest(String memoryRequest) {
            this.memoryRequest = memoryRequest;
        }

        public String getMemoryLimit() {
            return memoryLimit;
        }

        public void setMemoryLimit(String memoryLimit) {
            this.memoryLimit = memoryLimit;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AutoScale {
        /** 是否启用自动扩缩容 */
        private Boolean enabled = false;

        /** 最小副本数 */
        private Integer minReplicas = StreamMQK8sDefaults.AUTOSCALE_MIN_REPLICAS;

        /** 最大副本数 */
        private Integer maxReplicas = StreamMQK8sDefaults.AUTOSCALE_MAX_REPLICAS;

        /** 目标消费积压阈值（每实例积压消息数） */
        private Integer targetLag = StreamMQK8sDefaults.AUTOSCALE_TARGET_LAG;

        /** 扩容阈值百分比 */
        private Integer scaleUpThreshold = StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_THRESHOLD;

        /** 缩容阈值百分比 */
        private Integer scaleDownThreshold = StreamMQK8sDefaults.AUTOSCALE_SCALE_DOWN_THRESHOLD;

        /** 稳定窗口秒数（防抖动） */
        private Integer stabilizationWindowSeconds =
                StreamMQK8sDefaults.AUTOSCALE_STABILIZATION_WINDOW_SECONDS;

        /** 扩容冷却时间（秒） */
        private Integer scaleUpCooldownSeconds =
                StreamMQK8sDefaults.AUTOSCALE_SCALE_UP_COOLDOWN_SECONDS;

        /** 缩容冷却时间（秒） */
        private Integer scaleDownCooldownSeconds =
                StreamMQK8sDefaults.AUTOSCALE_SCALE_DOWN_COOLDOWN_SECONDS;

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public Integer getMinReplicas() {
            return minReplicas;
        }

        public void setMinReplicas(Integer minReplicas) {
            this.minReplicas = minReplicas;
        }

        public Integer getMaxReplicas() {
            return maxReplicas;
        }

        public void setMaxReplicas(Integer maxReplicas) {
            this.maxReplicas = maxReplicas;
        }

        public Integer getTargetLag() {
            return targetLag;
        }

        public void setTargetLag(Integer targetLag) {
            this.targetLag = targetLag;
        }

        public Integer getScaleUpThreshold() {
            return scaleUpThreshold;
        }

        public void setScaleUpThreshold(Integer scaleUpThreshold) {
            this.scaleUpThreshold = scaleUpThreshold;
        }

        public Integer getScaleDownThreshold() {
            return scaleDownThreshold;
        }

        public void setScaleDownThreshold(Integer scaleDownThreshold) {
            this.scaleDownThreshold = scaleDownThreshold;
        }

        public Integer getStabilizationWindowSeconds() {
            return stabilizationWindowSeconds;
        }

        public void setStabilizationWindowSeconds(Integer stabilizationWindowSeconds) {
            this.stabilizationWindowSeconds = stabilizationWindowSeconds;
        }

        public Integer getScaleUpCooldownSeconds() {
            return scaleUpCooldownSeconds;
        }

        public void setScaleUpCooldownSeconds(Integer scaleUpCooldownSeconds) {
            this.scaleUpCooldownSeconds = scaleUpCooldownSeconds;
        }

        public Integer getScaleDownCooldownSeconds() {
            return scaleDownCooldownSeconds;
        }

        public void setScaleDownCooldownSeconds(Integer scaleDownCooldownSeconds) {
            this.scaleDownCooldownSeconds = scaleDownCooldownSeconds;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Status {
        /** 集群状态：Pending, Ready, NotReady, Updating, Failed */
        private String phase = StreamMQK8sDefaults.PHASE_PENDING;

        /** 当前副本数 */
        private Integer replicas = 0;

        /** 就绪副本数 */
        private Integer readyReplicas = 0;

        /** 最后更新时间 */
        private String lastUpdateTime;

        /** 条件列表 */
        private List<Condition> conditions;

        /** 最后观察到的配置生成版本 */
        private Long observedGeneration;

        public String getPhase() {
            return phase;
        }

        public void setPhase(String phase) {
            this.phase = phase;
        }

        public Integer getReplicas() {
            return replicas;
        }

        public void setReplicas(Integer replicas) {
            this.replicas = replicas;
        }

        public Integer getReadyReplicas() {
            return readyReplicas;
        }

        public void setReadyReplicas(Integer readyReplicas) {
            this.readyReplicas = readyReplicas;
        }

        public String getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(String lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public List<Condition> getConditions() {
            return conditions;
        }

        public void setConditions(List<Condition> conditions) {
            this.conditions = conditions;
        }

        public Long getObservedGeneration() {
            return observedGeneration;
        }

        public void setObservedGeneration(Long observedGeneration) {
            this.observedGeneration = observedGeneration;
        }

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static class Condition {
            private String type;
            private String status;
            private String reason;
            private String message;
            private String lastTransitionTime;
        }
    }
}
