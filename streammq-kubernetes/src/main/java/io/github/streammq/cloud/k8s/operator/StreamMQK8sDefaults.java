package io.github.streammq.cloud.k8s.operator;

import io.github.streammq.core.StreamMQConstants;

/**
 * StreamMQ Kubernetes Operator 默认值与协议常量。
 *
 * <p>集中管理 CRD 模型默认值、容器环境变量名、标签、端口与 CR 状态 phase， 供 {@link StreamMQCluster} / {@link
 * StreamMQTopic} / {@link StreamMQConsumerGroup} 与控制器共享， 避免同一默认值散落在多个模型中导致漂移。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class StreamMQK8sDefaults {

    // ==================== 副本与容量 ====================
    /** 后端存储类型：Redis */
    public static final String BACKEND_TYPE_REDIS = "redis";

    /** 默认消费者副本数 */
    public static final int DEFAULT_REPLICAS = 3;

    /** 默认 Topic 分区数 */
    public static final int DEFAULT_PARTITIONS = 8;

    /** 默认 Stream 最大长度 */
    public static final long DEFAULT_MAX_LENGTH = 1_000_000L;

    /** 默认保留时长（小时） */
    public static final int DEFAULT_RETENTION_HOURS = 72;

    /** 默认连接池大小 */
    public static final int DEFAULT_POOL_SIZE = 50;

    /** 默认连接超时（毫秒） */
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;

    /** 默认读取超时（毫秒） */
    public static final int DEFAULT_READ_TIMEOUT_MS =
            (int) StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    // ==================== 追踪 ====================
    /** 默认追踪采样率 */
    public static final double DEFAULT_SAMPLE_RATE = 0.1;

    /** 导出器类型：OTLP */
    public static final String EXPORTER_OTLP = "otlp";

    /** 导出器类型：Zipkin */
    public static final String EXPORTER_ZIPKIN = "zipkin";

    /** 导出器类型：Jaeger */
    public static final String EXPORTER_JAEGER = "jaeger";

    // ==================== 压缩 ====================
    /** 压缩算法：gzip（与 GzipCompressionCodec 的协议标识 "gzip" 保持一致） */
    public static final String COMPRESSION_GZIP = "gzip";

    /** 默认压缩阈值（字节） */
    public static final int DEFAULT_COMPRESS_THRESHOLD_BYTES = 1024;

    // ==================== 自动扩缩容 ====================
    /** 最小副本数默认值 */
    public static final int AUTOSCALE_MIN_REPLICAS = 1;

    /** 最大副本数默认值 */
    public static final int AUTOSCALE_MAX_REPLICAS = 10;

    /** 目标消费积压阈值默认值（每实例） */
    public static final int AUTOSCALE_TARGET_LAG = 100;

    /** 扩容阈值百分比默认值 */
    public static final int AUTOSCALE_SCALE_UP_THRESHOLD = 80;

    /** 缩容阈值百分比默认值 */
    public static final int AUTOSCALE_SCALE_DOWN_THRESHOLD = 20;

    /** 稳定窗口秒数默认值 */
    public static final int AUTOSCALE_STABILIZATION_WINDOW_SECONDS = 300;

    /** 扩容冷却时间（秒）默认值 */
    public static final int AUTOSCALE_SCALE_UP_COOLDOWN_SECONDS = 60;

    /** 缩容冷却时间（秒）默认值 */
    public static final int AUTOSCALE_SCALE_DOWN_COOLDOWN_SECONDS = 300;

    // ==================== k8s 资源量纲默认值 ====================
    /** CPU 请求默认值 */
    public static final String RESOURCE_CPU_REQUEST = "500m";

    /** CPU 限制默认值 */
    public static final String RESOURCE_CPU_LIMIT = "1000m";

    /** 内存请求默认值 */
    public static final String RESOURCE_MEMORY_REQUEST = "512Mi";

    /** 内存限制默认值 */
    public static final String RESOURCE_MEMORY_LIMIT = "1Gi";

    // ==================== CR 状态 Phase ====================
    /** Phase：等待处理 */
    public static final String PHASE_PENDING = "Pending";

    /** Phase：运行中 */
    public static final String PHASE_RUNNING = "Running";

    /** Phase：就绪 */
    public static final String PHASE_READY = "Ready";

    /** Phase：未就绪 */
    public static final String PHASE_NOT_READY = "NotReady";

    /** Phase：更新中 */
    public static final String PHASE_UPDATING = "Updating";

    /** Phase：失败 */
    public static final String PHASE_FAILED = "Failed";

    // ==================== 容器与工作负载 ====================
    /** 消费者容器名称 */
    public static final String CONTAINER_NAME = "streammq-consumer";

    /** 消费者镜像默认值 */
    public static final String DEFAULT_IMAGE = "streammq/streammq-consumer:latest";

    /** 镜像拉取策略 */
    public static final String PULL_POLICY_IF_NOT_PRESENT = "IfNotPresent";

    /** 容器端口：HTTP */
    public static final int PORT_HTTP = 8080;

    /** 容器端口：Metrics */
    public static final int PORT_METRICS = 9090;

    /** HTTP 端口名称 */
    public static final String PORT_NAME_HTTP = "http";

    /** Metrics 端口名称 */
    public static final String PORT_NAME_METRICS = "metrics";

    /** Operator 调度线程名 */
    public static final String THREAD_OPERATOR_SCHEDULER = "streammq-operator-scheduler";

    /** Operator 关闭等待时间（秒） */
    public static final long OPERATOR_AWAIT_TERMINATION_SECONDS = 10L;

    /** 默认调和间隔（秒） */
    public static final int DEFAULT_RECONCILE_INTERVAL_SECONDS = 30;

    /** Informer 全量重同步周期（毫秒，与调和间隔保持一致） */
    public static final long DEFAULT_RESYNC_PERIOD_MILLIS =
            DEFAULT_RECONCILE_INTERVAL_SECONDS * 1000L;

    // ==================== 注入容器的环境变量名 ====================
    /** 环境变量：集群名称 */
    public static final String ENV_CLUSTER_NAME = "STREAMMQ_CLUSTER_NAME";

    /** 环境变量：命名空间 */
    public static final String ENV_NAMESPACE = "STREAMMQ_NAMESPACE";

    /** 环境变量：Redis 地址 */
    public static final String ENV_REDIS_ADDRESS = "STREAMMQ_REDIS_ADDRESS";

    /** 环境变量：Redis 命名空间 */
    public static final String ENV_REDIS_NAMESPACE = "STREAMMQ_REDIS_NAMESPACE";

    /** 环境变量：Redis 密码 */
    public static final String ENV_REDIS_PASSWORD = "STREAMMQ_REDIS_PASSWORD";

    /** 环境变量：追踪开关 */
    public static final String ENV_TRACING_ENABLED = "STREAMMQ_TRACING_ENABLED";

    /** 环境变量：追踪导出器类型 */
    public static final String ENV_TRACING_EXPORTER = "STREAMMQ_TRACING_EXPORTER";

    /** 环境变量：追踪导出端点 */
    public static final String ENV_TRACING_ENDPOINT = "STREAMMQ_TRACING_ENDPOINT";

    // ==================== k8s 标签 ====================
    /** 标签 key：应用名（简写） */
    public static final String LABEL_APP = "app";

    /** 标签 key：推荐应用名 */
    public static final String LABEL_APP_K8S_NAME = "app.kubernetes.io/name";

    /** 标签 key：组件 */
    public static final String LABEL_APP_K8S_COMPONENT = "app.kubernetes.io/component";

    /** 标签 key：管理者 */
    public static final String LABEL_APP_K8S_MANAGED_BY = "app.kubernetes.io/managed-by";

    /** 标签值：应用名 */
    public static final String LABEL_VALUE_APP_NAME = "streammq";

    /** 标签值：组件名（消费者） */
    public static final String LABEL_VALUE_COMPONENT_CONSUMER = "consumer";

    /** 标签值：管理者 */
    public static final String LABEL_VALUE_MANAGED_BY = "streammq-operator";

    private StreamMQK8sDefaults() {}
}
