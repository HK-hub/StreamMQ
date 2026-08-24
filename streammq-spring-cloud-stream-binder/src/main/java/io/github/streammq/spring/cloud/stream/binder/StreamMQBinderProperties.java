package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.StreamMQConstants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * StreamMQ Binder 全局配置属性，绑定前缀 {@code spring.cloud.stream.streammq.binder}。
 *
 * <p>典型 {@code application.yml} 示例：
 *
 * <pre>{@code
 * spring:
 *   cloud:
 *     stream:
 *       streammq:
 *         binder:
 *           namespace: streammq
 *           send-timeout: 3000
 *           retry-times: 2
 *           consume-thread-min: 1
 *           consume-thread-max: 64
 *           max-reconsume-times: 16
 *           consume-timeout: 30000
 *           pull-batch-size: 32
 * }</pre>
 *
 * <p>这些属性为 Binder 全局默认值，可在 per-binding 级别通过 {@link StreamMQConsumerProperties} / {@link
 * StreamMQProducerProperties} 覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ConfigurationProperties(prefix = StreamMQBinderConstants.BINDER_PREFIX)
@Data
public class StreamMQBinderProperties {

    /**
     * 命名空间（用于多租户/多环境隔离）。
     *
     * <p>默认为空字符串，表示使用 {@code streammq.namespace} 配置的全局命名空间 （即 Listener 容器的
     * defaultNamespace）。设置非空值可让 Binder 使用独立命名空间， 但通常应保持为空以与生产者（{@link
     * io.github.streammq.core.template.StreamMessageTemplate}）一致。
     */
    private String namespace = "";

    /** 默认发送超时（毫秒） */
    private long sendTimeout = StreamMQConstants.DEFAULT_SEND_TIMEOUT_MS;

    /** 默认同步发送重试次数 */
    private int retryTimes = StreamMQConstants.DEFAULT_SYNC_RETRY_TIMES;

    /** 最小消费线程数 */
    private int consumeThreadMin = StreamMQConstants.DEFAULT_CONSUME_THREAD_MIN;

    /** 最大消费线程数 */
    private int consumeThreadMax = StreamMQConstants.DEFAULT_CONSUME_THREAD_MAX;

    /** 最大重试消费次数 */
    private int maxReconsumeTimes = StreamMQConstants.DEFAULT_MAX_RECONSUME_TIMES;

    /** 单条消息消费超时（毫秒） */
    private long consumeTimeout = StreamMQConstants.DEFAULT_CONSUME_TIMEOUT_MS;

    /** 单次拉取批量大小 */
    private int pullBatchSize = StreamMQConstants.DEFAULT_CONSUME_BATCH_SIZE;
}
