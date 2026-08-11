package io.github.streammq.spring.cloud.stream.binder;

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
@ConfigurationProperties(prefix = "spring.cloud.stream.streammq.binder")
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

    /** 默认发送超时（毫秒），默认 3000 */
    private long sendTimeout = 3000L;

    /** 默认同步发送重试次数，默认 2 */
    private int retryTimes = 2;

    /** 最小消费线程数，默认 1 */
    private int consumeThreadMin = 1;

    /** 最大消费线程数，默认 64 */
    private int consumeThreadMax = 64;

    /** 最大重试消费次数，默认 16 */
    private int maxReconsumeTimes = 16;

    /** 单条消息消费超时（毫秒），默认 30000 */
    private long consumeTimeout = 30000L;

    /** 单次拉取批量大小，默认 32 */
    private int pullBatchSize = 32;
}
