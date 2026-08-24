package io.github.streammq.sample.quickstart;

/**
 * QuickStart 示例常量：Topic、消费组与消息 tag。
 *
 * <p>生产者（{@code OrderProducer}）、消费者（{@code OrderConsumer}） 与集成测试共享同一组取值，避免各处硬编码导致收发不一致。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "order-topic";

    /** 示例消费组（主流程） */
    public static final String CONSUMER_GROUP = "order-consumer-group";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "test-consumer-group";

    /** 同步发送示例 tag */
    public static final String TAG_CREATED = "created";

    /** 异步发送示例 tag */
    public static final String TAG_ASYNC = "async";

    /** 回调发送示例 tag */
    public static final String TAG_CALLBACK = "callback";

    /** 单向发送示例 tag */
    public static final String TAG_ONEWAY = "oneway";

    /** 批量发送示例 tag */
    public static final String TAG_BATCH = "batch";

    /** 简化批量 API 示例 tag */
    public static final String TAG_SIMPLE_BATCH = "simple-batch";

    /** 元数据模式示例 tag */
    public static final String TAG_METADATA = "metadata";

    /** 超时重试模式示例 tag */
    public static final String TAG_TIMEOUT_RETRY = "timeout-retry";

    /** 用户属性 key：来源标识 */
    public static final String PROP_SOURCE = "source";

    /** 用户属性值：来源标识 */
    public static final String SOURCE = "quickstart-sample";

    private SampleConstants() {}
}
