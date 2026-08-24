package io.github.streammq.sample.dlq;

/**
 * 死信队列示例常量：Topic、消费组、命名空间、tag 与用户属性 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "order-topic";

    /** 主流程消费组 */
    public static final String CONSUMER_GROUP = "order-consumer-group";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "test-collector-group";

    /** DLQ 消费失败测试消费组 */
    public static final String TEST_FAIL_CONSUMER_GROUP = "test-fail-group";

    /** 示例命名空间 */
    public static final String NAMESPACE = "dlq";

    /** 示例 tag */
    public static final String TAG = "dlq-test";

    /** 用户属性 key：来源标识 */
    public static final String PROP_SOURCE = "source";

    /** 用户属性值：来源标识 */
    public static final String SOURCE = "dlq-sample";

    private SampleConstants() {}
}
