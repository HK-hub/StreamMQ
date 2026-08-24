package io.github.streammq.sample.delay;

/**
 * 延时消息示例常量：Topic、消费组、tag 与用户属性 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "delay-order-topic";

    /** 主流程消费组 */
    public static final String CONSUMER_GROUP = "delay-order-consumer-group";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "delay-order-consumer-group-it";

    /** 固定延时等级示例 tag */
    public static final String TAG_DELAY = "delay";

    /** 自定义延时示例 tag */
    public static final String TAG_CUSTOM_DELAY = "custom-delay";

    /** 用户属性 key：来源标识 */
    public static final String PROP_SOURCE = "source";

    /** 用户属性值：来源标识 */
    public static final String SOURCE = "delay-sample";

    private SampleConstants() {}
}
