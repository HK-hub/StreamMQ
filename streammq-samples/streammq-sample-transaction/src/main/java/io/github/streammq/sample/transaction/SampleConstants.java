package io.github.streammq.sample.transaction;

/**
 * 事务消息示例常量：Topic、事务组与 tag。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "order-topic";

    /** 事务组（与 {@code StreamMQConstants.DEFAULT_TX_GROUP} 默认值一致） */
    public static final String TRANSACTION_GROUP = "default-tx-group";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "test-tx-consumer-group";

    /** 示例 tag */
    public static final String TAG = "transaction";

    /** 用户属性 key：业务类型 */
    public static final String PROP_BIZ_TYPE = "bizType";

    /** 用户属性值：下单业务 */
    public static final String BIZ_TYPE_ORDER_CREATE = "order-create";

    private SampleConstants() {}
}
