/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.transaction;

/**
 * 事务消息示例常量：Topic、事务组与 tag。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "order-topic";

    /** 事务组（与 {@code StreamMQConstants.DEFAULT_TX_GROUP} 默认值一致） */
    public static final String TRANSACTION_GROUP = "default-tx-group";

    /** 示例消费组（OrderTransactionConsumer 使用，接收已提交的事务消息） */
    public static final String CONSUMER_GROUP = "transaction-order-consumer-group";

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
