/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.orderly;

/**
 * 顺序消息示例常量：Topic、消费组与 tag。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "orderly-order-topic";

    /** 主流程消费组 */
    public static final String CONSUMER_GROUP = "orderly-order-consumer-group";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "test-orderly-consumer-group";

    /** 示例 tag */
    public static final String TAG = "orderly";

    /** 用户属性 key：消息序号 */
    public static final String PROP_SEQUENCE = "sequence";

    private SampleConstants() {}
}
