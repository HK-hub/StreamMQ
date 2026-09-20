/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.dlq;

/**
 * 死信队列示例常量：Topic、消费组、tag 与用户属性 key。
 *
 * <p>命名空间不在本类声明：它只由全局配置 {@code streammq.namespace} 提供（见 {@code application.yml}）， 生产与消费（含 DLQ
 * 消费）统一从该配置继承，避免注解里的编译期常量与运行期命名空间分离。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
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

    /** 默认演示（DemoRunner）发送并强制失败的订单号：重试耗尽后进入 DLQ */
    public static final String DEMO_ORDER_ID = "dlq-demo-001";

    /** 示例 tag */
    public static final String TAG = "dlq-test";

    /** 用户属性 key：来源标识 */
    public static final String PROP_SOURCE = "source";

    /** 用户属性值：来源标识 */
    public static final String SOURCE = "dlq-sample";

    private SampleConstants() {}
}
