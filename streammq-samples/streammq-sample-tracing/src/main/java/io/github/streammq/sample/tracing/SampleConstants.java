/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.tracing;

/**
 * 链路追踪示例常量：Topic、消费组、tag 与用户属性 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "tracing-events";

    /** 主流程消费组 */
    public static final String CONSUMER_GROUP = "tracing-sample-consumer";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "tracing-test-consumer";

    /** 示例 tag */
    public static final String TAG = "event";

    /** 用户属性 key：W3C traceparent（标准头，不可更名） */
    public static final String PROP_TRACEPARENT = "traceparent";

    private SampleConstants() {}
}
