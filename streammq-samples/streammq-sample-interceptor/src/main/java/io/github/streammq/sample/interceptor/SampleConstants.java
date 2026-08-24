package io.github.streammq.sample.interceptor;

import io.github.streammq.core.StreamMQConstants;

/**
 * 拦截器示例常量：Topic、消费组、tag 与用户属性 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "interceptor-order-topic";

    /** 主流程消费组 */
    public static final String CONSUMER_GROUP = "interceptor-order-consumer-group";

    /** 集成测试消费组 */
    public static final String TEST_CONSUMER_GROUP = "test-interceptor-consumer-group";

    /** 示例 tag */
    public static final String TAG = "order";

    /** 用户属性 key：traceId（与框架追踪契约一致） */
    public static final String PROP_TRACE_ID = StreamMQConstants.TRACE_ATTR_TRACE_ID;

    /** 用户属性 key：spanId */
    public static final String PROP_SPAN_ID = "spanId";

    private SampleConstants() {}
}
