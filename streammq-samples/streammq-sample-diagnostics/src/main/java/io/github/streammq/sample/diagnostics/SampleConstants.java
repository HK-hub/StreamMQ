package io.github.streammq.sample.diagnostics;

/**
 * 诊断示例常量：Topic、消费组与 tag。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class SampleConstants {

    /** 示例 Topic */
    public static final String TOPIC = "order-events";

    /** 主流程消费组 */
    public static final String CONSUMER_GROUP = "diagnostics-sample-consumer";

    /** 示例 tag */
    public static final String TAG = "created";

    private SampleConstants() {}
}
