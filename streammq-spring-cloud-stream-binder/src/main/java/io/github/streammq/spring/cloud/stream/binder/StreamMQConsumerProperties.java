package io.github.streammq.spring.cloud.stream.binder;

import org.springframework.cloud.stream.binder.ConsumerProperties;

/**
 * StreamMQ 消费者扩展属性。
 *
 * <p>继承 Spring Cloud Stream {@link ConsumerProperties}，额外暴露 StreamMQ 特有的消费端配置项。
 * 用户可通过 {@code spring.cloud.stream.streammq.bindings.<bindingName>.consumer.*} 前缀进行配置。
 *
 * <p>属性说明：
 * <ul>
 *   <li>{@code selectorExpression} - Tag 过滤表达式，默认 "*" 表示全部接收</li>
 *   <li>{@code selectorType} - 过滤类型，默认 "TAG"</li>
 *   <li>{@code shardCount} - 顺序消费分区数，默认 4</li>
 *   <li>{@code enableMsgTrace} - 是否启用消息追踪，默认 false</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQConsumerProperties extends ConsumerProperties {

    /** 默认 Tag 过滤表达式 */
    public static final String DEFAULT_SELECTOR_EXPRESSION = "*";

    /** 默认过滤类型 */
    public static final String DEFAULT_SELECTOR_TYPE = "TAG";

    /** 默认顺序消费分区数 */
    public static final int DEFAULT_SHARD_COUNT = 4;

    /** Tag 过滤表达式（SQL92 风格子集），"*" 表示全部接收 */
    private String selectorExpression = DEFAULT_SELECTOR_EXPRESSION;

    /** 过滤类型：TAG 或 SQL92 */
    private String selectorType = DEFAULT_SELECTOR_TYPE;

    /** 顺序消费分区数 */
    private int shardCount = DEFAULT_SHARD_COUNT;

    /** 是否启用消息追踪 */
    private boolean enableMsgTrace = false;

    /**
     * 返回 Tag 过滤表达式。
     *
     * @return 过滤表达式
     */
    public String getSelectorExpression() {
        return selectorExpression;
    }

    /**
     * 设置 Tag 过滤表达式。
     *
     * @param selectorExpression 过滤表达式
     */
    public void setSelectorExpression(String selectorExpression) {
        this.selectorExpression = selectorExpression;
    }

    /**
     * 返回过滤类型。
     *
     * @return 过滤类型
     */
    public String getSelectorType() {
        return selectorType;
    }

    /**
     * 设置过滤类型。
     *
     * @param selectorType 过滤类型
     */
    public void setSelectorType(String selectorType) {
        this.selectorType = selectorType;
    }

    /**
     * 返回顺序消费分区数。
     *
     * @return 分区数
     */
    public int getShardCount() {
        return shardCount;
    }

    /**
     * 设置顺序消费分区数。
     *
     * @param shardCount 分区数
     */
    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    /**
     * 返回是否启用消息追踪。
     *
     * @return true 表示启用追踪
     */
    public boolean isEnableMsgTrace() {
        return enableMsgTrace;
    }

    /**
     * 设置是否启用消息追踪。
     *
     * @param enableMsgTrace true 启用追踪
     */
    public void setEnableMsgTrace(boolean enableMsgTrace) {
        this.enableMsgTrace = enableMsgTrace;
    }
}
