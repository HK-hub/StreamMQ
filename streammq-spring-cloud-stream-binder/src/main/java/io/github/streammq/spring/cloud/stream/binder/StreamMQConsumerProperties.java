package io.github.streammq.spring.cloud.stream.binder;

/**
 * StreamMQ 消费者扩展属性（Plain POJO，不继承 {@link org.springframework.cloud.stream.binder.ConsumerProperties}）。
 *
 * <p>本类仅承载 StreamMQ 特有的消费端配置项，由 Spring Cloud Stream 的
 * {@link org.springframework.cloud.stream.binder.ExtendedConsumerProperties} 包装后
 * 传递给 {@link StreamMQMessageBinder}。
 *
 * <p>用户可通过 {@code spring.cloud.stream.streammq.bindings.<bindingName>.consumer.*} 前缀进行配置，
 * 或通过 {@code spring.cloud.stream.streammq.default.consumer.*} 配置全局默认值。
 *
 * <p>属性说明：
 * <ul>
 *   <li>{@code selectorExpression} - Tag 过滤表达式，默认 "*" 表示全部接收</li>
 *   <li>{@code selectorType} - 过滤类型，默认 "TAG"</li>
 *   <li>{@code shardCount} - 顺序消费分区数，默认 4</li>
 *   <li>{@code enableMsgTrace} - 是否启用消息追踪，默认 false</li>
 *   <li>{@code concurrency} - 消费线程数（覆盖 Binder 全局默认），默认 -1 表示使用全局值</li>
 *   <li>{@code maxAttempts} - 最大重试次数（覆盖 Binder 全局默认），默认 -1 表示使用全局值</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQConsumerProperties {

    /** 默认 Tag 过滤表达式 */
    public static final String DEFAULT_SELECTOR_EXPRESSION = "*";

    /** 默认过滤类型 */
    public static final String DEFAULT_SELECTOR_TYPE = "TAG";

    /** 默认顺序消费分区数 */
    public static final int DEFAULT_SHARD_COUNT = 4;

    /** 默认值标记：表示未设置，使用 Binder 全局默认值 */
    public static final int UNSET = -1;

    /** Tag 过滤表达式（SQL92 风格子集），"*" 表示全部接收 */
    private String selectorExpression = DEFAULT_SELECTOR_EXPRESSION;

    /** 过滤类型：TAG 或 SQL92 */
    private String selectorType = DEFAULT_SELECTOR_TYPE;

    /** 顺序消费分区数 */
    private int shardCount = DEFAULT_SHARD_COUNT;

    /** 是否启用消息追踪 */
    private boolean enableMsgTrace = false;

    /** 消费线程数（&lt;=0 表示使用 Binder 全局默认值） */
    private int concurrency = UNSET;

    /** 最大重试次数（&lt;=0 表示使用 Binder 全局默认值） */
    private int maxAttempts = UNSET;

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

    /**
     * 返回消费线程数。
     *
     * @return 消费线程数，&lt;=0 表示使用 Binder 全局默认值
     */
    public int getConcurrency() {
        return concurrency;
    }

    /**
     * 设置消费线程数。
     *
     * @param concurrency 消费线程数，&lt;=0 表示使用 Binder 全局默认值
     */
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    /**
     * 返回最大重试次数。
     *
     * @return 最大重试次数，&lt;=0 表示使用 Binder 全局默认值
     */
    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * 设置最大重试次数。
     *
     * @param maxAttempts 最大重试次数，&lt;=0 表示使用 Binder 全局默认值
     */
    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }
}
