package io.github.streammq.spring.cloud.stream.binder;

import org.springframework.cloud.stream.binder.ProducerProperties;

/**
 * StreamMQ 生产者扩展属性。
 *
 * <p>继承 Spring Cloud Stream {@link ProducerProperties}，额外暴露 StreamMQ 特有的生产端配置项。
 * 用户可通过 {@code spring.cloud.stream.streammq.bindings.<bindingName>.producer.*} 前缀进行配置。
 *
 * <p>属性说明：
 * <ul>
 *   <li>{@code tag} - 消息标签，默认 null</li>
 *   <li>{@code keys} - 业务键，默认 null</li>
 *   <li>{@code shardingKey} - 分片键（顺序消息路由），默认 null</li>
 *   <li>{@code sendTimeout} - 发送超时（毫秒），默认 3000</li>
 *   <li>{@code retryTimes} - 同步发送重试次数，默认 2</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQProducerProperties extends ProducerProperties {

    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_SEND_TIMEOUT = 3000L;

    /** 默认重试次数 */
    public static final int DEFAULT_RETRY_TIMES = 2;

    /** 消息标签 */
    private String tag;

    /** 业务键 */
    private String keys;

    /** 分片键（顺序消息路由） */
    private String shardingKey;

    /** 发送超时（毫秒） */
    private long sendTimeout = DEFAULT_SEND_TIMEOUT;

    /** 同步发送重试次数 */
    private int retryTimes = DEFAULT_RETRY_TIMES;

    /**
     * 返回消息标签。
     *
     * @return 标签，可能为 null
     */
    public String getTag() {
        return tag;
    }

    /**
     * 设置消息标签。
     *
     * @param tag 标签
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    /**
     * 返回业务键。
     *
     * @return 业务键，可能为 null
     */
    public String getKeys() {
        return keys;
    }

    /**
     * 设置业务键。
     *
     * @param keys 业务键
     */
    public void setKeys(String keys) {
        this.keys = keys;
    }

    /**
     * 返回分片键。
     *
     * @return 分片键，可能为 null
     */
    public String getShardingKey() {
        return shardingKey;
    }

    /**
     * 设置分片键。
     *
     * @param shardingKey 分片键
     */
    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    /**
     * 返回发送超时（毫秒）。
     *
     * @return 超时毫秒数
     */
    public long getSendTimeout() {
        return sendTimeout;
    }

    /**
     * 设置发送超时（毫秒）。
     *
     * @param sendTimeout 超时毫秒数
     */
    public void setSendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    /**
     * 返回同步发送重试次数。
     *
     * @return 重试次数
     */
    public int getRetryTimes() {
        return retryTimes;
    }

    /**
     * 设置同步发送重试次数。
     *
     * @param retryTimes 重试次数
     */
    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }
}
