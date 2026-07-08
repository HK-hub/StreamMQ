package io.github.streammq.amqp;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * AMQP 风格消息载体。
 *
 * <p>封装一条 AMQP 消息的完整信息，包括消息体（byte[]）、属性（headers）、
 * 路由键（routingKey）、Exchange 名称以及投递元数据（deliveryTag、redelivered）。
 *
 * <p>对应 RabbitMQ 客户端中的：
 * <ul>
 *   <li>{@code body} - 消息体字节数组</li>
 *   <li>{@code Envelope} - deliveryTag / exchange / routingKey / redeliver</li>
 *   <li>{@code AMQP.BasicProperties} - headers / contentType / contentEncoding 等</li>
 * </ul>
 *
 * <p>概念映射：
 * <ul>
 *   <li>exchange → StreamMQ Topic</li>
 *   <li>routingKey → StreamMQ Tag</li>
 *   <li>properties（headers）→ StreamMQ userProperties</li>
 * </ul>
 *
 * <p>该对象不可变（immutable），通过 Builder 构造。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AmqpMessage {

    /** 消息体原始字节数组 */
    private final byte[] body;

    /** 目标 Exchange 名称（对应 StreamMQ Topic） */
    private final String exchange;

    /** 路由键（对应 StreamMQ Tag） */
    private final String routingKey;

    /** 消息属性/头信息（不可变视图） */
    private final Map<String, Object> properties;

    /** 投递标签，Channel 内单调递增，用于手动 ACK */
    private final long deliveryTag;

    /** 是否重新投递 */
    private final boolean redelivered;

    /** 消费者标签 */
    private final String consumerTag;

    /**
     * 全参构造。
     *
     * @param body         消息体字节数组
     * @param exchange     Exchange 名称
     * @param routingKey   路由键
     * @param properties   消息属性
     * @param deliveryTag  投递标签
     * @param redelivered  是否重投
     * @param consumerTag  消费者标签
     */
    private AmqpMessage(byte[] body, String exchange, String routingKey,
                        Map<String, Object> properties, long deliveryTag,
                        boolean redelivered, String consumerTag) {
        this.body = body != null ? body.clone() : new byte[0];
        this.exchange = (exchange != null) ? exchange : "";
        this.routingKey = (routingKey != null) ? routingKey : "";
        this.properties = properties != null
                ? Collections.unmodifiableMap(new LinkedHashMap<>(properties))
                : Collections.emptyMap();
        this.deliveryTag = deliveryTag;
        this.redelivered = redelivered;
        this.consumerTag = (consumerTag != null) ? consumerTag : "";
    }

    /**
     * 返回消息体字节数组（防御性拷贝）。
     *
     * @return body 副本
     */
    public byte[] getBody() {
        return body.clone();
    }

    /**
     * 返回 Exchange 名称。
     *
     * @return exchange
     */
    public String getExchange() {
        return exchange;
    }

    /**
     * 返回路由键。
     *
     * @return routingKey
     */
    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * 返回消息属性（不可变视图）。
     *
     * @return properties Map
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * 返回投递标签。
     *
     * @return deliveryTag
     */
    public long getDeliveryTag() {
        return deliveryTag;
    }

    /**
     * 是否重新投递。
     *
     * @return true 如果是重投消息
     */
    public boolean isRedelivered() {
        return redelivered;
    }

    /**
     * 返回消费者标签。
     *
     * @return consumerTag
     */
    public String getConsumerTag() {
        return consumerTag;
    }

    @Override
    public String toString() {
        return "AmqpMessage{"
                + "exchange='" + exchange + '\''
                + ", routingKey='" + routingKey + '\''
                + ", bodyLength=" + body.length
                + ", deliveryTag=" + deliveryTag
                + ", redelivered=" + redelivered
                + ", consumerTag='" + consumerTag + '\''
                + ", propertiesSize=" + properties.size()
                + '}';
    }

    /**
     * 创建 Builder 实例。
     *
     * @return 新的 Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 便捷构造：创建仅含 body 的消息。
     *
     * @param body 消息体字节数组
     * @return AmqpMessage 实例
     */
    public static AmqpMessage of(byte[] body) {
        return builder().body(body).build();
    }

    /**
     * 便捷构造：创建含 body 和 routingKey 的消息。
     *
     * @param body       消息体字节数组
     * @param routingKey 路由键
     * @return AmqpMessage 实例
     */
    public static AmqpMessage of(byte[] body, String routingKey) {
        return builder().body(body).routingKey(routingKey).build();
    }

    /**
     * {@link AmqpMessage} 的 Builder。
     */
    public static class Builder {
        private byte[] body;
        private String exchange;
        private String routingKey;
        private final Map<String, Object> properties = new LinkedHashMap<>();
        private long deliveryTag;
        private boolean redelivered;
        private String consumerTag;

        /**
         * 设置消息体（必填）。
         *
         * @param body 字节数组
         * @return this
         */
        public Builder body(byte[] body) {
            this.body = body;
            return this;
        }

        /**
         * 设置 Exchange 名称。
         *
         * @param exchange Exchange 名称
         * @return this
         */
        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        /**
         * 设置路由键。
         *
         * @param routingKey 路由键
         * @return this
         */
        public Builder routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        /**
         * 添加一条消息属性。
         *
         * @param key   属性键
         * @param value 属性值
         * @return this
         */
        public Builder property(String key, Object value) {
            this.properties.put(
                    Objects.requireNonNull(key, "property key"),
                    value);
            return this;
        }

        /**
         * 批量设置消息属性（覆盖现有）。
         *
         * @param properties 属性 Map
         * @return this
         */
        public Builder properties(Map<String, Object> properties) {
            this.properties.clear();
            if (properties != null) {
                this.properties.putAll(properties);
            }
            return this;
        }

        /**
         * 设置投递标签。
         *
         * @param deliveryTag 投递标签
         * @return this
         */
        public Builder deliveryTag(long deliveryTag) {
            this.deliveryTag = deliveryTag;
            return this;
        }

        /**
         * 设置是否重投。
         *
         * @param redelivered 是否重投
         * @return this
         */
        public Builder redelivered(boolean redelivered) {
            this.redelivered = redelivered;
            return this;
        }

        /**
         * 设置消费者标签。
         *
         * @param consumerTag 消费者标签
         * @return this
         */
        public Builder consumerTag(String consumerTag) {
            this.consumerTag = consumerTag;
            return this;
        }

        /**
         * 构建 {@link AmqpMessage} 实例。
         *
         * @return AmqpMessage 实例
         * @throws NullPointerException 如果 body 为 null
         */
        public AmqpMessage build() {
            Objects.requireNonNull(body, "body must not be null");
            return new AmqpMessage(body, exchange, routingKey, properties,
                    deliveryTag, redelivered, consumerTag);
        }
    }
}
