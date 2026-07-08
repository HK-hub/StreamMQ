package io.github.streammq.amqp;

import io.github.streammq.core.converter.MessageConverter;
import org.redisson.api.RedissonClient;

import java.util.Objects;

/**
 * AMQP 兼容层配置类。
 *
 * <p>封装创建 {@link AmqpClient} 所需的核心依赖项，包括 Redisson 客户端、
 * {@link MessageConverter} 以及命名空间和默认 Exchange 等 AMQP 风格参数。
 *
 * <p>使用 Builder 模式构造：
 * <pre>{@code
 * AmqpConfig config = AmqpConfig.builder()
 *     .redissonClient(redisson)
 *     .namespace("my-ns")
 *     .defaultExchange("my-exchange")
 *     .build();
 * AmqpClient client = AmqpClient.create(config);
 * }</pre>
 *
 * <p>概念映射（AMQP → StreamMQ）：
 * <ul>
 *   <li>Exchange → Topic（发送目标）</li>
 *   <li>Queue → ConsumerGroup（消费组）</li>
 *   <li>Binding（Exchange → Queue）→ ConsumerGroup 订阅 Topic</li>
 *   <li>Routing Key → Tag（消息二级标签）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AmqpConfig {

    /** Redisson 客户端（必填），用于底层 Redis 操作 */
    private final RedissonClient redissonClient;

    /** 消息转换器（可选，为空时使用默认实现），负责 Message 与 Stream Entry 字段互转 */
    private final MessageConverter messageConverter;

    /** 命名空间（可选，默认空字符串），用于隔离不同环境的 Redis Key */
    private final String namespace;

    /** 默认 Exchange 名称（可选），{@link AmqpChannel#basicPublish} 时可省略 exchange 参数 */
    private final String defaultExchange;

    /**
     * 全参构造。
     *
     * @param redissonClient   Redisson 客户端
     * @param messageConverter 消息转换器
     * @param namespace        命名空间
     * @param defaultExchange  默认 Exchange
     * @throws NullPointerException 如果 redissonClient 为 null
     */
    private AmqpConfig(RedissonClient redissonClient, MessageConverter messageConverter,
                       String namespace, String defaultExchange) {
        this.redissonClient = Objects.requireNonNull(redissonClient, "redissonClient");
        this.messageConverter = messageConverter;
        this.namespace = (namespace != null) ? namespace : "";
        this.defaultExchange = (defaultExchange != null) ? defaultExchange : "";
    }

    /**
     * 返回 Redisson 客户端。
     *
     * @return Redisson 客户端实例
     */
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    /**
     * 返回消息转换器。
     *
     * @return 消息转换器，可能为 null
     */
    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    /**
     * 返回命名空间。
     *
     * @return 命名空间，不为 null
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * 返回默认 Exchange 名称。
     *
     * @return 默认 Exchange，不为 null
     */
    public String getDefaultExchange() {
        return defaultExchange;
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
     * {@link AmqpConfig} 的 Builder。
     */
    public static class Builder {
        private RedissonClient redissonClient;
        private MessageConverter messageConverter;
        private String namespace = "";
        private String defaultExchange = "";

        /**
         * 设置 Redisson 客户端（必填）。
         *
         * @param redissonClient Redisson 客户端
         * @return this
         */
        public Builder redissonClient(RedissonClient redissonClient) {
            this.redissonClient = redissonClient;
            return this;
        }

        /**
         * 设置消息转换器（可选）。
         *
         * @param messageConverter 消息转换器
         * @return this
         */
        public Builder messageConverter(MessageConverter messageConverter) {
            this.messageConverter = messageConverter;
            return this;
        }

        /**
         * 设置命名空间（可选，默认空字符串）。
         *
         * @param namespace 命名空间
         * @return this
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * 设置默认 Exchange 名称（可选）。
         *
         * @param defaultExchange 默认 Exchange
         * @return this
         */
        public Builder defaultExchange(String defaultExchange) {
            this.defaultExchange = defaultExchange;
            return this;
        }

        /**
         * 构建 {@link AmqpConfig} 实例。
         *
         * @return AmqpConfig 实例
         * @throws NullPointerException 如果 redissonClient 为 null
         */
        public AmqpConfig build() {
            return new AmqpConfig(redissonClient, messageConverter, namespace, defaultExchange);
        }
    }
}
