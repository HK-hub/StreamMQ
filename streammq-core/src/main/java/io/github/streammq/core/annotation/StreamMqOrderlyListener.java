package io.github.streammq.core.annotation;

import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.spi.MessageSerializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * StreamMQ 顺序消费专用监听注解。
 *
 * <p>语义上等价于 {@code @StreamMqListener(messageModel = MessageModel.ORDERLY)}，
 * 但强制 {@code messageModel = ORDERLY} 且仅允许标注在 {@code StreamMqOrderlyListener} 实现类上。
 *
 * <p>使用示例：
 * <pre>{@code
 * @Component
 * @StreamMqOrderlyListener(topic = "order-topic", consumerGroup = "order-consumer-group")
 * public class OrderOrderlyListener implements StreamMqOrderlyListener<Order> {
 *     @Override
 *     public Action onMessage(Message<Order> message, OrderlyContext context) {
 *         processOrder(message.getBody());
 *         return Action.SUCCESS;
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMqOrderlyListener {

    /**
     * 主题（必填）。
     *
     * @return 主题
     */
    String topic();

    /**
     * 消费者组名（必填）。
     *
     * @return 消费者组
     */
    String consumerGroup();

    /**
     * 消费模式，默认 {@link ConsumeMode#CLUSTERING}。
     * 顺序消费通常与 CLUSTERING 搭配，BROADCASTING 仅在广播场景下使用。
     *
     * @return 消费模式
     */
    ConsumeMode consumeMode() default ConsumeMode.CLUSTERING;

    /**
     * ACK 模式，顺序消费场景下默认 {@link AcknowledgeMode#AUTO}。
     *
     * @return ACK 模式
     */
    AcknowledgeMode acknowledgeMode() default AcknowledgeMode.AUTO;

    /**
     * 最小消费线程数，默认 1。
     * 顺序消费场景下建议保持 1，避免破坏顺序。
     *
     * @return 最小消费线程数
     */
    int consumeThreadMin() default 1;

    /**
     * 最大消费线程数，默认 1。
     * 顺序消费场景下保持 1 以保证顺序；分片场景下可适当增大（按 shard 维度并发）。
     *
     * @return 最大消费线程数
     */
    int consumeThreadMax() default 1;

    /**
     * 最大重试次数，默认 Integer.MAX_VALUE（顺序消费场景下默认无限重试，避免消息丢失）。
     *
     * @return 最大重试次数
     */
    int maxReconsumeTimes() default Integer.MAX_VALUE;

    /**
     * 单条消息消费超时（毫秒），默认 30000（30 秒）。
     *
     * @return 超时毫秒数
     */
    long consumeTimeout() default 30000L;

    /**
     * 最大 shard 数（顺序消费分区数），默认 4。
     *
     * @return shard 数
     */
    int shardCount() default 4;

    /**
     * Tag 过滤表达式，默认 {@code "*"} 表示接收所有 Tag。
     *
     * @return 过滤表达式
     */
    String selectorExpression() default "*";

    /**
     * 自定义序列化器类，默认 {@link MessageSerializer} 表示使用全局配置。
     *
     * @return 序列化器类
     */
    Class<? extends MessageSerializer<?>> serializer() default MessageSerializer.class;

    /**
     * 命名空间，默认使用全局配置。
     *
     * @return 命名空间
     */
    String namespace() default "";

    /**
     * 是否启用消费，默认 true。
     *
     * @return true 启用
     */
    boolean enable() default true;
}
