package io.github.streammq.core.annotation;

import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.spi.MessageSerializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * StreamMQ 消费监听注解（方法级），标注在 {@code StreamMqListener} / {@code StreamMqAckListener} 实现方法上。
 *
 * <p>对齐 RocketMQ {@code @RocketMQMessageListener} 体验。
 *
 * <p>使用示例：
 * <pre>{@code
 * @Component
 * @StreamMqListener(topic = "order-topic", consumerGroup = "order-consumer-group")
 * public class OrderListener implements StreamMqListener<Order> {
 *     @Override
 *     public Action onMessage(Message<Order> message, ConsumerContext context) {
 *         processOrder(message.getBody());
 *         return Action.SUCCESS;
 *     }
 * }
 * }</pre>
 *
 * <p>注：本注解为类级别注解（与 RocketMQ 一致，标注在 Listener 实现类上）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMqListener {

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
     *
     * @return 消费模式
     */
    ConsumeMode consumeMode() default ConsumeMode.CLUSTERING;

    /**
     * 消息模型，默认 {@link MessageModel#CONCURRENT}。
     *
     * @return 消息模型
     */
    MessageModel messageModel() default MessageModel.CONCURRENT;

    /**
     * ACK 模式，默认 {@link AcknowledgeMode#AUTO}。
     *
     * @return ACK 模式
     */
    AcknowledgeMode acknowledgeMode() default AcknowledgeMode.AUTO;

    /**
     * 最小消费线程数，默认 1。
     *
     * @return 最小消费线程数
     */
    int consumeThreadMin() default 1;

    /**
     * 最大消费线程数，默认 64。
     *
     * @return 最大消费线程数
     */
    int consumeThreadMax() default 64;

    /**
     * 最大重试次数，默认 16。
     *
     * @return 最大重试次数
     */
    int maxReconsumeTimes() default 16;

    /**
     * 单条消息消费超时（毫秒），默认 30000（30 秒）。
     *
     * @return 超时毫秒数
     */
    long consumeTimeout() default 30000L;

    /**
     * Tag 过滤表达式（SQL92 风格子集），默认 "*" 表示全部接收。
     * 例如：{@code "tag1 || tag2"} / {@code "tag1 && tag2"}。
     *
     * @return 过滤表达式
     */
    String selectorExpression() default "*";

    /**
     * 序列化器实现类，默认使用全局配置。
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
     * 设置为 false 时仅注册但不启动 Listener。
     *
     * @return true 启用，false 仅注册
     */
    boolean enable() default true;
}
