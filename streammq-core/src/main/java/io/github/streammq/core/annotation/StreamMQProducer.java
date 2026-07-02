package io.github.streammq.core.annotation;

import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.MessageSerializer;

import java.lang.annotation.*;

/**
 * StreamMQ 生产者注入注解，标注在 {@code StreamMqTemplate} 字段上。
 *
 * <p>框架将根据注解参数创建对应的 Producer 实例并注入。
 *
 * <p>使用示例：
 * <pre>{@code
 * @Service
 * public class OrderService {
 *
 *     @StreamMqProducer(group = "order-producer-group")
 *     private StreamMqTemplate<String> template;
 *
 *     public void sendOrder(Order order) {
 *         Message<Order> msg = MessageBuilder.<Order>withTopic("order-topic")
 *             .withTag("created")
 *             .withKeys(order.getId())
 *             .withBody(order)
 *             .build();
 *         template.syncSend(msg);
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StreamMQProducer {

    /**
     * 生产者组名（必填），逻辑分组，用于监控与统计。
     *
     * @return 组名
     */
    String group();

    /**
     * 命名空间，默认使用全局配置。
     *
     * @return 命名空间
     */
    String namespace() default "";

    /**
     * 序列化器实现类，默认使用全局配置（JacksonJsonSerializer）。
     *
     * @return 序列化器类
     */
    Class<? extends MessageSerializer> serializer() default MessageSerializer.class;

    /**
     * 发送超时（毫秒），0 表示使用全局默认。
     *
     * @return 超时毫秒
     */
    long sendMessageTimeout() default 0L;

    /**
     * 同步发送重试次数，0 表示不重试，-1 表示使用全局默认。
     *
     * @return 重试次数
     */
    int retryTimes() default -1;

    /**
     * 生产者重试上限，-1 表示使用全局默认。
     *
     * @return 重试上限
     */
    int maxReconsumeTimes() default -1;

    /**
     * 是否启用消息追踪，默认 false。
     * 设置为 true 时将覆盖全局追踪开关，对该生产者单独启用追踪。
     *
     * @return true 启用追踪
     */
    boolean enableMsgTrace() default false;

    /**
     * Stream 最大长度（0=使用全局配置）。
     *
     * @return Stream 最大长度
     */
    int streamMaxLen() default 0;

    /**
     * 每个生产者专属消息转换器（默认表示使用全局）。
     *
     * <p>注：使用 raw type {@code Class<? extends MessageConverter>}，因为
     * {@code MessageConverter.class} 返回的是 raw type，无法直接用于泛型
     * {@code Class<? extends MessageConverter<?>>}。
     *
     * @return 消息转换器类
     */
    Class<? extends MessageConverter> messageConverter() default MessageConverter.class;
}
