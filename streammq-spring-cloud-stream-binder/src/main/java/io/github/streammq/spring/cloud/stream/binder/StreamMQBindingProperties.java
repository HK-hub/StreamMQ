package io.github.streammq.spring.cloud.stream.binder;

import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * StreamMQ Binding 级别扩展属性容器，持有 per-binding 的消费者与生产者扩展属性。
 *
 * <p>对应配置前缀 {@code spring.cloud.stream.streammq.bindings.<bindingName>}，
 * 下设 {@code consumer.*} 与 {@code producer.*} 两个子节点，分别绑定到
 * {@link StreamMQConsumerProperties} 与 {@link StreamMQProducerProperties}。
 *
 * <p>典型 {@code application.yml} 示例：
 * <pre>{@code
 * spring:
 *   cloud:
 *     stream:
 *       streammq:
 *         bindings:
 *           myBinding-in-0:
 *             consumer:
 *               selectorExpression: "tag1 || tag2"
 *               shardCount: 8
 *           myBinding-out-0:
 *             producer:
 *               tag: order
 *               shardingKey: orderId
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQBindingProperties implements BinderSpecificPropertiesProvider {

    /** 消费者扩展属性 */
    private StreamMQConsumerProperties consumer = new StreamMQConsumerProperties();

    /** 生产者扩展属性 */
    private StreamMQProducerProperties producer = new StreamMQProducerProperties();

    @Override
    public Object getConsumer() {
        return consumer;
    }

    @Override
    public Object getProducer() {
        return producer;
    }

    /**
     * 设置消费者扩展属性。
     *
     * @param consumer 消费者扩展属性
     */
    public void setConsumer(StreamMQConsumerProperties consumer) {
        this.consumer = consumer;
    }

    /**
     * 设置生产者扩展属性。
     *
     * @param producer 生产者扩展属性
     */
    public void setProducer(StreamMQProducerProperties producer) {
        this.producer = producer;
    }
}
