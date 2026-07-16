package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.util.StringUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.Objects;

/**
 * StreamMQ Spring Cloud Stream Binder 核心实现。
 *
 * <p>桥接 Spring Cloud Stream Binder SPI 与 StreamMQ 生产/消费 API：
 * <ul>
 *   <li>生产端：{@link #createProducerMessageHandler} 创建 {@link StreamMQMessageHandler}，
 *       将 Spring Messaging 消息转换为 StreamMQ 消息并通过 {@link StreamMessageTemplate} 发送</li>
 *   <li>消费端：{@link #createConsumerEndpoint} 创建 {@link StreamMQMessageProducer}，
 *       注册 StreamMQ 消费者并将收到的消息转换为 Spring Integration 消息输出</li>
 * </ul>
 *
 * <p>用户通过 {@code spring.cloud.stream.bindings.<bindingName>.binder: streammq} 指定使用本 Binder，
 * 生产/消费的 StreamMQ 特有配置通过 {@link StreamMQProducerProperties} /
 * {@link StreamMQConsumerProperties} 在 per-binding 级别覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@Slf4j
public class StreamMQMessageBinder
        extends AbstractMessageChannelBinder<StreamMQConsumerProperties, StreamMQProducerProperties,
                StreamMQMessageBinder.StreamMQProvisioningProvider> {

    /**
     * -- GETTER --
     *  返回 StreamMQ 消息模板。
     */
    private final StreamMessageTemplate template;

    /**
     * -- GETTER --
     *  返回 StreamMQ Listener 容器。
     */
    private final StreamMQListenerContainer listenerContainer;

    /**
     * -- GETTER --
     *  返回 Binder 全局属性。
     */
    private final StreamMQBinderProperties binderProperties;

    /**
     * 构造 StreamMQ Binder。
     *
     * @param template StreamMQ 消息模板
     * @param listenerContainer StreamMQ Listener 容器
     * @param binderProperties Binder 全局属性
     */
    public StreamMQMessageBinder(StreamMessageTemplate template,
                                 StreamMQListenerContainer listenerContainer,
                                 StreamMQBinderProperties binderProperties) {
        super(new String[] {BinderHeaders.PARTITION_HEADER}, new StreamMQProvisioningProvider());
        this.template = Objects.requireNonNull(template, "template");
        this.listenerContainer = Objects.requireNonNull(listenerContainer, "listenerContainer");
        this.binderProperties = Objects.requireNonNull(binderProperties, "binderProperties");
    }

    @Override
    protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
                                                          StreamMQProducerProperties producerProperties,
                                                          MessageChannel errorChannel) throws Exception {
        log.info("创建 StreamMQ 生产者: destination={}, tag={}, shardingKey={}, sendTimeout={}, retryTimes={}",
            destination.getName(), producerProperties.getTag(), producerProperties.getShardingKey(),
            producerProperties.getSendTimeout(), producerProperties.getRetryTimes());
        return new StreamMQMessageHandler(template, destination.getName(), producerProperties, errorChannel);
    }

    @Override
    protected MessageProducer createConsumerEndpoint(ConsumerDestination destination, String group,
                                                     StreamMQConsumerProperties consumerProperties) throws Exception {
        log.info("创建 StreamMQ 消费者: destination={}, group={}, selectorExpression={}, shardCount={}",
            destination.getName(), group, consumerProperties.getSelectorExpression(),
            consumerProperties.getShardCount());
        return new StreamMQMessageProducer(
            listenerContainer, destination.getName(), group, consumerProperties);
    }

    /**
     * StreamMQ ProvisioningProvider 实现，提供生产/消费目的地的创建与查找。
     *
     * <p>StreamMQ 的目的地（Topic）无需预创建，由 Redis Stream 在首次写入时自动创建，
     * 因此本实现仅返回目的地的名称包装对象，不执行实际的资源分配。
     *
     * @author StreamMQ Contributors
     * @since 0.1.0
     */
    public static class StreamMQProvisioningProvider
            implements ProvisioningProvider<StreamMQConsumerProperties, StreamMQProducerProperties> {

        @Override
        public ProducerDestination provisionProducerDestination(String name,
                                                                 StreamMQProducerProperties properties)
                throws ProvisioningException {
            if (StringUtils.isEmpty(name)) {
                throw new ProvisioningException("Producer destination name must not be empty");
            }
            return new StreamMQProducerDestination(name);
        }

        @Override
        public ConsumerDestination provisionConsumerDestination(String name, String group,
                                                                StreamMQConsumerProperties properties)
                throws ProvisioningException {
            if (StringUtils.isEmpty(name)) {
                throw new ProvisioningException("Consumer destination name must not be empty");
            }
            return new StreamMQConsumerDestination(name);
        }
    }

    /**
     * 生产者目的地实现。
     */
    private record StreamMQProducerDestination(String name) implements ProducerDestination {
        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getNameForPartition(int partition) {
            return name;
        }
    }

    /**
     * 消费者目的地实现。
     */
    private record StreamMQConsumerDestination(String name) implements ConsumerDestination {
        @Override
        public String getName() {
            return name;
        }
    }
}
