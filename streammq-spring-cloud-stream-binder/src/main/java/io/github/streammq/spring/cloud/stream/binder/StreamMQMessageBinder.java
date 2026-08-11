package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.util.StringUtils;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * StreamMQ Spring Cloud Stream Binder 核心实现。
 *
 * <p>桥接 Spring Cloud Stream Binder SPI 与 StreamMQ 生产/消费 API：
 *
 * <ul>
 *   <li>生产端：{@link #createProducerMessageHandler} 创建 {@link StreamMQMessageHandler}， 将 Spring
 *       Messaging 消息转换为 StreamMQ 消息并通过 {@link StreamMessageTemplate} 发送
 *   <li>消费端：{@link #createConsumerEndpoint} 创建 {@link StreamMQMessageProducer}， 注册 StreamMQ
 *       消费者并将收到的消息转换为 Spring Integration 消息输出
 * </ul>
 *
 * <p>实现 {@link ExtendedPropertiesBinder} 以支持 StreamMQ 特有的扩展属性：
 *
 * <ul>
 *   <li>消费者扩展属性 {@link StreamMQConsumerProperties}：selectorExpression / selectorType / shardCount /
 *       enableMsgTrace / concurrency / maxAttempts
 *   <li>生产者扩展属性 {@link StreamMQProducerProperties}：tag / keys / shardingKey / sendTimeout /
 *       retryTimes
 * </ul>
 *
 * <p>用户通过 {@code spring.cloud.stream.bindings.<bindingName>.binder: streammq} 指定使用本 Binder， 生产/消费的
 * StreamMQ 特有配置通过 {@code spring.cloud.stream.streammq.bindings.<bindingName>.producer.*} 或 {@code
 * spring.cloud.stream.streammq.bindings.<bindingName>.consumer.*} 在 per-binding 级别覆盖， 也可通过 {@code
 * spring.cloud.stream.streammq.default.*} 配置全局默认值。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
@Slf4j
public class StreamMQMessageBinder
        extends AbstractMessageChannelBinder<
                ExtendedConsumerProperties<StreamMQConsumerProperties>,
                ExtendedProducerProperties<StreamMQProducerProperties>,
                StreamMQMessageBinder.StreamMQProvisioningProvider>
        implements ExtendedPropertiesBinder<
                MessageChannel, StreamMQConsumerProperties, StreamMQProducerProperties> {

    /** StreamMQ 消息模板（生产端 API） */
    private final StreamMessageTemplate template;

    /** StreamMQ Listener 容器（消费端入口） */
    private final StreamMQListenerContainer listenerContainer;

    /** Binder 全局属性 */
    private final StreamMQBinderProperties binderProperties;

    /** 扩展绑定属性（per-binding 的 StreamMQ 特有属性） */
    private StreamMQExtendedBindingProperties extendedBindingProperties;

    /**
     * 构造 StreamMQ Binder。
     *
     * @param template StreamMQ 消息模板
     * @param listenerContainer StreamMQ Listener 容器
     * @param binderProperties Binder 全局属性
     */
    public StreamMQMessageBinder(
            StreamMessageTemplate template,
            StreamMQListenerContainer listenerContainer,
            StreamMQBinderProperties binderProperties) {
        super(new String[] {BinderHeaders.PARTITION_HEADER}, new StreamMQProvisioningProvider());
        this.template = Objects.requireNonNull(template, "template");
        this.listenerContainer = Objects.requireNonNull(listenerContainer, "listenerContainer");
        this.binderProperties = Objects.requireNonNull(binderProperties, "binderProperties");
    }

    /**
     * 注入扩展绑定属性，由 {@link StreamMQBinderConfiguration} 在创建 Binder 时调用。
     *
     * @param extendedBindingProperties 扩展绑定属性
     */
    public void setExtendedBindingProperties(
            StreamMQExtendedBindingProperties extendedBindingProperties) {
        this.extendedBindingProperties =
                Objects.requireNonNull(extendedBindingProperties, "extendedBindingProperties");
    }

    @Override
    public StreamMQConsumerProperties getExtendedConsumerProperties(String bindingName) {
        return this.extendedBindingProperties.getExtendedConsumerProperties(bindingName);
    }

    @Override
    public StreamMQProducerProperties getExtendedProducerProperties(String bindingName) {
        return this.extendedBindingProperties.getExtendedProducerProperties(bindingName);
    }

    @Override
    public String getDefaultsPrefix() {
        return this.extendedBindingProperties.getDefaultsPrefix();
    }

    @Override
    public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
        return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
    }

    @Override
    protected MessageHandler createProducerMessageHandler(
            ProducerDestination destination,
            ExtendedProducerProperties<StreamMQProducerProperties> producerProperties,
            MessageChannel errorChannel)
            throws Exception {
        StreamMQProducerProperties extension = producerProperties.getExtension();
        long sendTimeout =
                extension.getSendTimeout() > 0
                        ? extension.getSendTimeout()
                        : binderProperties.getSendTimeout();
        int retryTimes =
                extension.getRetryTimes() >= 0
                        ? extension.getRetryTimes()
                        : binderProperties.getRetryTimes();
        log.info(
                "创建 StreamMQ 生产者: destination={}, tag={}, shardingKey={}, sendTimeout={},"
                        + " retryTimes={}",
                destination.getName(),
                extension.getTag(),
                extension.getShardingKey(),
                sendTimeout,
                retryTimes);
        return new StreamMQMessageHandler(
                template, destination.getName(), extension, errorChannel, sendTimeout, retryTimes);
    }

    @Override
    protected MessageProducer createConsumerEndpoint(
            ConsumerDestination destination,
            String group,
            ExtendedConsumerProperties<StreamMQConsumerProperties> consumerProperties)
            throws Exception {
        StreamMQConsumerProperties extension = consumerProperties.getExtension();
        log.info(
                "创建 StreamMQ 消费者: destination={}, group={}, selectorExpression={}, shardCount={}",
                destination.getName(),
                group,
                extension.getSelectorExpression(),
                extension.getShardCount());
        return new StreamMQMessageProducer(
                listenerContainer, destination.getName(), group, extension, binderProperties);
    }

    /**
     * StreamMQ ProvisioningProvider 实现，提供生产/消费目的地的创建与查找。
     *
     * <p>StreamMQ 的目的地（Topic）无需预创建，由 Redis Stream 在首次写入时自动创建， 因此本实现仅返回目的地的名称包装对象，不执行实际的资源分配。
     *
     * @author StreamMQ Contributors
     * @since 0.1.0
     */
    public static class StreamMQProvisioningProvider
            implements ProvisioningProvider<
                    ExtendedConsumerProperties<StreamMQConsumerProperties>,
                    ExtendedProducerProperties<StreamMQProducerProperties>> {

        @Override
        public ProducerDestination provisionProducerDestination(
                String name, ExtendedProducerProperties<StreamMQProducerProperties> properties)
                throws ProvisioningException {
            if (StringUtils.isEmpty(name)) {
                throw new ProvisioningException("Producer destination name must not be empty");
            }
            return new StreamMQProducerDestination(name);
        }

        @Override
        public ConsumerDestination provisionConsumerDestination(
                String name,
                String group,
                ExtendedConsumerProperties<StreamMQConsumerProperties> properties)
                throws ProvisioningException {
            if (StringUtils.isEmpty(name)) {
                throw new ProvisioningException("Consumer destination name must not be empty");
            }
            return new StreamMQConsumerDestination(name);
        }
    }

    /** 生产者目的地实现。 */
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

    /** 消费者目的地实现。 */
    private record StreamMQConsumerDestination(String name) implements ConsumerDestination {
        @Override
        public String getName() {
            return name;
        }
    }
}
