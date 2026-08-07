package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamMQMessageBinder} 单元测试。
 *
 * <p>使用 Mockito 模拟 {@link StreamMessageTemplate} 与 {@link StreamMQListenerContainer}，
 * 验证 Binder 能正确创建生产者与消费者：
 * <ul>
 *   <li>生产者：{@link StreamMQMessageBinder#createProducerMessageHandler} 返回的
 *       {@link StreamMQMessageHandler} 能正确将 Spring 消息转换为 StreamMQ 消息并发送</li>
 *   <li>消费者：{@link StreamMQMessageBinder#createConsumerEndpoint} 返回的
 *       {@link StreamMQMessageProducer} 能正确注册到 Listener 容器</li>
 *   <li>健康检查：{@link StreamMQBinderHealthIndicator} 能正确报告容器运行状态</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StreamMQMessageBinder 单元测试")
@SuppressWarnings({"rawtypes", "unchecked"})
class StreamMQMessageBinderTest {

    @Mock
    private StreamMessageTemplate template;

    @Mock
    private StreamMQListenerContainer listenerContainer;

    @Mock
    private ProducerDestination producerDestination;

    @Mock
    private ConsumerDestination consumerDestination;

    @Mock
    private ConsumeContext consumeContext;

    private StreamMQMessageBinder binder;

    @BeforeEach
    void setUp() {
        StreamMQBinderProperties binderProperties = new StreamMQBinderProperties();
        binder = new StreamMQMessageBinder(template, listenerContainer, binderProperties);
        binder.setExtendedBindingProperties(new StreamMQExtendedBindingProperties());
    }

    @Test
    @DisplayName("createProducerMessageHandler 返回 StreamMQMessageHandler 并正确发送消息")
    void createProducerMessageHandler_shouldSendViaTemplate() throws Exception {
        // Given
        when(producerDestination.getName()).thenReturn("test-topic");
        SendResult sendResult = new SendResult(new MessageId("1-0"), "test-topic", "tag1",
            SendStatus.SEND_OK, System.currentTimeMillis(), null, null);
        when(template.syncSend(any(), anyLong(), anyInt())).thenReturn(sendResult);

        StreamMQProducerProperties extension = new StreamMQProducerProperties();
        extension.setTag("tag1");
        extension.setShardingKey("key1");
        extension.setKeys("businessKey1");
        extension.setSendTimeout(5000);
        extension.setRetryTimes(3);
        ExtendedProducerProperties<StreamMQProducerProperties> producerProperties =
            new ExtendedProducerProperties<>(extension);

        // When
        MessageHandler handler = binder.createProducerMessageHandler(
            producerDestination, producerProperties, null);
        assertThat(handler).isInstanceOf(StreamMQMessageHandler.class);

        Map<String, Object> headers = new HashMap<>();
        headers.put("custom-header", "custom-value");
        org.springframework.messaging.Message<String> springMessage =
            new GenericMessage<>("hello", headers);
        handler.handleMessage(springMessage);

        // Then
        verify(template, times(1)).syncSend(any(io.github.streammq.core.message.Message.class),
            eq(5000L), eq(3));
    }

    @Test
    @DisplayName("createProducerMessageHandler 发送失败时抛出 MessagingException")
    void createProducerMessageHandler_shouldThrowOnSendFailure() throws Exception {
        // Given
        when(producerDestination.getName()).thenReturn("test-topic");
        when(template.syncSend(any(), anyLong(), anyInt()))
            .thenThrow(new RuntimeException("连接失败"));

        StreamMQProducerProperties extension = new StreamMQProducerProperties();
        ExtendedProducerProperties<StreamMQProducerProperties> producerProperties =
            new ExtendedProducerProperties<>(extension);

        // When
        MessageHandler handler = binder.createProducerMessageHandler(
            producerDestination, producerProperties, null);
        org.springframework.messaging.Message<String> springMessage =
            new GenericMessage<>("hello");

        // Then
        assertThatThrownBy(() -> handler.handleMessage(springMessage))
            .isInstanceOf(MessagingException.class);
    }

    @Test
    @DisplayName("createConsumerEndpoint 返回 StreamMQMessageProducer 并注册到容器")
    void createConsumerEndpoint_shouldRegisterConsumer() throws Exception {
        // Given
        String topic = "test-topic";
        String group = "test-group";
        when(consumerDestination.getName()).thenReturn(topic);

        StreamMQConsumerProperties extension = new StreamMQConsumerProperties();
        extension.setSelectorExpression("tag1 || tag2");
        extension.setShardCount(8);
        ExtendedConsumerProperties<StreamMQConsumerProperties> consumerProperties =
            new ExtendedConsumerProperties<>(extension);

        // When
        org.springframework.integration.core.MessageProducer producer =
            binder.createConsumerEndpoint(consumerDestination, group, consumerProperties);
        assertThat(producer).isInstanceOf(StreamMQMessageProducer.class);

        // 启动 Producer，触发注册
        ((StreamMQMessageProducer) producer).doStart();

        // Then
        verify(listenerContainer, times(1)).registerConsumer(
            any(StreamMQMessageProducer.class), any());
    }

    @Test
    @DisplayName("StreamMQMessageProducer onMessage 将 StreamMQ 消息转换为 Spring 消息")
    void messageProducer_onMessage_shouldConvertAndEmit() throws Exception {
        // Given
        when(consumerDestination.getName()).thenReturn("test-topic");
        StreamMQConsumerProperties extension = new StreamMQConsumerProperties();
        ExtendedConsumerProperties<StreamMQConsumerProperties> consumerProperties =
            new ExtendedConsumerProperties<>(extension);

        StreamMQMessageProducer producer = (StreamMQMessageProducer) binder.createConsumerEndpoint(
            consumerDestination, "test-group", consumerProperties);

        // 捕获输出的消息
        List<org.springframework.messaging.Message<?>> emitted = new java.util.ArrayList<>();
        org.springframework.messaging.MessageChannel outputChannel = (message, timeout) -> {
            emitted.add(message);
            return true;
        };
        producer.setOutputChannel(outputChannel);

        producer.doStart();

        // 构造 StreamMQ 消息
        Message<Object> streamMessage = MessageBuilder.<Object>withTopic("test-topic")
            .tag("tag1")
            .keys("key1")
            .shardingKey("shard1")
            .body("hello")
            .build();

        // When
        ConsumeAction action = producer.onMessage(streamMessage, consumeContext);

        // Then
        assertThat(action).isEqualTo(ConsumeAction.SUCCESS);
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).getPayload()).isEqualTo("hello");
        assertThat(emitted.get(0).getHeaders().get(StreamMQMessageHandler.HEADER_TOPIC))
            .isEqualTo("test-topic");
        assertThat(emitted.get(0).getHeaders().get(StreamMQMessageHandler.HEADER_TAG))
            .isEqualTo("tag1");
        assertThat(emitted.get(0).getHeaders().get(StreamMQMessageHandler.HEADER_KEYS))
            .isEqualTo("key1");
    }

    @Test
    @DisplayName("StreamMQMessageProducer onMessage 收到空消息返回 RECONSUME_LATER")
    void messageProducer_onMessage_nullMessage_shouldReconsume() throws Exception {
        when(consumerDestination.getName()).thenReturn("test-topic");
        StreamMQConsumerProperties extension = new StreamMQConsumerProperties();
        ExtendedConsumerProperties<StreamMQConsumerProperties> consumerProperties =
            new ExtendedConsumerProperties<>(extension);

        StreamMQMessageProducer producer = (StreamMQMessageProducer) binder.createConsumerEndpoint(
            consumerDestination, "test-group", consumerProperties);
        org.springframework.messaging.MessageChannel outputChannel = (message, timeout) -> {
            return true;
        };
        producer.setOutputChannel(outputChannel);
        producer.doStart();

        ConsumeAction action = producer.onMessage(null, consumeContext);
        assertThat(action).isEqualTo(ConsumeAction.RECONSUME_LATER);
    }

    @Test
    @DisplayName("StreamMQBinderHealthIndicator 容器运行中报告 UP")
    void healthIndicator_containerRunning_shouldReportUp() {
        when(listenerContainer.isRunning()).thenReturn(true);
        when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

        StreamMQBinderHealthIndicator indicator = new StreamMQBinderHealthIndicator(listenerContainer);
        org.springframework.boot.actuate.health.Health health = indicator.health();

        assertThat(health.getStatus())
            .isEqualTo(org.springframework.boot.actuate.health.Status.UP);
        assertThat(health.getDetails()).containsKey("listenerContainer.running");
        assertThat(health.getDetails()).containsKey("listenerContainer.consumerCount");
    }

    @Test
    @DisplayName("StreamMQBinderHealthIndicator 容器未运行报告 DOWN")
    void healthIndicator_containerNotRunning_shouldReportDown() {
        when(listenerContainer.isRunning()).thenReturn(false);
        when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

        StreamMQBinderHealthIndicator indicator = new StreamMQBinderHealthIndicator(listenerContainer);
        org.springframework.boot.actuate.health.Health health = indicator.health();

        assertThat(health.getStatus())
            .isEqualTo(org.springframework.boot.actuate.health.Status.DOWN);
    }

    @Test
    @DisplayName("StreamMQBinderProperties 默认值正确")
    void binderProperties_defaultsAreCorrect() {
        StreamMQBinderProperties props = new StreamMQBinderProperties();
        assertThat(props.getNamespace()).isEqualTo("");
        assertThat(props.getSendTimeout()).isEqualTo(3000L);
        assertThat(props.getRetryTimes()).isEqualTo(2);
        assertThat(props.getConsumeThreadMin()).isEqualTo(1);
        assertThat(props.getConsumeThreadMax()).isEqualTo(64);
        assertThat(props.getMaxReconsumeTimes()).isEqualTo(16);
        assertThat(props.getConsumeTimeout()).isEqualTo(30000L);
        assertThat(props.getPullBatchSize()).isEqualTo(32);
    }

    @Test
    @DisplayName("StreamMQConsumerProperties 默认值正确")
    void consumerProperties_defaultsAreCorrect() {
        StreamMQConsumerProperties props = new StreamMQConsumerProperties();
        assertThat(props.getSelectorExpression()).isEqualTo("*");
        assertThat(props.getSelectorType()).isEqualTo("TAG");
        assertThat(props.getShardCount()).isEqualTo(4);
        assertThat(props.isEnableMsgTrace()).isFalse();
        assertThat(props.getConcurrency()).isEqualTo(-1);
        assertThat(props.getMaxAttempts()).isEqualTo(-1);
    }

    @Test
    @DisplayName("StreamMQProducerProperties 默认值正确")
    void producerProperties_defaultsAreCorrect() {
        StreamMQProducerProperties props = new StreamMQProducerProperties();
        assertThat(props.getSendTimeout()).isEqualTo(-1);
        assertThat(props.getRetryTimes()).isEqualTo(-1);
        assertThat(props.getTag()).isNull();
        assertThat(props.getKeys()).isNull();
        assertThat(props.getShardingKey()).isNull();
    }

    @Test
    @DisplayName("getExtendedConsumerProperties 返回扩展消费者属性")
    void getExtendedConsumerProperties_shouldReturnExtension() {
        StreamMQConsumerProperties props = binder.getExtendedConsumerProperties("test-binding");
        assertThat(props).isNotNull();
        assertThat(props.getSelectorExpression()).isEqualTo("*");
    }

    @Test
    @DisplayName("getExtendedProducerProperties 返回扩展生产者属性")
    void getExtendedProducerProperties_shouldReturnExtension() {
        StreamMQProducerProperties props = binder.getExtendedProducerProperties("test-binding");
        assertThat(props).isNotNull();
        assertThat(props.getSendTimeout()).isEqualTo(-1);
    }
}
