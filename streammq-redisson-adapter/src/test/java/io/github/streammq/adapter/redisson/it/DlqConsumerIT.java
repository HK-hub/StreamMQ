package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.container.DefaultStreamMqListenerContainer;
import io.github.streammq.adapter.redisson.consumer.RedissonStreamConsumerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMqKeys;
import io.github.streammq.core.annotation.StreamMqDlqListener;
import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.Action;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.MessageSerializer;
import io.github.streammq.core.spi.RebalanceStrategy;
import io.github.streammq.core.spi.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamCreateGroupArgs;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * DLQ 消费者集成测试。
 *
 * <p>验证完整链路：业务消息消费失败 → 进入 DLQ Stream → DLQ 消费者接收并处理。
 *
 * <p>覆盖场景：
 * <ul>
 *   <li>DLQ 消费者正常接收死信消息</li>
 *   <li>DLQ 消费者消费成功后 ACK（消息从 DLQ PEL 移除）</li>
 *   <li>DLQ 消费者使用自定义 dlqConsumerGroup</li>
 *   <li>DLQ 消费者消费失败后直接丢弃（不进入重试循环）</li>
 * </ul>
 */
@DisplayName("DLQ 消费者集成测试")
class DlqConsumerIT extends AbstractRedisIT {

    /**
     * 测试用快速重试策略，maxRetries=0 表示不重试，直接返回 null 触发 DLQ 路由。
     */
    static class NoRetryPolicy implements RetryPolicy {
        @Override
        public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
            return null;
        }

        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
            return true;
        }
    }

    /**
     * 通过动态代理构造 {@link io.github.streammq.core.annotation.StreamMqListener} 注解实例。
     */
    @SuppressWarnings("unchecked")
    private static io.github.streammq.core.annotation.StreamMqListener mkListenerAnnotation(
            String topic, String group, int maxReconsumeTimes) {
        return (io.github.streammq.core.annotation.StreamMqListener) Proxy.newProxyInstance(
            io.github.streammq.core.annotation.StreamMqListener.class.getClassLoader(),
            new Class<?>[]{io.github.streammq.core.annotation.StreamMqListener.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "topic" -> topic;
                case "consumerGroup" -> group;
                case "consumeMode" -> ConsumeMode.CLUSTERING;
                case "messageModel" -> MessageModel.CONCURRENT;
                case "acknowledgeMode" -> AcknowledgeMode.AUTO;
                case "maxReconsumeTimes" -> maxReconsumeTimes;
                case "consumeThreadMin" -> 1;
                case "consumeThreadMax" -> 64;
                case "consumeTimeout" -> 30000L;
                case "selectorExpression" -> "*";
                case "serializer" -> MessageSerializer.class;
                case "namespace" -> "";
                case "enable" -> true;
                case "selectorType" -> io.github.streammq.core.enums.SelectorType.TAG;
                case "pullBatchSize" -> 32;
                case "retryPolicy" -> RetryPolicy.class;
                case "enableMsgTrace" -> false;
                case "streamMaxLen" -> 0;
                case "messageConverter" -> MessageConverter.class;
                case "rebalanceStrategy" -> RebalanceStrategy.class;
                case "pullInterval" -> 0L;
                case "suspendCurrentQueueTimeMillis" -> 1000L;
                case "annotationType" -> io.github.streammq.core.annotation.StreamMqListener.class;
                case "hashCode" -> (topic + group).hashCode();
                case "equals" -> args != null && args.length > 0 && proxy == args[0];
                case "toString" -> "@StreamMqListener(topic=" + topic + ", consumerGroup=" + group + ")";
                default -> defaultAnnotationValue(method.getReturnType());
            });
    }

    /**
     * 通过动态代理构造 {@link StreamMqDlqListener} 注解实例。
     */
    @SuppressWarnings("unchecked")
    private static StreamMqDlqListener mkDlqAnnotation(
            String topic, String group, String dlqConsumerGroup, int maxReconsumeTimes) {
        return (StreamMqDlqListener) Proxy.newProxyInstance(
            StreamMqDlqListener.class.getClassLoader(),
            new Class<?>[]{StreamMqDlqListener.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "topic" -> topic;
                case "consumerGroup" -> group;
                case "dlqConsumerGroup" -> dlqConsumerGroup;
                case "namespace" -> "";
                case "consumerName" -> "";
                case "consumeThreadMin" -> 1;
                case "consumeThreadMax" -> 16;
                case "consumeTimeout" -> 30000L;
                case "pullBatchSize" -> 32;
                case "maxReconsumeTimes" -> maxReconsumeTimes;
                case "acknowledgeMode" -> AcknowledgeMode.AUTO;
                case "serializer" -> MessageSerializer.class;
                case "messageConverter" -> MessageConverter.class;
                case "enable" -> true;
                case "annotationType" -> StreamMqDlqListener.class;
                case "hashCode" -> (topic + group + dlqConsumerGroup).hashCode();
                case "equals" -> args != null && args.length > 0 && proxy == args[0];
                case "toString" -> "@StreamMqDlqListener(topic=" + topic + ", consumerGroup=" + group
                    + ", dlqConsumerGroup=" + dlqConsumerGroup + ")";
                default -> defaultAnnotationValue(method.getReturnType());
            });
    }

    /**
     * 根据返回类型返回注解属性的默认值，避免新增注解属性时测试代理崩溃。
     */
    private static Object defaultAnnotationValue(Class<?> returnType) {
        if (returnType == String.class) return "";
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == boolean.class) return false;
        if (returnType == Class.class) return null;
        if (returnType.isEnum()) return returnType.getEnumConstants()[0];
        return null;
    }

    @Test
    @DisplayName("DLQ 消费者接收死信消息：业务消息消费失败后进入 DLQ，DLQ 消费者成功消费")
    void dlqConsumer_receivesDeadLetterMessage() {
        String topic = "dlq-consume-topic";
        String group = "dlq-consume-group";
        String dlqConsumerGroup = "dlq-consumer-" + group;

        RetryPolicy noRetryPolicy = new NoRetryPolicy();
        RedissonStreamConsumerFactory consumerFactory = new RedissonStreamConsumerFactory(redisson, converter);
        DefaultStreamMqListenerContainer container =
            new DefaultStreamMqListenerContainer(redisson, consumerFactory, converter, noRetryPolicy, namespace);

        // 业务消费者：始终失败，触发 DLQ 路由
        io.github.streammq.core.listener.StreamMqListener<String> businessListener =
            (msg, ctx) -> { throw new RuntimeException("always fails, trigger DLQ"); };
        container.registerListener(businessListener, mkListenerAnnotation(topic, group, 0));

        // DLQ 消费者：接收死信消息
        AtomicReference<Message<?>> receivedDlqMessage = new AtomicReference<>();
        io.github.streammq.core.listener.StreamMqListener<String> dlqListener = (msg, ctx) -> {
            receivedDlqMessage.set(msg);
            return Action.SUCCESS;
        };
        container.registerDlqListener(dlqListener, mkDlqAnnotation(topic, group, dlqConsumerGroup, 0));

        // 创建消费者组：业务 Stream + DLQ Stream
        createConsumerGroup(topic, group);
        createDlqConsumerGroup(topic, group, dlqConsumerGroup);

        container.start();
        try {
            // 发送业务消息
            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("dlq-tag")
                .keys("dlq-key")
                .body("dlq-body")
                .build());
            producer.close();

            // 等待 DLQ 消费者接收消息
            await().atMost(20, TimeUnit.SECONDS).until(() -> receivedDlqMessage.get() != null);

            Message<?> dlqMsg = receivedDlqMessage.get();
            assertThat(dlqMsg).isNotNull();
            assertThat(dlqMsg.getBody()).isEqualTo("dlq-body");
            assertThat(dlqMsg.getTag()).isEqualTo("dlq-tag");
            assertThat(dlqMsg.getKeys()).isEqualTo("dlq-key");
            assertThat(dlqMsg.getMessageId()).isNotNull();

            // DLQ 消费成功后 ACK，DLQ PEL 应为空
            String dlqStreamKey = StreamMqKeys.dlqStream(namespace, topic, group);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                assertThat(dlqStream.listPending(dlqConsumerGroup,
                    StreamMessageId.MIN, StreamMessageId.MAX, 100)).isEmpty();
            });
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("DLQ 消费者使用默认 dlqConsumerGroup（dlq-consumer-{originalGroup}）")
    void dlqConsumer_defaultDlqConsumerGroup() {
        String topic = "dlq-default-group-topic";
        String group = "dlq-default-group";
        String expectedDlqGroup = "dlq-consumer-" + group;

        RetryPolicy noRetryPolicy = new NoRetryPolicy();
        RedissonStreamConsumerFactory consumerFactory = new RedissonStreamConsumerFactory(redisson, converter);
        DefaultStreamMqListenerContainer container =
            new DefaultStreamMqListenerContainer(redisson, consumerFactory, converter, noRetryPolicy, namespace);

        // 业务消费者：始终失败
        container.registerListener(
            (msg, ctx) -> { throw new RuntimeException("fail"); },
            mkListenerAnnotation(topic, group, 0));

        // DLQ 消费者：使用默认 dlqConsumerGroup（传空字符串）
        AtomicReference<Message<?>> receivedDlqMessage = new AtomicReference<>();
        container.registerDlqListener(
            (io.github.streammq.core.listener.StreamMqListener<String>) (msg, ctx) -> {
                receivedDlqMessage.set(msg);
                return Action.SUCCESS;
            },
            mkDlqAnnotation(topic, group, "", 0));

        createConsumerGroup(topic, group);
        createDlqConsumerGroup(topic, group, expectedDlqGroup);

        container.start();
        try {
            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("default-group-body").build());
            producer.close();

            await().atMost(20, TimeUnit.SECONDS).until(() -> receivedDlqMessage.get() != null);
            assertThat(receivedDlqMessage.get().getBody()).isEqualTo("default-group-body");
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("DLQ 消费者消费失败后直接丢弃：消息从 DLQ PEL 移除，不进入重试循环")
    void dlqConsumer_consumeFailure_dropsMessage() {
        String topic = "dlq-drop-topic";
        String group = "dlq-drop-group";
        String dlqConsumerGroup = "dlq-consumer-" + group;

        RetryPolicy noRetryPolicy = new NoRetryPolicy();
        RedissonStreamConsumerFactory consumerFactory = new RedissonStreamConsumerFactory(redisson, converter);
        DefaultStreamMqListenerContainer container =
            new DefaultStreamMqListenerContainer(redisson, consumerFactory, converter, noRetryPolicy, namespace);

        // 业务消费者：始终失败
        container.registerListener(
            (msg, ctx) -> { throw new RuntimeException("fail"); },
            mkListenerAnnotation(topic, group, 0));

        // DLQ 消费者：也始终失败（应被直接丢弃）
        java.util.concurrent.atomic.AtomicInteger dlqAttempts = new java.util.concurrent.atomic.AtomicInteger(0);
        container.registerDlqListener(
            (io.github.streammq.core.listener.StreamMqListener<String>) (msg, ctx) -> {
                dlqAttempts.incrementAndGet();
                throw new RuntimeException("DLQ consumer also fails");
            },
            mkDlqAnnotation(topic, group, dlqConsumerGroup, 0));

        createConsumerGroup(topic, group);
        createDlqConsumerGroup(topic, group, dlqConsumerGroup);

        container.start();
        try {
            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("drop-body").build());
            producer.close();

            // 等待 DLQ 消费者被调用至少一次
            await().atMost(20, TimeUnit.SECONDS).until(() -> dlqAttempts.get() >= 1);

            // DLQ 消费失败后应直接 ACK 丢弃，PEL 应为空
            String dlqStreamKey = StreamMqKeys.dlqStream(namespace, topic, group);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                assertThat(dlqStream.listPending(dlqConsumerGroup,
                    StreamMessageId.MIN, StreamMessageId.MAX, 100)).isEmpty();
            });
        } finally {
            container.stop();
        }
    }

    /**
     * 在 DLQ Stream 上创建消费者组。
     *
     * @param topic 原始主题
     * @param originalGroup 原始消费者组
     * @param dlqConsumerGroup DLQ 消费者组名
     */
    protected void createDlqConsumerGroup(String topic, String originalGroup, String dlqConsumerGroup) {
        String dlqStreamKey = StreamMqKeys.dlqStream(namespace, topic, originalGroup);
        RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
        try {
            dlqStream.createGroup(StreamCreateGroupArgs.name(dlqConsumerGroup).makeStream().id(new StreamMessageId(0, 0)));
        } catch (RuntimeException ex) {
            // BUSYGROUP 表示组已存在，忽略
            if (ex.getMessage() == null || !ex.getMessage().contains("BUSYGROUP")) {
                throw ex;
            }
        }
    }
}
