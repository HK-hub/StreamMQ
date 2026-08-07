package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.DlqFailureDecision;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.RetryPolicy;
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
 *   <li>DLQ 消费者使用 dlqMode = true 标识</li>
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
     * 通过动态代理构造 {@link StreamMQConsumer} 注解实例（非 DLQ 模式）。
     */
    @SuppressWarnings("unchecked")
    private static StreamMQConsumer mkListenerAnnotation(
            String topic, String group, int maxReconsumeTimes) {
        return mkListenerAnnotation(topic, group, maxReconsumeTimes, false);
    }

    /**
     * 通过动态代理构造 {@link StreamMQConsumer} 注解实例。
     *
     * @param topic 主题
     * @param group 消费者组（DLQ 模式下为原始消费者组，用于构造 DLQ Stream Key）
     * @param maxReconsumeTimes 最大重试次数
     * @param dlqMode 是否为 DLQ 消费者
     * @return 注解代理实例
     */
    @SuppressWarnings("unchecked")
    private static StreamMQConsumer mkListenerAnnotation(
            String topic, String group, int maxReconsumeTimes, boolean dlqMode) {
        return (StreamMQConsumer) Proxy.newProxyInstance(
            StreamMQConsumer.class.getClassLoader(),
            new Class<?>[]{StreamMQConsumer.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "topic" -> topic;
                case "consumerGroup" -> group;
                case "consumeMode" -> ConsumeMode.CLUSTERING;
                case "messageModel" -> MessageModel.CONCURRENT;
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
                case "shardCount" -> 4;
                case "dlqMode" -> dlqMode;
                case "consumerName" -> "";
                case "annotationType" -> StreamMQConsumer.class;
                case "hashCode" -> (topic + group + dlqMode).hashCode();
                case "equals" -> args != null && args.length > 0 && proxy == args[0];
                case "toString" -> "@StreamMQConsumer(topic=" + topic + ", consumerGroup=" + group
                    + ", dlqMode=" + dlqMode + ")";
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

        RetryPolicy noRetryPolicy = new NoRetryPolicy();
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, noRetryPolicy, namespace);

        // 业务消费者：始终失败，触发 DLQ 路由
        StreamMessageConcurrentlyConsumer<String> businessListener =
            (msg, ctx) -> { throw new RuntimeException("always fails, trigger DLQ"); };
        container.registerConsumer(businessListener, mkListenerAnnotation(topic, group, 0));

        // DLQ 消费者：接收死信消息
        AtomicReference<Message<?>> receivedDlqMessage = new AtomicReference<>();
        StreamMessageConcurrentlyConsumer<String> dlqListener = (msg, ctx) -> {
            receivedDlqMessage.set(msg);
            return ConsumeAction.SUCCESS;
        };
        container.registerConsumer(dlqListener, mkListenerAnnotation(topic, group, 0, true));

        // 创建消费者组：业务 Stream + DLQ Stream
        createConsumerGroup(topic, group);
        createDlqConsumerGroup(group);

        container.start();
        try {
            // 发送业务消息
            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
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
            String dlqStreamKey = StreamMQKeys.dlqStream(namespace, group);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                assertThat(dlqStream.listPending(group,
                    StreamMessageId.MIN, StreamMessageId.MAX, 100)).isEmpty();
            });
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("DLQ 消费者使用 dlqMode=true（对齐 RocketMQ %DLQ%{group}）")
    void dlqConsumer_defaultDlqConsumerGroup() {
        String topic = "dlq-default-group-topic";
        String group = "dlq-default-group";

        RetryPolicy noRetryPolicy = new NoRetryPolicy();
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, noRetryPolicy, namespace);

        // 业务消费者：始终失败
        container.registerConsumer(
            (msg, ctx) -> { throw new RuntimeException("fail"); },
            mkListenerAnnotation(topic, group, 0));

        // DLQ 消费者：dlqMode=true
        AtomicReference<Message<?>> receivedDlqMessage = new AtomicReference<>();
        container.registerConsumer(
            (StreamMessageConcurrentlyConsumer<String>) (msg, ctx) -> {
                receivedDlqMessage.set(msg);
                return ConsumeAction.SUCCESS;
            },
            mkListenerAnnotation(topic, group, 0, true));

        createConsumerGroup(topic, group);
        createDlqConsumerGroup(group);

        container.start();
        try {
            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("default-group-body").build());
            producer.close();

            await().atMost(20, TimeUnit.SECONDS).until(() -> receivedDlqMessage.get() != null);
            assertThat(receivedDlqMessage.get().getBody()).isEqualTo("default-group-body");
        } finally {
            container.stop();
        }
    }

    @Test
    @DisplayName("DLQ 消费者消费失败后调用 DlqFailureStrategy 并丢弃：消息从 DLQ PEL 移除，不进入重试循环")
    void dlqConsumer_consumeFailure_dropsMessage() {
        String topic = "dlq-drop-topic";
        String group = "dlq-drop-group";

        RetryPolicy noRetryPolicy = new NoRetryPolicy();
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        java.util.concurrent.atomic.AtomicReference<Message<?>> handledMessage = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> handledCause = new java.util.concurrent.atomic.AtomicReference<>();
        DlqFailureStrategy recordingStrategy = new DlqFailureStrategy() {
            @Override
            public DlqFailureDecision decide(Message<?> message, DlqFailureContext ctx) {
                handledMessage.set(message);
                handledCause.set(ctx.lastFailureCause());
                return DlqFailureDecision.drop();
            }

            @Override
            public String name() {
                return "recording-strategy";
            }
        };
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, noRetryPolicy, recordingStrategy, namespace);

        // 业务消费者：始终失败
        container.registerConsumer(
            (msg, ctx) -> { throw new RuntimeException("fail"); },
            mkListenerAnnotation(topic, group, 0));

        // DLQ 消费者：也始终失败（应被直接丢弃，并触发 DlqFailureStrategy）
        java.util.concurrent.atomic.AtomicInteger dlqAttempts = new java.util.concurrent.atomic.AtomicInteger(0);
        container.registerConsumer(
            (StreamMessageConcurrentlyConsumer<String>) (msg, ctx) -> {
                dlqAttempts.incrementAndGet();
                throw new RuntimeException("DLQ consumer also fails");
            },
            mkListenerAnnotation(topic, group, 0, true));

        createConsumerGroup(topic, group);
        createDlqConsumerGroup(group);

        container.start();
        try {
            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0, 0);
            producer.syncSend(MessageBuilder.<String>withTopic(topic).body("drop-body").build());
            producer.close();

            // 等待 DLQ 消费者被调用至少一次
            await().atMost(20, TimeUnit.SECONDS).until(() -> dlqAttempts.get() >= 1);

            // DlqFailureStrategy 应被调用，cause 为消费异常
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(handledMessage.get()).isNotNull();
                assertThat(handledCause.get()).isInstanceOf(RuntimeException.class);
                assertThat(handledCause.get().getMessage()).isEqualTo("DLQ consumer also fails");
            });

            // DLQ 消费失败后应直接 ACK 丢弃，PEL 应为空
            String dlqStreamKey = StreamMQKeys.dlqStream(namespace, group);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
                assertThat(dlqStream.listPending(group,
                    StreamMessageId.MIN, StreamMessageId.MAX, 100)).isEmpty();
            });
        } finally {
            container.stop();
        }
    }

    /**
     * 在 DLQ Stream 上创建消费者组。
     *
     * @param group 消费者组名（即原始消费者组，用于构造 DLQ Stream Key）
     */
    protected void createDlqConsumerGroup(String group) {
        String dlqStreamKey = StreamMQKeys.dlqStream(namespace, group);
        RStream<String, String> dlqStream = redisson.getStream(dlqStreamKey);
        try {
            dlqStream.createGroup(StreamCreateGroupArgs.name(group).makeStream().id(new StreamMessageId(0, 0)));
        } catch (RuntimeException ex) {
            // BUSYGROUP 表示组已存在，忽略
            if (ex.getMessage() == null || !ex.getMessage().contains("BUSYGROUP")) {
                throw ex;
            }
        }
    }
}
