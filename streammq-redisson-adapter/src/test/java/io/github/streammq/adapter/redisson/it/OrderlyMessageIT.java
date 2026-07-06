package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.container.ContainerState;
import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.*;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 顺序消费({@code @StreamMQConsumer(messageModel = ORDERLY)})端到端 Redis 联动集成测试。
 *
 * <p>覆盖 {@link DefaultStreamMQListenerContainer#registerOrderlyConsumer} 注册的
 * {@link StreamMessageOrderlyConsumer} 在真实 Redis 环境下的消息接收、顺序消费、
 * {@link OrderlyAction#SUSPEND_CURRENT_QUEUE_A_MOMENT} 挂起行为以及容器生命周期管理。
 */
@DisplayName("顺序消费集成测试")
class OrderlyMessageIT extends AbstractRedisIT {

    /**
     * 测试用快速重试策略,固定延迟与最大重试次数。
     * 当 {@code reconsumeTimes >= maxRetries} 时返回 {@code null},触发 DLQ 路由。
     */
    static class FastRetryPolicy implements RetryPolicy {
        private final long delayMs;
        private final int maxRetries;

        FastRetryPolicy(long delayMs, int maxRetries) {
            this.delayMs = delayMs;
            this.maxRetries = maxRetries;
        }

        @Override
        public Duration nextRetryDelay(int reconsumeTimes, Message<?> message) {
            if (reconsumeTimes >= maxRetries) {
                return null;
            }
            return Duration.ofMillis(delayMs);
        }

        @Override
        public boolean shouldStopRetry(int reconsumeTimes, Message<?> message) {
            return reconsumeTimes >= maxRetries;
        }
    }

    /**
     * 通过动态代理构造 {@link StreamMQConsumer} 注解实例（messageModel=ORDERLY）。
     *
     * @param topic 主题
     * @param group 消费者组
     * @param maxReconsumeTimes 最大重试次数
     * @return 注解代理实例
     */
    @SuppressWarnings("unchecked")
    private static StreamMQConsumer mkOrderlyAnnotation(
            String topic, String group, int maxReconsumeTime) {
        return (StreamMQConsumer) Proxy.newProxyInstance(
            StreamMQConsumer.class.getClassLoader(),
            new Class<?>[]{StreamMQConsumer.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "topic" -> topic;
                case "consumerGroup" -> group;
                case "consumeMode" -> ConsumeMode.CLUSTERING;
                case "messageModel" -> MessageModel.ORDERLY;
                case "acknowledgeMode" -> AcknowledgeMode.AUTO;
                case "maxReconsumeTimes" -> maxReconsumeTime;
                case "consumeThreadMin" -> 1;
                case "consumeThreadMax" -> 1;
                case "consumeTimeout" -> 30000L;
                case "shardCount" -> 4;
                case "selectorExpression" -> "*";
                case "serializer" -> MessageSerializer.class;
                case "namespace" -> "";
                case "enable" -> true;
                case "selectorType" -> SelectorType.TAG;
                case "pullBatchSize" -> 32;
                case "retryPolicy" -> RetryPolicy.class;
                case "enableMsgTrace" -> false;
                case "streamMaxLen" -> 0;
                case "messageConverter" -> MessageConverter.class;
                case "rebalanceStrategy" -> RebalanceStrategy.class;
                case "pullInterval" -> 0L;
                case "suspendCurrentQueueTimeMillis" -> 1000L;
                case "consumerName" -> "";
                case "annotationType" -> StreamMQConsumer.class;
                case "hashCode" -> (topic + group).hashCode();
                case "equals" -> args != null && args.length > 0 && proxy == args[0];
                case "toString" -> "@StreamMQConsumer(topic=" + topic + ", consumerGroup=" + group
                    + ", messageModel=ORDERLY)";
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
    @DisplayName("单条消息:OrderlyListener 收到消息并返回 SUCCESS 后消息被 ACK")
    void orderlyListener_receivesMessage() {
        String topic = "orderly-recv-topic";
        String group = "orderly-recv-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, retryPolicy, namespace);

        AtomicReference<Message<?>> receivedRef = new AtomicReference<>();
        StreamMessageOrderlyConsumer<String> listener = (msg, ctx) -> {
            receivedRef.set(msg);
            return OrderlyAction.SUCCESS;
        };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("t1")
                .keys("k1")
                .body("orderly-body")
                .build());

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(receivedRef.get()).isNotNull());

            Message<?> received = receivedRef.get();
            assertThat(received.getTopic()).isEqualTo(topic);
            assertThat(received.getTag()).isEqualTo("t1");
            assertThat(received.getKeys()).isEqualTo("k1");
            assertThat(received.getBody()).isEqualTo("orderly-body");

            // SUCCESS 后 PEL 应为空
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100)).isEmpty();
            });
        } finally {
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("多条消息:全部被 OrderlyListener 顺序消费,内容与发送顺序一致")
    void orderlyListener_multipleMessages_allConsumed() {
        String topic = "orderly-multi-topic";
        String group = "orderly-multi-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, retryPolicy, namespace);

        List<String> consumedBodies = new java.util.concurrent.CopyOnWriteArrayList<>();
        StreamMessageOrderlyConsumer<String> listener = (msg, ctx) -> {
            consumedBodies.add((String) msg.getBody());
            return OrderlyAction.SUCCESS;
        };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
        try {
            // 按顺序发送 5 条消息
            for (int i = 0; i < 5; i++) {
                producer.syncSend(MessageBuilder.<String>withTopic(topic)
                    .body("m-" + i)
                    .build());
            }

            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(consumedBodies).hasSize(5));

            // 验证消费顺序与发送顺序一致
            assertThat(consumedBodies).containsExactly("m-0", "m-1", "m-2", "m-3", "m-4");

            // 全部 ACK 后 PEL 应为空
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100)).isEmpty();
            });
        } finally {
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("SUSPEND_CURRENT_QUEUE_A_MOMENT:OrderlyListener 返回 SUSPEND 后消息留在 PEL,不进入 retry ZSet")
    void orderlyListener_suspend_staysInPel() {
        String topic = "orderly-suspend-topic";
        String group = "orderly-suspend-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 100);
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, retryPolicy, namespace);

        AtomicInteger attempt = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener = (msg, ctx) -> {
            attempt.incrementAndGet();
            // 始终返回 SUSPEND,消息应留在 PEL
            return OrderlyAction.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 16));
        createConsumerGroup(topic, group);
        container.start();

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .body("suspend-body")
                .build());

            // 等待消费者被调用至少一次
            await().atMost(10, TimeUnit.SECONDS).until(() -> attempt.get() >= 1);

            // SUSPEND 后消息应留在 PEL 中(未 ACK)
            RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
            assertThat(stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .as("SUSPEND 后消息应留在 PEL 中")
                .isNotEmpty();

            // retry ZSet 不应有消息(SUSPEND 不写入 retry ZSet)
            String retryKey = StreamMQKeys.retryZSet(namespace, topic, group);
            RScoredSortedSet<String> zset = redisson.getScoredSortedSet(retryKey);
            assertThat(zset.size())
                .as("SUSPEND 不应写入 retry ZSet")
                .isZero();
        } finally {
            producer.close();
            container.stop();
        }
    }

    @Test
    @DisplayName("生命周期:start 后容器运行,stop 后容器停止且不再消费消息")
    void orderlyListener_lifecycle_startStop() {
        String topic = "orderly-lifecycle-topic";
        String group = "orderly-lifecycle-group";

        RetryPolicy retryPolicy = new FastRetryPolicy(100, 3);
        RedissonStreamListenerFactory consumerFactory = new RedissonStreamListenerFactory(redisson, converter);
        DefaultStreamMQListenerContainer container =
            new DefaultStreamMQListenerContainer(redisson, consumerFactory, converter, retryPolicy, namespace);

        AtomicInteger consumed = new AtomicInteger(0);
        StreamMessageOrderlyConsumer<String> listener = (msg, ctx) -> {
            consumed.incrementAndGet();
            return OrderlyAction.SUCCESS;
        };
        container.registerOrderlyConsumer(listener, mkOrderlyAnnotation(topic, group, 3));
        createConsumerGroup(topic, group);

        // 启动前应为 INIT 状态且非运行
        assertThat(container.isRunning()).isFalse();
        assertThat(container.getState()).isEqualTo(ContainerState.INIT);

        container.start();
        try {
            // 启动后应为 RUNNING 状态
            assertThat(container.isRunning()).isTrue();
            assertThat(container.getState()).isEqualTo(ContainerState.RUNNING);

            RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0);
            try {
                producer.syncSend(MessageBuilder.<String>withTopic(topic).body("lc-body").build());

                // 等待消费发生
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(consumed.get()).isGreaterThan(0));
            } finally {
                producer.close();
            }
        } finally {
            container.stop();
        }

        // 停止后应为 STOPPED 状态且非运行
        assertThat(container.isRunning()).isFalse();
        assertThat(container.getState()).isEqualTo(ContainerState.STOPPED);
    }
}
