package io.github.streammq.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.dlq.LogAndDropDlqFailureStrategy;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.retry.NoRetryPolicy;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.core.template.StreamMessageTemplate;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

/**
 * StreamMQ 核心集成测试，基于真实本地 Redis 连接验证完整消息收发链路。
 *
 * <p>本测试覆盖以下场景：
 *
 * <ul>
 *   <li>syncSend（基础 / 超时 / 重试）
 *   <li>asyncSend（CompletableFuture / 回调）
 *   <li>sendOneway
 *   <li>syncSendBatch
 *   <li>消费者注册与消息消费
 *   <li>Tag / Key / UserProperties 元数据传递
 *   <li>MessageMetadataBuilder 使用
 *   <li>异常处理
 * </ul>
 *
 * <p>每个测试方法使用独立的 Topic 与 ConsumerGroup，通过 flushdb 保证数据隔离。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("StreamMQ Core Integration Test (Real Redis)")
class CoreIntegrationTest extends StreamMQTestBase {

    private static final String NAMESPACE = "it-core";
    private static final String PRODUCER_GROUP = "it-core-producer";

    private MessageConverter converter;
    private StreamMessageProducerFactory producerFactory;
    private StreamMessageTemplate template;
    private StreamMQListenerContainer container;
    private RedissonStreamListenerFactory listenerFactory;

    @BeforeEach
    void setUp() {
        clearRedisData();

        MessageSerializer<Object> serializer = new JacksonJsonSerializer<>();
        converter = new DefaultMessageConverter(serializer);

        ProducerConfig producerConfig =
                ProducerConfig.builder()
                        .group(PRODUCER_GROUP)
                        .namespace(NAMESPACE)
                        .sendMessageTimeout(5000L)
                        .build();

        producerFactory = new RedissonStreamProducerFactory(redissonClient, converter);
        producerFactory.createProducer(producerConfig);

        template =
                new DefaultStreamMessageTemplate(
                        producerFactory, PRODUCER_GROUP, converter, producerConfig, null);

        listenerFactory = new RedissonStreamListenerFactory(redissonClient, converter);
        NoRetryPolicy retryPolicy = new NoRetryPolicy();
        container =
                new DefaultStreamMQListenerContainer(
                        redissonClient,
                        listenerFactory,
                        converter,
                        retryPolicy,
                        new LogAndDropDlqFailureStrategy(),
                        NAMESPACE);
    }

    @AfterEach
    void tearDown() {
        if (container != null && container.isRunning()) {
            container.stop();
        }
        if (listenerFactory != null && !listenerFactory.isClosed()) {
            listenerFactory.close();
        }
        if (producerFactory != null && !producerFactory.isClosed()) {
            producerFactory.close();
        }
    }

    // ==================== 辅助方法 ====================

    /** 使用动态代理创建 StreamMQConsumer 注解实例，避免引入 Mockito 注解 mock。 */
    private StreamMQConsumer buildConsumerAnnotation(String topic, String consumerGroup) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) -> {
                            String name = method.getName();
                            switch (name) {
                                case "topic":
                                    return topic;
                                case "consumerGroup":
                                    return consumerGroup;
                                case "consumeMode":
                                    return ConsumeMode.CLUSTERING;
                                case "messageModel":
                                    return MessageModel.CONCURRENT;
                                case "consumeThreadMin":
                                    return 1;
                                case "consumeThreadMax":
                                    return 64;
                                case "maxReconsumeTimes":
                                    return 3;
                                case "consumeTimeout":
                                    return 30000L;
                                case "selectorExpression":
                                    return "*";
                                case "namespace":
                                    return NAMESPACE;
                                case "selectorType":
                                    return SelectorType.TAG;
                                case "pullBatchSize":
                                    return 32;
                                case "pullInterval":
                                    return 0L;
                                case "streamMaxLen":
                                    return 0;
                                case "enableMsgTrace":
                                    return false;
                                case "enable":
                                    return true;
                                case "shardCount":
                                    return 4;
                                case "consumerName":
                                    return "";
                                case "retryStreamMaxLen":
                                    return 0;
                                case "suspendCurrentQueueTimeMillis":
                                    return 1000L;
                                default:
                                    Class<?> returnType = method.getReturnType();
                                    if (returnType == boolean.class) return false;
                                    if (returnType == int.class) return 0;
                                    if (returnType == long.class) return 0L;
                                    if (returnType == String.class) return "";
                                    if (returnType == Class[].class) return new Class<?>[0];
                                    return null;
                            }
                        });
    }

    /** 为指定 Topic 创建消费者组（绕过 RedissonStreamListener.ensureGroup() 的已知 bug）。 */
    private void ensureConsumerGroup(String topic, String group) {
        String streamKey = StreamMQKeys.topicStream(NAMESPACE, topic);
        redissonClient
                .getStream(streamKey)
                .createGroup(
                        org.redisson.api.stream.StreamCreateGroupArgs.name(group)
                                .makeStream()
                                .id(new StreamMessageId(0, 0)));
    }

    // ==================== Nested: syncSend ====================

    @Nested
    @DisplayName("syncSend tests")
    class SyncSendTests {

        @Test
        @DisplayName("syncSend 基本消息: 验证 SendResult 与 Stream 写入")
        void syncSend_basicMessage_success() {
            String topic = "sync-basic-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("hello-world").build();

            SendResult result = template.syncSend(msg);

            StreamMQAssertions.assertThat(result).isSuccess().hasTopic(topic).hasMessageId();
            assertThat(result.getMessageId().getStreamEntryId()).isNotEmpty();

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(1L);
        }

        @Test
        @DisplayName("syncSend 带超时: 在合理超时内完成")
        void syncSend_withTimeout_success() {
            String topic = "sync-timeout-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("timeout-body").build();

            SendResult result = template.syncSend(msg, 5000L);

            StreamMQAssertions.assertThat(result).isSuccess().hasTopic(topic);
        }

        @Test
        @DisplayName("syncSend 带重试: 正常消息一次成功不触发重试")
        void syncSend_withRetry_success() {
            String topic = "sync-retry-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("retry-body").build();

            SendResult result = template.syncSend(msg, 5000L, 3);

            StreamMQAssertions.assertThat(result).isSuccess();
            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(1L);
        }

        @Test
        @DisplayName("syncSend 多条消息: 各自独立写入")
        void syncSend_multipleMessages_allWritten() {
            String topic = "sync-multi-" + System.nanoTime();

            for (int i = 0; i < 5; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic).body("msg-" + i).build();
                SendResult result = template.syncSend(msg);
                StreamMQAssertions.assertThat(result).isSuccess();
            }

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(5L);
        }

        @Test
        @DisplayName("syncSend 后验证 Stream Entry 字段: body, tag, keys 正确写入")
        void syncSend_streamFieldsVerified() {
            String topic = "sync-fields-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic)
                            .tag("order-tag")
                            .keys("order-key-001")
                            .body("order-payload")
                            .withUserProperty("traceId", "trace-xyz")
                            .build();

            SendResult result = template.syncSend(msg);
            StreamMQAssertions.assertThat(result).isSuccess().hasTag("order-tag").hasTopic(topic);

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(range).hasSize(1);

            Map<String, String> fields = range.values().iterator().next();
            assertThat(fields).containsEntry("tag", "order-tag");
            assertThat(fields).containsEntry("keys", "order-key-001");
            assertThat(fields.get("body")).isNotEmpty();
            assertThat(fields).containsEntry("bodyType", String.class.getName());
            assertThat(fields).containsKey("props");
            assertThat(fields.get("props")).contains("traceId");
        }

        @Test
        @DisplayName("syncSend 消息回读: 通过 MessageConverter 反序列化后字段一致")
        void syncSend_roundTrip_contentMatches() {
            String topic = "sync-roundtrip-" + System.nanoTime();
            String originalBody = "round-trip-body-content";
            String originalTag = "rt-tag";
            String originalKeys = "rt-key";

            Message<String> sent =
                    MessageBuilder.<String>withTopic(topic)
                            .tag(originalTag)
                            .keys(originalKeys)
                            .body(originalBody)
                            .withUserProperty("env", "test")
                            .build();

            SendResult sendResult = template.syncSend(sent);
            StreamMQAssertions.assertThat(sendResult).isSuccess();

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            Map<String, String> fields = range.values().iterator().next();

            Message<String> received = converter.fromStreamFields(fields, String.class);
            DefaultMessageConverter.applyTopic(received, topic);
            DefaultMessageConverter.applyMessageId(
                    received, range.keySet().iterator().next().toString());

            StreamMQAssertions.assertThat(received)
                    .hasTopic(topic)
                    .hasTag(originalTag)
                    .hasKeys(originalKeys)
                    .hasBody(originalBody)
                    .hasUserProperty("env", "test");
        }
    }

    // ==================== Nested: asyncSend ====================

    @Nested
    @DisplayName("asyncSend tests")
    class AsyncSendTests {

        @Test
        @DisplayName("asyncSend CompletableFuture: 发送完成后 SendResult 非空")
        void asyncSend_future_completesWithResult() {
            String topic = "async-future-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("async-body").build();

            CompletableFuture<SendResult> future = template.asyncSend(msg);

            SendResult result =
                    await().atMost(5, TimeUnit.SECONDS)
                            .until(future::join, r -> r != null && r.isSuccess());

            assertThat(result).isNotNull();
            assertThat(result.getTopic()).isEqualTo(topic);
            assertThat(result.getMessageId()).isNotNull();
            assertThat(result.isSuccess()).isTrue();

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(1L);
        }

        @Test
        @DisplayName("asyncSend CompletableFuture: 并发发送 10 条全部成功")
        void asyncSend_future_concurrentAllSucceed() {
            String topic = "async-concurrent-" + System.nanoTime();
            int count = 10;

            CompletableFuture<?>[] futures = new CompletableFuture<?>[count];
            for (int i = 0; i < count; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic).body("concurrent-" + i).build();
                futures[i] = template.asyncSend(msg);
            }

            CompletableFuture.allOf(futures).join();

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(count);
        }

        @Test
        @DisplayName("asyncSend 回调方式: onSuccess 被调用且 SendResult 正确")
        void asyncSend_callback_successInvoked() throws Exception {
            String topic = "async-callback-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("callback-body").build();

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<SendResult> resultRef = new AtomicReference<>();
            AtomicBoolean successCalled = new AtomicBoolean(false);
            AtomicBoolean errorCalled = new AtomicBoolean(false);

            template.asyncSend(
                    msg,
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult result) {
                            resultRef.set(result);
                            successCalled.set(true);
                            latch.countDown();
                        }

                        @Override
                        public void onException(Throwable ex) {
                            errorCalled.set(true);
                            latch.countDown();
                        }
                    });

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(successCalled).isTrue();
            assertThat(errorCalled).isFalse();
            assertThat(resultRef.get()).isNotNull();
            assertThat(resultRef.get().isSuccess()).isTrue();
            assertThat(resultRef.get().getTopic()).isEqualTo(topic);
        }

        @Test
        @DisplayName("asyncSend 回调方式: 多条消息回调均收到")
        void asyncSend_callback_multipleMessages() throws Exception {
            String topic = "async-cb-multi-" + System.nanoTime();
            int count = 5;
            CountDownLatch latch = new CountDownLatch(count);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < count; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic).body("cb-body-" + i).build();
                template.asyncSend(
                        msg,
                        new SendCallback() {
                            @Override
                            public void onSuccess(SendResult result) {
                                successCount.incrementAndGet();
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(successCount.get()).isEqualTo(count);

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(count);
        }
    }

    // ==================== Nested: sendOneway ====================

    @Nested
    @DisplayName("sendOneway tests")
    class SendOnewayTests {

        @Test
        @DisplayName("sendOneway: 消息最终写入 Stream")
        void sendOneway_eventuallyWrittenToStream() {
            String topic = "oneway-basic-" + System.nanoTime();
            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("oneway-body").build();

            template.sendOneway(msg);

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                RStream<String, String> stream =
                                        redissonClient.getStream(
                                                StreamMQKeys.topicStream(NAMESPACE, topic));
                                assertThat(stream.size()).isEqualTo(1L);
                            });
        }

        @Test
        @DisplayName("sendOneway 多条: 全部最终写入")
        void sendOneway_multiple_allWritten() {
            String topic = "oneway-multi-" + System.nanoTime();
            int count = 8;

            for (int i = 0; i < count; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic).body("oneway-" + i).build();
                template.sendOneway(msg);
            }

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                RStream<String, String> stream =
                                        redissonClient.getStream(
                                                StreamMQKeys.topicStream(NAMESPACE, topic));
                                assertThat(stream.size()).isEqualTo(count);
                            });
        }
    }

    // ==================== Nested: syncSendBatch ====================

    @Nested
    @DisplayName("syncSendBatch tests")
    class SyncSendBatchTests {

        @Test
        @DisplayName("syncSendBatch 批量发送: 所有消息写入 Stream")
        void syncSendBatch_allMessagesWritten() {
            String topic = "batch-basic-" + System.nanoTime();
            int count = 5;

            BatchMessage.Builder<String> batchBuilder = BatchMessage.<String>withTopic(topic);
            for (int i = 0; i < count; i++) {
                batchBuilder.add(
                        MessageBuilder.<String>withTopic(topic)
                                .tag("batch-tag")
                                .keys("batch-key-" + i)
                                .body("batch-body-" + i)
                                .build());
            }
            BatchMessage<String> batch = batchBuilder.build();

            List<SendResult> results = template.syncSendBatch(batch);

            assertThat(results).hasSize(count);
            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(count);
        }

        @Test
        @DisplayName("syncSendBatch 空批抛 IllegalArgumentException")
        void syncSendBatch_emptyBatch_throws() {
            BatchMessage.Builder<String> builder = BatchMessage.<String>withTopic("empty-topic");
            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("empty");
        }

        @Test
        @DisplayName("syncSendBatch 不同 Topic 抛 IllegalArgumentException")
        void syncSendBatch_differentTopics_throws() {
            String topicA = "batch-a-" + System.nanoTime();
            String topicB = "batch-b-" + System.nanoTime();

            BatchMessage.Builder<String> builder = BatchMessage.<String>withTopic(topicA);
            builder.add(MessageBuilder.<String>withTopic(topicA).body("a").build());

            assertThatThrownBy(
                            () ->
                                    builder.add(
                                            MessageBuilder.<String>withTopic(topicB)
                                                    .body("b")
                                                    .build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not match");
        }

        @Test
        @DisplayName("syncSendBatch 批量消息包含元数据: tag/keys/userProps 正确写入")
        void syncSendBatch_withMetadata_correctlyWritten() {
            String topic = "batch-meta-" + System.nanoTime();

            BatchMessage.Builder<String> batchBuilder = BatchMessage.<String>withTopic(topic);
            for (int i = 0; i < 3; i++) {
                batchBuilder.add(
                        MessageBuilder.<String>withTopic(topic)
                                .tag("meta-tag-" + i)
                                .keys("meta-key-" + i)
                                .body("meta-body-" + i)
                                .withUserProperty("orderNo", "ORD-" + i)
                                .build());
            }

            List<SendResult> results = template.syncSendBatch(batchBuilder.build());

            assertThat(results).hasSize(3);
            assertThat(results)
                    .allSatisfy(
                            r -> {
                                assertThat(r.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                                assertThat(r.isSuccess()).isTrue();
                            });

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(3L);

            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(3, StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(range.values())
                    .allSatisfy(
                            fields -> {
                                assertThat(fields).containsKey("tag");
                                assertThat(fields).containsKey("keys");
                                assertThat(fields).containsKey("body");
                            });
        }
    }

    // ==================== Nested: Consumer ====================

    @Nested
    @DisplayName("Consumer registration and consumption tests")
    class ConsumerTests {

        @Test
        @DisplayName("消费者注册 + 发送 + 消费: 消息正确送达")
        void consumer_sendAndReceive_messageDelivered() throws Exception {
            String topic = "consume-basic-" + System.nanoTime();
            String group = "consume-basic-group";

            ensureConsumerGroup(topic, group);

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            StreamMQConsumer annotation = buildConsumerAnnotation(topic, group);
            container.registerConsumer(consumer, annotation);
            container.start();

            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).body("consume-body").build();
            template.syncSend(msg);

            consumer.awaitMessages(1, 5000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(1);
            StreamMQAssertions.assertThat(received.get(0)).hasTopic(topic).hasBody("consume-body");
        }

        @Test
        @DisplayName("消费者消费消息内容完整: tag/keys/userProps 一致")
        void consumer_contentIntegrity_allFieldsMatched() throws Exception {
            String topic = "consume-meta-" + System.nanoTime();
            String group = "consume-meta-group";

            ensureConsumerGroup(topic, group);

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            Message<String> sent =
                    MessageBuilder.<String>withTopic(topic)
                            .tag("important")
                            .keys("order-123")
                            .body("full-content")
                            .withUserProperty("priority", "high")
                            .withUserProperty("region", "us-east")
                            .build();
            template.syncSend(sent);

            consumer.awaitMessages(1, 5000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(1);
            Message<String> r = received.get(0);
            StreamMQAssertions.assertThat(r)
                    .hasTopic(topic)
                    .hasTag("important")
                    .hasKeys("order-123")
                    .hasBody("full-content")
                    .hasUserProperty("priority", "high")
                    .hasUserProperty("region", "us-east");
        }

        @Test
        @DisplayName("消费者批量消费: 发送多条后全部收到")
        void consumer_multipleMessages_allReceived() throws Exception {
            String topic = "consume-multi-" + System.nanoTime();
            String group = "consume-multi-group";

            ensureConsumerGroup(topic, group);

            int count = 5;
            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            for (int i = 0; i < count; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic).body("msg-" + i).build();
                template.syncSend(msg);
            }

            consumer.awaitMessages(count, 8000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(count);
            assertThat(received.stream().map(Message::getBody).toList())
                    .containsExactlyInAnyOrderElementsOf(
                            java.util.stream.IntStream.range(0, count)
                                    .mapToObj(i -> "msg-" + i)
                                    .toList());
        }

        @Test
        @DisplayName("消费者 onMessage 返回 SUCCESS: 自动 ACK")
        void consumer_returnSuccess_automaticallyAck() throws Exception {
            String topic = "consume-ack-" + System.nanoTime();
            String group = "consume-ack-group";

            ensureConsumerGroup(topic, group);

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            consumer.setNextAction(ConsumeAction.SUCCESS);

            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            template.syncSend(MessageBuilder.<String>withTopic(topic).body("ack-test").build());

            consumer.awaitMessages(1, 5000);
            assertThat(consumer.getSuccessCount()).isEqualTo(1);

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            assertThat(stream.size()).isEqualTo(1L);
        }

        @Test
        @DisplayName("消费者 onMessage 抛异常: 统计 failCount")
        void consumer_throwException_failCount() throws Exception {
            String topic = "consume-fail-" + System.nanoTime();
            String group = "consume-fail-group";

            ensureConsumerGroup(topic, group);

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            consumer.setShouldFail(true);
            consumer.setFailAfterCount(0);
            consumer.prepareAwait(1);

            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            template.syncSend(MessageBuilder.<String>withTopic(topic).body("fail-test").build());

            consumer.waitForMessages(5000);
            assertThat(consumer.getExceptions()).isNotEmpty();
            assertThat(consumer.getFailCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("消费者并发消费: 多线程同时处理消息")
        void consumer_concurrentConsumption_multipleThreads() throws Exception {
            String topic = "consume-concurrent-" + System.nanoTime();
            String group = "consume-concurrent-group";

            ensureConsumerGroup(topic, group);

            int count = 10;
            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            consumer.prepareAwait(count);
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            for (int i = 0; i < count; i++) {
                Message<String> msg =
                        MessageBuilder.<String>withTopic(topic).body("concurrent-" + i).build();
                template.syncSend(msg);
            }

            consumer.waitForMessages(10000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(count);
            assertThat(consumer.getSuccessCount()).isEqualTo(count);
        }
    }

    // ==================== Nested: Metadata ====================

    @Nested
    @DisplayName("MessageMetadataBuilder and message metadata tests")
    class MetadataTests {

        @Test
        @DisplayName("MessageMetadataBuilder 构建完整元数据: tag/keys/shardingKey/userProps 全部传递")
        void metadataBuilder_fullMetadata_propagated() {
            String topic = "meta-full-" + System.nanoTime();
            String metadataTag = "full-tag";
            String metadataKeys = "full-key";
            String metadataShardingKey = "shard-42";

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create()
                            .tag(metadataTag)
                            .keys(metadataKeys)
                            .shardingKey(metadataShardingKey)
                            .userProperty("orderType", "PREMIUM")
                            .userProperty("customerId", "CUST-001")
                            .property("sysSource", "integration-test");

            MessageBuilder<String> builder = MessageBuilder.<String>withTopic(topic);
            metadata.applyTo(builder);
            Message<String> builtMsg = builder.body("metadata-body").build();

            SendResult result = template.syncSend(builtMsg);
            StreamMQAssertions.assertThat(result).isSuccess().hasTag(metadataTag).hasTopic(topic);

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            Map<String, String> fields = range.values().iterator().next();
            assertThat(fields).containsEntry("tag", metadataTag);
            assertThat(fields).containsEntry("keys", metadataKeys);
            assertThat(fields).containsEntry("shardingKey", metadataShardingKey);
            assertThat(fields.get("props")).contains("orderType");
        }

        @Test
        @DisplayName("Message with tags: tag 正确传递到 Stream Entry")
        void messageWithTag_tagCorrectlyStored() {
            String topic = "tag-test-" + System.nanoTime();
            String tag = "urgent";

            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).tag(tag).body("tag-body").build();

            SendResult result = template.syncSend(msg);
            StreamMQAssertions.assertThat(result).isSuccess().hasTag(tag);

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(range.values().iterator().next()).containsEntry("tag", tag);
        }

        @Test
        @DisplayName("Message with keys: keys 正确传递")
        void messageWithKeys_keysCorrectlyStored() {
            String topic = "keys-test-" + System.nanoTime();
            String keys = "INV-2024-001";

            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic).keys(keys).body("keys-body").build();

            SendResult result = template.syncSend(msg);
            StreamMQAssertions.assertThat(result).isSuccess().hasTopic(topic);

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(range.values().iterator().next()).containsEntry("keys", keys);
        }

        @Test
        @DisplayName("Message with user properties: 多个 userProperty 正确合并存储")
        void messageWithUserProps_allPropsCorrectlyStored() {
            String topic = "userprops-test-" + System.nanoTime();

            Message<String> msg =
                    MessageBuilder.<String>withTopic(topic)
                            .body("props-body")
                            .withUserProperty("orderId", "ORD-999")
                            .withUserProperty("amount", "199.99")
                            .withUserProperty("currency", "USD")
                            .build();

            SendResult result = template.syncSend(msg);
            StreamMQAssertions.assertThat(result).isSuccess();

            RStream<String, String> stream =
                    redissonClient.getStream(StreamMQKeys.topicStream(NAMESPACE, topic));
            Map<StreamMessageId, Map<String, String>> range =
                    stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            String props = range.values().iterator().next().get("props");
            assertThat(props).contains("orderId");
            assertThat(props).contains("amount");
            assertThat(props).contains("currency");
        }

        @Test
        @DisplayName("MessageMetadataBuilder 空元数据: 不影响消息正常发送")
        void metadataBuilder_emptyMetadata_messageSentNormally() {
            String topic = "meta-empty-" + System.nanoTime();

            MessageMetadataBuilder emptyMetadata = MessageMetadataBuilder.create();

            MessageBuilder<String> builder = MessageBuilder.<String>withTopic(topic);
            emptyMetadata.applyTo(builder);
            Message<String> msg = builder.body("empty-meta-body").build();

            SendResult result = template.syncSend(msg);
            StreamMQAssertions.assertThat(result).isSuccess();
        }

        @Test
        @DisplayName("MessageMetadataBuilder hasDelay: 未设置延时返回 false")
        void metadataBuilder_hasDelay_returnsFalseWhenNoDelay() {
            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("test").keys("key1");
            assertThat(metadata.hasDelay()).isFalse();
        }
    }

    // ==================== Nested: Error handling ====================

    @Nested
    @DisplayName("Error handling tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("syncSend 关闭后发送抛 IllegalStateException")
        void syncSend_afterClose_throws() {
            String topic = "err-close-" + System.nanoTime();
            template = new DefaultStreamMessageTemplate(producerFactory, PRODUCER_GROUP, converter);
            producerFactory.close();

            Message<String> msg = MessageBuilder.<String>withTopic(topic).body("test").build();

            assertThatThrownBy(() -> template.syncSend(msg))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        }

        @Test
        @DisplayName("syncSend null body 抛 NullPointerException")
        void syncSend_nullBody_throws() {
            String topic = "err-null-body-" + System.nanoTime();
            MessageBuilder<String> builder = MessageBuilder.withTopic(topic);

            assertThatThrownBy(() -> builder.build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("body");
        }

        @Test
        @DisplayName("syncSend null topic 抛 NullPointerException")
        void syncSend_nullTopic_throws() {
            assertThatThrownBy(() -> MessageBuilder.<String>withTopic(null).body("test").build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("syncSend 空字符串 topic 抛 IllegalArgumentException")
        void syncSend_emptyTopic_throws() {
            assertThatThrownBy(() -> MessageBuilder.<String>withTopic("").body("test").build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty");
        }

        @Test
        @DisplayName("asyncSend null message 抛 NullPointerException")
        void asyncSend_nullMessage_throws() {
            assertThatThrownBy(() -> template.asyncSend(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("syncSendBatch null message 列表抛 NullPointerException")
        void syncSendBatch_nullList_throws() {
            assertThatThrownBy(() -> template.syncSendBatch(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("MessageId 构造: 非法格式抛 IllegalArgumentException")
        void messageId_invalidFormat_throws() {
            assertThatThrownBy(() -> new MessageId("invalid-format"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("MessageMetadataBuilder delayTimeMillis 非正数抛 IllegalArgumentException")
        void metadataBuilder_invalidDelay_throws() {
            assertThatThrownBy(() -> MessageMetadataBuilder.create().delayTimeMillis(0))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> MessageMetadataBuilder.create().delayTimeMillis(-1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Consumer 容器重复注册后启动: 状态异常抛 IllegalStateException")
        void container_duplicateStart_throws() throws Exception {
            String topic = "err-dup-start-" + System.nanoTime();
            ensureConsumerGroup(topic, "err-group");

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, "err-group"));
            container.start();

            assertThatThrownBy(container::start).isInstanceOf(IllegalStateException.class);

            container.stop();
        }

        @Test
        @DisplayName("StreamMQAssertions.isSuccess 对失败结果返回 false")
        void assertions_isSuccess_failedResult() {
            SendResult failedResult =
                    new SendResult(
                            MessageId.sentinel(),
                            "test-topic",
                            null,
                            SendStatus.SEND_FAILED,
                            System.currentTimeMillis(),
                            null,
                            "test-error");

            assertThat(failedResult.isSuccess()).isFalse();
            assertThat(failedResult.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
            assertThat(failedResult.getErrorMessage()).isEqualTo("test-error");
        }

        @Test
        @DisplayName("SendResult 构造: sentinel MessageId 不抛异常")
        void sendResult_sentinelMessageId_constructs() {
            MessageId sentinel = MessageId.sentinel();
            SendResult result =
                    new SendResult(
                            sentinel,
                            "topic",
                            null,
                            SendStatus.SEND_FAILED,
                            System.currentTimeMillis(),
                            null,
                            "failed");

            assertThat(result.getMessageId()).isNotNull();
            assertThat(result.isSuccess()).isFalse();
        }
    }

    // ==================== Nested: Integration scenarios ====================

    @Nested
    @DisplayName("Full integration flow tests")
    class FullFlowTests {

        @Test
        @DisplayName("完整流程: syncSend -> 消费 -> 验证内容")
        void fullFlow_syncSendAndConsume() throws Exception {
            String topic = "flow-sync-" + System.nanoTime();
            String group = "flow-sync-group";

            ensureConsumerGroup(topic, group);

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            Message<String> sent =
                    MessageBuilder.<String>withTopic(topic)
                            .tag("order-tag")
                            .keys("order-key-001")
                            .body("full-flow-payload")
                            .withUserProperty("env", "integration-test")
                            .build();

            SendResult sendResult = template.syncSend(sent);
            StreamMQAssertions.assertThat(sendResult).isSuccess();

            consumer.awaitMessages(1, 5000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(1);

            Message<String> r = received.get(0);
            assertThat(r.getTopic()).isEqualTo(topic);
            assertThat(r.getTag()).isEqualTo("order-tag");
            assertThat(r.getKeys()).isEqualTo("order-key-001");
            assertThat(r.getBody()).isEqualTo("full-flow-payload");
            assertThat(r.getUserProperties()).containsEntry("env", "integration-test");
            assertThat(r.getMessageId()).isNotNull();
        }

        @Test
        @DisplayName("完整流程: asyncSend -> 消费 -> 验证")
        void fullFlow_asyncSendAndConsume() throws Exception {
            String topic = "flow-async-" + System.nanoTime();
            String group = "flow-async-group";

            ensureConsumerGroup(topic, group);

            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            consumer.prepareAwait(1);
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            Message<String> sent =
                    MessageBuilder.<String>withTopic(topic)
                            .tag("async-tag")
                            .body("async-payload")
                            .build();

            CompletableFuture<SendResult> future = template.asyncSend(sent);
            SendResult sendResult = future.get(5, TimeUnit.SECONDS);
            StreamMQAssertions.assertThat(sendResult).isSuccess();

            consumer.waitForMessages(5000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(1);
            assertThat(received.get(0).getBody()).isEqualTo("async-payload");
        }

        @Test
        @DisplayName("完整流程: 批量发送 -> 批量消费 -> 验证")
        void fullFlow_batchSendAndConsume() throws Exception {
            String topic = "flow-batch-" + System.nanoTime();
            String group = "flow-batch-group";

            ensureConsumerGroup(topic, group);

            int count = 10;
            TestStreamMQListener<String> consumer = new TestStreamMQListener<>();
            consumer.prepareAwait(count);
            container.registerConsumer(consumer, buildConsumerAnnotation(topic, group));
            container.start();

            BatchMessage.Builder<String> batchBuilder = BatchMessage.<String>withTopic(topic);
            for (int i = 0; i < count; i++) {
                batchBuilder.add(
                        MessageBuilder.<String>withTopic(topic)
                                .tag("batch-t" + (i % 3))
                                .keys("batch-key-" + i)
                                .body("batch-payload-" + i)
                                .build());
            }

            List<SendResult> results = template.syncSendBatch(batchBuilder.build());
            assertThat(results).hasSize(count);
            assertThat(results)
                    .allSatisfy(
                            r -> {
                                assertThat(r.isSuccess()).isTrue();
                                assertThat(r.getTopic()).isEqualTo(topic);
                            });

            consumer.waitForMessages(10000);

            List<Message<String>> received = consumer.getReceivedMessages();
            assertThat(received).hasSize(count);
        }

        @Test
        @DisplayName("SendResult.messageId 非空且格式合法")
        void sendResult_messageId_format() {
            String topic = "msgid-format-" + System.nanoTime();

            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(topic).body("mid-test").build());

            assertThat(result.getMessageId()).isNotNull();
            assertThat(result.getMessageId().getStreamEntryId()).isNotEmpty();
            assertThat(result.getMessageId().getTimestamp()).isGreaterThan(0);
        }

        @Test
        @DisplayName("MessageId 工厂方法: sentinel 与 fromStreamEntry 正确")
        void messageId_factoryMethods() {
            MessageId sentinel = MessageId.sentinel();
            assertThat(sentinel.getStreamEntryId()).isNotEmpty();
            assertThat(sentinel.getTimestamp()).isGreaterThan(0);

            MessageId fromEntry = MessageId.fromStreamEntry("1700000000000-0");
            assertThat(fromEntry.getTimestamp()).isEqualTo(1700000000000L);
            assertThat(fromEntry.getSequence()).isEqualTo(0L);

            MessageId fromStreamMessageId = MessageId.fromStreamMessageId("1234567890-42");
            assertThat(fromStreamMessageId.getTimestamp()).isEqualTo(1234567890L);
            assertThat(fromStreamMessageId.getSequence()).isEqualTo(42L);
        }
    }
}
