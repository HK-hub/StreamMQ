package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducerFactory;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendOptions;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.SendCallback;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

/**
 * {@link DefaultStreamMessageTemplate} 的 Redis 联动集成测试。
 *
 * <p>覆盖 syncSend/asyncSend/syncSendBatch 往返、拦截器链(含中止)与多 Topic 场景。
 */
@DisplayName("DefaultStreamMQTemplate 集成测试")
class TemplateIT extends AbstractRedisIT {

    private RedissonStreamProducerFactory producerFactory;
    private DefaultStreamMessageTemplate template;
    private static final String DEFAULT_GROUP = "template-test-group";

    @BeforeEach
    void setUpTemplate() {
        producerFactory = new RedissonStreamProducerFactory(redisson, converter);
        ProducerConfig defaultProps =
                ProducerConfig.builder().group(DEFAULT_GROUP).namespace(namespace).build();
        template =
                new DefaultStreamMessageTemplate(
                        producerFactory, DEFAULT_GROUP, converter, defaultProps, null);
    }

    @AfterEach
    void tearDownTemplate() {
        if (producerFactory != null) {
            producerFactory.close();
        }
    }

    @Test
    @DisplayName("syncSend + receive 往返:内容一致")
    void syncSendAndReceive_roundTrip() {
        String topic = "tpl-rt-topic";
        Message<String> msg =
                MessageBuilder.<String>withTopic(topic)
                        .tag("rt-tag")
                        .keys("rt-key")
                        .body("round-trip-body")
                        .build();

        SendResult result = template.syncSend(msg);
        assertThat(result.isSuccess()).isTrue();

        RedissonStreamListener consumer =
                new RedissonStreamListener(
                        redisson, namespace, topic, "tpl-rt-group", "c1", converter);
        createConsumerGroup(topic, "tpl-rt-group");
        try {
            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getBody()).isEqualTo("round-trip-body");
            assertThat(messages.get(0).getTag()).isEqualTo("rt-tag");
            assertThat(messages.get(0).getKeys()).isEqualTo("rt-key");
        } finally {
            consumer.close();
        }
    }

    @Test
    @DisplayName("asyncSend + callback:callback.onSuccess 被调用")
    void asyncSendWithCallback_onSuccessInvoked() {
        String topic = "tpl-async-topic";
        Message<String> msg = MessageBuilder.<String>withTopic(topic).body("async-cb").build();

        AtomicReference<SendResult> resultRef = new AtomicReference<>();
        AtomicBoolean successCalled = new AtomicBoolean(false);
        template.asyncSend(
                msg,
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult result) {
                        resultRef.set(result);
                        successCalled.set(true);
                    }

                    @Override
                    public void onException(Throwable ex) {
                        // no-op
                    }
                });

        await().atMost(5, TimeUnit.SECONDS).untilTrue(successCalled);
        assertThat(resultRef.get()).isNotNull();
        assertThat(resultRef.get().isSuccess()).isTrue();
    }

    @Test
    @DisplayName("syncSendBatch:批量发送后批量消费")
    void syncSendBatch_batchConsume() {
        String topic = "tpl-batch-topic";
        BatchMessage<String> batch =
                BatchMessage.<String>withTopic(topic)
                        .add(MessageBuilder.<String>withTopic(topic).tag("b").body("m1").build())
                        .add(MessageBuilder.<String>withTopic(topic).tag("b").body("m2").build())
                        .add(MessageBuilder.<String>withTopic(topic).tag("b").body("m3").build())
                        .build();

        List<SendResult> results = template.syncSendBatch(batch);
        assertThat(results).hasSize(3);

        RedissonStreamListener consumer =
                new RedissonStreamListener(
                        redisson, namespace, topic, "tpl-batch-group", "c1", converter);
        createConsumerGroup(topic, "tpl-batch-group");
        try {
            List<Message<?>> messages = consumer.pull(10);
            assertThat(messages).hasSize(3);
        } finally {
            consumer.close();
        }
    }

    @Test
    @DisplayName("拦截器:beforeSend 与 afterSend 均被调用")
    void interceptor_beforeAndAfterCalled() {
        String topic = "tpl-interceptor-topic";
        AtomicBoolean beforeCalled = new AtomicBoolean(false);
        AtomicBoolean afterCalled = new AtomicBoolean(false);
        template.addProducerInterceptor(
                new ProducerInterceptor() {
                    @Override
                    public boolean beforeSend(Message<?> message) {
                        beforeCalled.set(true);
                        return true;
                    }

                    @Override
                    public void afterSend(Message<?> message, SendResult result) {
                        afterCalled.set(true);
                    }
                });

        Message<String> msg = MessageBuilder.<String>withTopic(topic).body("intercept").build();
        template.syncSend(msg);

        assertThat(beforeCalled).isTrue();
        assertThat(afterCalled).isTrue();
    }

    @Test
    @DisplayName("拦截器中止:beforeSend 返回 false 时消息不被发送")
    void interceptor_abort_blocksSend() {
        String topic = "tpl-abort-topic";
        template.addProducerInterceptor(
                new ProducerInterceptor() {
                    @Override
                    public boolean beforeSend(Message<?> message) {
                        return false;
                    }

                    @Override
                    public void afterSend(Message<?> message, SendResult result) {
                        // no-op
                    }
                });

        Message<String> msg = MessageBuilder.<String>withTopic(topic).body("aborted").build();
        SendResult result = template.syncSend(msg);

        assertThat(result.isSuccess()).isFalse();
        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        assertThat(stream.size()).isEqualTo(0L);
    }

    @Test
    @DisplayName("多拦截器按 order 升序执行")
    void multipleInterceptors_executedByOrder() {
        String topic = "tpl-multi-interceptor-topic";
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger firstOrder = new AtomicInteger(-1);
        AtomicInteger secondOrder = new AtomicInteger(-1);

        template.addProducerInterceptor(
                new ProducerInterceptor() {
                    @Override
                    public boolean beforeSend(Message<?> message) {
                        firstOrder.set(counter.incrementAndGet());
                        return true;
                    }

                    @Override
                    public void afterSend(Message<?> message, SendResult result) {}

                    @Override
                    public int order() {
                        return 1;
                    }
                });
        template.addProducerInterceptor(
                new ProducerInterceptor() {
                    @Override
                    public boolean beforeSend(Message<?> message) {
                        secondOrder.set(counter.incrementAndGet());
                        return true;
                    }

                    @Override
                    public void afterSend(Message<?> message, SendResult result) {}

                    @Override
                    public int order() {
                        return 2;
                    }
                });

        template.syncSend(MessageBuilder.<String>withTopic(topic).body("multi").build());

        assertThat(firstOrder.get()).isEqualTo(1);
        assertThat(secondOrder.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("多 Topic 场景:发送到不同 Topic 分别消费")
    void multiTopic_sendAndConsumeSeparately() {
        String topicA = "tpl-topic-a";
        String topicB = "tpl-topic-b";

        template.syncSend(MessageBuilder.<String>withTopic(topicA).body("a-body").build());
        template.syncSend(MessageBuilder.<String>withTopic(topicB).body("b-body").build());

        RedissonStreamListener consumerA =
                new RedissonStreamListener(redisson, namespace, topicA, "grp-a", "c1", converter);
        RedissonStreamListener consumerB =
                new RedissonStreamListener(redisson, namespace, topicB, "grp-b", "c1", converter);
        createConsumerGroup(topicA, "grp-a");
        createConsumerGroup(topicB, "grp-b");
        try {
            List<Message<?>> fromA = consumerA.pull(1);
            List<Message<?>> fromB = consumerB.pull(1);

            assertThat(fromA).hasSize(1);
            assertThat(fromA.get(0).getBody()).isEqualTo("a-body");
            assertThat(fromB).hasSize(1);
            assertThat(fromB.get(0).getBody()).isEqualTo("b-body");
        } finally {
            consumerA.close();
            consumerB.close();
        }
    }

    @Test
    @DisplayName("asyncSend(CompletableFuture) 完成后消息写入 Stream")
    void asyncSendFuture_writesToStream() {
        String topic = "tpl-future-topic";
        Message<String> msg = MessageBuilder.<String>withTopic(topic).body("future-body").build();

        SendResult result = template.asyncSend(msg).orTimeout(5, TimeUnit.SECONDS).join();

        assertThat(result.isSuccess()).isTrue();
        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        assertThat(stream.size()).isEqualTo(1L);
    }

    @Test
    @DisplayName("asyncSend(SendOptions) 应用超时/重试参数并写入真实 Entry ID")
    void asyncSendWithSendOptions_appliesOptions() {
        String topic = "tpl-options-topic";
        Message<String> msg =
                MessageBuilder.<String>withTopic(topic).keys("opt-key").body("opt-body").build();

        SendOptions options = SendOptions.builder().timeoutMillis(5000).retryTimes(1).build();
        SendResult result = template.asyncSend(msg, options).orTimeout(10, TimeUnit.SECONDS).join();

        assertThat(result.isSuccess()).isTrue();
        // 返回真实 Stream Entry ID
        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        Map<StreamMessageId, Map<String, String>> entries =
                stream.range(StreamMessageId.MIN, StreamMessageId.MAX);
        assertThat(entries.keySet())
                .anyMatch(id -> id.toString().equals(result.getMessageId().getStreamEntryId()));
    }

    @Test
    @DisplayName("sendOneway 消息最终写入 Stream")
    void sendOneway_writesToStream() {
        String topic = "tpl-oneway-topic";
        Message<String> msg = MessageBuilder.<String>withTopic(topic).body("oneway").build();

        template.sendOneway(msg);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            RStream<String, String> stream =
                                    redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
                            assertThat(stream.size()).isEqualTo(1L);
                        });
    }

    @Test
    @DisplayName("拦截器可在 beforeSend 中修改消息属性")
    void interceptor_canModifyMessageProperties() {
        String topic = "tpl-modify-topic";
        template.addProducerInterceptor(
                new ProducerInterceptor() {
                    @Override
                    public boolean beforeSend(Message<?> message) {
                        message.putUserProperty("injected", "yes");
                        return true;
                    }

                    @Override
                    public void afterSend(Message<?> message, SendResult result) {}
                });

        template.syncSend(MessageBuilder.<String>withTopic(topic).body("modified").build());

        RedissonStreamListener consumer =
                new RedissonStreamListener(
                        redisson, namespace, topic, "tpl-modify-group", "c1", converter);
        createConsumerGroup(topic, "tpl-modify-group");
        try {
            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getUserProperties()).containsEntry("injected", "yes");
        } finally {
            consumer.close();
        }
    }
}
