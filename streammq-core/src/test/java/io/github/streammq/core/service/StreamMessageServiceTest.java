package io.github.streammq.core.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.*;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * {@link StreamMessageService} 单元测试，验证各便捷发送方法正确构造 {@link Message} 并委派给 {@link
 * StreamMessageTemplate}。
 *
 * <p>测试针对 {@link StreamMessageService} 接口，使用默认实现 {@link DefaultStreamMessageService} 进行验证。 使用
 * Mockito 模拟 {@link StreamMessageTemplate}，通过 {@link ArgumentCaptor} 捕获传入的消息， 校验 topic / body / tag
 * / keys / shardingKey / 延时参数是否正确设置。
 */
@DisplayName("StreamMQService 便捷发送服务测试")
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"rawtypes", "unchecked"})
class StreamMessageServiceTest {

    @Mock private StreamMessageTemplate template;

    private StreamMessageService mqService;

    @BeforeEach
    void setUp() {
        mqService = new DefaultStreamMessageService(template);
    }

    private static SendResult okResult() {
        return new SendResult(new MessageId("1-0"), "orders", null, 0L);
    }

    @Nested
    @DisplayName("同步发送 send")
    class SyncSend {

        @Test
        @DisplayName("send(topic, body) 构造正确 topic 与 body 的消息并返回模板结果")
        void sendTopicAndBody() {
            SendResult expected = okResult();
            when(template.syncSend(any(Message.class))).thenReturn(expected);

            SendResult result = mqService.send("orders", "payload");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTopic()).isEqualTo("orders");
            assertThat(msg.getBody()).isEqualTo("payload");
            assertThat(msg.getTag()).isNull();
            assertThat(msg.getKeys()).isNull();
            assertThat(result).isSameAs(expected);
        }

        @Test
        @DisplayName("send(topic, body, tag) 设置 tag")
        void sendWithTag() {
            mqService.send("orders", "payload", "tagA");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTopic()).isEqualTo("orders");
            assertThat(msg.getBody()).isEqualTo("payload");
            assertThat(msg.getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("send(topic, body, tag, keys) 设置 tag 与 keys")
        void sendWithTagAndKeys() {
            mqService.send("orders", "payload", "tagA", "order-key-1");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getKeys()).isEqualTo("order-key-1");
        }

        @Test
        @DisplayName("send(topic, body, tag, keys, shardingKey) 设置 shardingKey")
        void sendWithShardingKey() {
            mqService.send("orders", "payload", "tagA", "k1", "shard-1");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getKeys()).isEqualTo("k1");
            assertThat(msg.getShardingKey()).isEqualTo("shard-1");
        }

        @Test
        @DisplayName("send(topic, body, timeoutMillis) 调用带超时的 syncSend 并返回结果")
        void sendWithTimeout() {
            SendResult expected = okResult();
            when(template.syncSend(any(Message.class), eq(5000L))).thenReturn(expected);

            SendResult result = mqService.send("orders", "payload", 5000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(5000L));
            assertThat(captor.getValue().getTopic()).isEqualTo("orders");
            assertThat(captor.getValue().getBody()).isEqualTo("payload");
            assertThat(result).isSameAs(expected);
        }

        @Test
        @DisplayName("send(Message<T>) 委派 template.syncSend(Message)")
        void sendMessageObject() {
            SendResult expected = okResult();
            when(template.syncSend(any(Message.class))).thenReturn(expected);

            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("p").build();
            SendResult result = mqService.send(msg);

            assertThat(result).isSameAs(expected);
            verify(template).syncSend(msg);
        }

        @Test
        @DisplayName("send(Message<T>, timeout) 委派带超时的 syncSend")
        void sendMessageObjectWithTimeout() {
            SendResult expected = okResult();
            when(template.syncSend(any(Message.class), eq(2000L))).thenReturn(expected);

            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("p").build();
            mqService.send(msg, 2000L);

            verify(template).syncSend(msg, 2000L);
        }

        @Test
        @DisplayName("send(Message<T>, timeout, retry) 委派带超时和重试的 syncSend")
        void sendMessageObjectWithTimeoutAndRetry() {
            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("p").build();
            when(template.syncSend(any(Message.class), eq(2000L), eq(3))).thenReturn(okResult());

            mqService.send(msg, 2000L, 3);

            verify(template).syncSend(msg, 2000L, 3);
        }

        @Test
        @DisplayName("send(topic, body, tag, timeout) 同时设置 tag 与超时")
        void sendWithTagAndTimeout() {
            when(template.syncSend(any(Message.class), eq(1000L))).thenReturn(okResult());

            mqService.send("orders", "p", "tagA", 1000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L));
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("send(topic, body, tag, keys, timeout) 同时设置 tag/keys/超时")
        void sendWithTagKeysTimeout() {
            when(template.syncSend(any(Message.class), eq(1000L))).thenReturn(okResult());

            mqService.send("orders", "p", "tagA", "k1", 1000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L));
            assertThat(captor.getValue().getKeys()).isEqualTo("k1");
        }

        @Test
        @DisplayName("send(topic, body, tag, keys, shardingKey, timeout) 完整参数 + 超时")
        void sendWithAllParamsAndTimeout() {
            when(template.syncSend(any(Message.class), eq(1000L))).thenReturn(okResult());

            mqService.send("orders", "p", "tagA", "k1", "shard-1", 1000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L));
            assertThat(captor.getValue().getShardingKey()).isEqualTo("shard-1");
        }

        @Test
        @DisplayName("send(topic, body, timeout, retry) 设置超时和重试")
        void sendWithTimeoutAndRetry() {
            when(template.syncSend(any(Message.class), eq(1000L), eq(2))).thenReturn(okResult());

            mqService.send("orders", "p", 1000L, 2);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L), eq(2));
        }

        @Test
        @DisplayName("send(topic, body, tag, timeout, retry) 设置 tag + 超时 + 重试")
        void sendWithTagTimeoutRetry() {
            when(template.syncSend(any(Message.class), eq(1000L), eq(2))).thenReturn(okResult());

            mqService.send("orders", "p", "tagA", 1000L, 2);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L), eq(2));
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("send(topic, body, tag, keys, timeout, retry) 完整参数 + 超时 + 重试")
        void sendWithTagKeysTimeoutRetry() {
            when(template.syncSend(any(Message.class), eq(1000L), eq(2))).thenReturn(okResult());

            mqService.send("orders", "p", "tagA", "k1", 1000L, 2);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L), eq(2));
            assertThat(captor.getValue().getKeys()).isEqualTo("k1");
        }

        @Test
        @DisplayName("send(topic, body, tag, keys, shardingKey, timeout, retry) 全参数")
        void sendWithAllParamsTimeoutRetry() {
            when(template.syncSend(any(Message.class), eq(1000L), eq(2))).thenReturn(okResult());

            mqService.send("orders", "p", "tagA", "k1", "shard-1", 1000L, 2);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(1000L), eq(2));
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getKeys()).isEqualTo("k1");
            assertThat(msg.getShardingKey()).isEqualTo("shard-1");
        }
    }

    @Nested
    @DisplayName("MessageMetadataBuilder 模式发送")
    class MetadataSend {

        @Test
        @DisplayName("send(topic, body, metadata) 将元数据应用到消息")
        void sendWithMetadata() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create()
                            .tag("tagA")
                            .keys("k1")
                            .shardingKey("shard-1")
                            .userProperty("traceId", "t-001");

            mqService.send("orders", "p", metadata);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTopic()).isEqualTo("orders");
            assertThat(msg.getBody()).isEqualTo("p");
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getKeys()).isEqualTo("k1");
            assertThat(msg.getShardingKey()).isEqualTo("shard-1");
            assertThat(msg.getUserProperties()).containsEntry("traceId", "t-001");
        }

        @Test
        @DisplayName("send(topic, body, null) metadata 为 null 时仅构造 topic+body")
        void sendWithNullMetadata() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            mqService.send("orders", "p", (MessageMetadataBuilder) null);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            assertThat(captor.getValue().getTag()).isNull();
            assertThat(captor.getValue().getKeys()).isNull();
        }

        @Test
        @DisplayName("send(topic, body, metadata, timeout) 应用元数据 + 超时")
        void sendWithMetadataAndTimeout() {
            when(template.syncSend(any(Message.class), eq(2000L))).thenReturn(okResult());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("tagA").delayLevel(DelayLevel.SECOND_5);

            mqService.send("orders", "p", metadata, 2000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(2000L));
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
            assertThat(captor.getValue().getDelayLevel()).isEqualTo(DelayLevel.SECOND_5);
        }

        @Test
        @DisplayName("send(topic, body, metadata, timeout, retry) 应用元数据 + 超时 + 重试")
        void sendWithMetadataTimeoutRetry() {
            when(template.syncSend(any(Message.class), eq(2000L), eq(3))).thenReturn(okResult());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().keys("k1").userProperty("traceId", "t-001");

            mqService.send("orders", "p", metadata, 2000L, 3);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(2000L), eq(3));
            assertThat(captor.getValue().getKeys()).isEqualTo("k1");
            assertThat(captor.getValue().getUserProperties()).containsEntry("traceId", "t-001");
        }
    }

    @Nested
    @DisplayName("异步发送 asyncSend")
    class AsyncSend {

        @Test
        @DisplayName("asyncSend(topic, body) 委派 template.asyncSend 并返回其 Future")
        void asyncSendTopicAndBody() {
            CompletableFuture<SendResult> expected = CompletableFuture.completedFuture(okResult());
            when(template.asyncSend(any(Message.class))).thenReturn(expected);

            CompletableFuture<SendResult> future = mqService.asyncSend("orders", "payload");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTopic()).isEqualTo("orders");
            assertThat(msg.getBody()).isEqualTo("payload");
            assertThat(future).isSameAs(expected);
        }

        @Test
        @DisplayName("asyncSend(topic, body, tag) 设置 tag")
        void asyncSendWithTag() {
            when(template.asyncSend(any(Message.class)))
                    .thenReturn(CompletableFuture.completedFuture(okResult()));

            mqService.asyncSend("orders", "payload", "tagA");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture());
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("asyncSend(topic, body, tag, keys) 设置 tag 与 keys")
        void asyncSendWithTagAndKeys() {
            when(template.asyncSend(any(Message.class)))
                    .thenReturn(CompletableFuture.completedFuture(okResult()));

            mqService.asyncSend("orders", "p", "tagA", "k1");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture());
            assertThat(captor.getValue().getKeys()).isEqualTo("k1");
        }

        @Test
        @DisplayName("asyncSend(topic, body, tag, keys, shardingKey) 完整参数")
        void asyncSendWithAllParams() {
            when(template.asyncSend(any(Message.class)))
                    .thenReturn(CompletableFuture.completedFuture(okResult()));

            mqService.asyncSend("orders", "p", "tagA", "k1", "shard-1");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getShardingKey()).isEqualTo("shard-1");
        }

        @Test
        @DisplayName("asyncSend(Message<T>) 委派 template.asyncSend(Message)")
        void asyncSendMessageObject() {
            CompletableFuture<SendResult> expected = CompletableFuture.completedFuture(okResult());
            when(template.asyncSend(any(Message.class))).thenReturn(expected);

            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("p").build();
            CompletableFuture<SendResult> future = mqService.asyncSend(msg);

            assertThat(future).isSameAs(expected);
            verify(template).asyncSend(msg);
        }

        @Test
        @DisplayName("asyncSend(topic, body, metadata) 应用元数据")
        void asyncSendWithMetadata() {
            when(template.asyncSend(any(Message.class)))
                    .thenReturn(CompletableFuture.completedFuture(okResult()));

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("tagA").userProperty("traceId", "t-001");

            mqService.asyncSend("orders", "p", metadata);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getUserProperties()).containsEntry("traceId", "t-001");
        }

        @Test
        @DisplayName("asyncSend(topic, body, callback) 委派回调版 asyncSend")
        void asyncSendWithCallback() {
            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("p").build();
            SendCallback cb =
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult r) {}

                        @Override
                        public void onException(Throwable e) {}
                    };

            mqService.asyncSend("orders", "p", cb);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture(), eq(cb));
        }

        @Test
        @DisplayName("asyncSend(topic, body, tag, callback) 设置 tag + 回调")
        void asyncSendWithTagAndCallback() {
            SendCallback cb =
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult r) {}

                        @Override
                        public void onException(Throwable e) {}
                    };

            mqService.asyncSend("orders", "p", "tagA", cb);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture(), eq(cb));
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("asyncSend(topic, body, callback, timeout) 设置回调 + 超时")
        void asyncSendWithCallbackAndTimeout() {
            SendCallback cb =
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult r) {}

                        @Override
                        public void onException(Throwable e) {}
                    };

            mqService.asyncSend("orders", "p", cb, 2000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture(), eq(cb), eq(2000L));
        }

        @Test
        @DisplayName("asyncSend(topic, body, metadata, callback) 应用元数据 + 回调")
        void asyncSendWithMetadataAndCallback() {
            SendCallback cb =
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult r) {}

                        @Override
                        public void onException(Throwable e) {}
                    };
            MessageMetadataBuilder metadata = MessageMetadataBuilder.create().tag("tagA");

            mqService.asyncSend("orders", "p", metadata, cb);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture(), eq(cb));
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("asyncSend(topic, body, metadata, callback, timeout) 完整参数")
        void asyncSendWithMetadataCallbackTimeout() {
            SendCallback cb =
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult r) {}

                        @Override
                        public void onException(Throwable e) {}
                    };
            MessageMetadataBuilder metadata = MessageMetadataBuilder.create().keys("k1");

            mqService.asyncSend("orders", "p", metadata, cb, 2000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).asyncSend(captor.capture(), eq(cb), eq(2000L));
            assertThat(captor.getValue().getKeys()).isEqualTo("k1");
        }
    }

    @Nested
    @DisplayName("单向发送 sendOneway")
    class OnewaySend {

        @Test
        @DisplayName("sendOneway(topic, body) 委派 template.sendOneway")
        void sendOnewayTopicBody() {
            mqService.sendOneway("orders", "payload");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).sendOneway(captor.capture());
            assertThat(captor.getValue().getTopic()).isEqualTo("orders");
            assertThat(captor.getValue().getBody()).isEqualTo("payload");
        }

        @Test
        @DisplayName("sendOneway(topic, body, tag) 委派 template.sendOneway 并设置 tag")
        void sendOnewayWithTag() {
            mqService.sendOneway("orders", "payload", "tagA");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).sendOneway(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTopic()).isEqualTo("orders");
            assertThat(msg.getBody()).isEqualTo("payload");
            assertThat(msg.getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("sendOneway(topic, body, tag, keys) 设置 tag 与 keys")
        void sendOnewayWithTagKeys() {
            mqService.sendOneway("orders", "p", "tagA", "k1");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).sendOneway(captor.capture());
            assertThat(captor.getValue().getKeys()).isEqualTo("k1");
        }

        @Test
        @DisplayName("sendOneway(topic, body, tag, keys, shardingKey) 完整参数")
        void sendOnewayWithAllParams() {
            mqService.sendOneway("orders", "p", "tagA", "k1", "shard-1");

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).sendOneway(captor.capture());
            assertThat(captor.getValue().getShardingKey()).isEqualTo("shard-1");
        }

        @Test
        @DisplayName("sendOneway(Message<T>) 委派 template.sendOneway(Message)")
        void sendOnewayMessageObject() {
            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("p").build();

            mqService.sendOneway(msg);

            verify(template).sendOneway(msg);
        }

        @Test
        @DisplayName("sendOneway(topic, body, metadata) 应用元数据")
        void sendOnewayWithMetadata() {
            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("tagA").shardingKey("shard-1");

            mqService.sendOneway("orders", "p", metadata);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).sendOneway(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getShardingKey()).isEqualTo("shard-1");
        }
    }

    @Nested
    @DisplayName("批量发送 sendBatch")
    class BatchSend {

        @Test
        @DisplayName("sendBatch(topic, bodies) 构造同 Topic 的 BatchMessage 并委派 syncSendBatch")
        void sendBatchSameTopic() {
            List<SendResult> expected = List.of(okResult(), okResult());
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(expected);

            List<SendResult> results = mqService.sendBatch("orders", List.of("a", "b"));

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            BatchMessage batch = captor.getValue();
            List<Message> messages = batch.getMessages();
            assertThat(batch.getTopic()).isEqualTo("orders");
            assertThat(messages).hasSize(2);
            assertThat(messages.get(0).getBody()).isEqualTo("a");
            assertThat(messages.get(1).getBody()).isEqualTo("b");
            assertThat(results).isSameAs(expected);
        }

        @Test
        @DisplayName("sendBatch(topic, tag, bodies) 为每条消息设置 tag")
        void sendBatchWithTag() {
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(List.of(okResult()));

            mqService.sendBatch("orders", "tagA", List.of("a"));

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            BatchMessage batch = captor.getValue();
            List<Message> messages = batch.getMessages();
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("sendBatch(BatchMessage<T>) 委派 template.syncSendBatch")
        void sendBatchObject() {
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("orders")
                            .add(MessageBuilder.<String>withTopic("orders").body("a").build())
                            .build();
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(List.of(okResult()));

            mqService.sendBatch(batch);

            verify(template).syncSendBatch(batch);
        }

        @Test
        @DisplayName("sendBatch(topic, bodies, metadata) 为每条消息应用元数据")
        void sendBatchWithMetadata() {
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(List.of(okResult()));

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("tagA").keys("k1");

            mqService.sendBatch("orders", List.of("a"), metadata);

            ArgumentCaptor<BatchMessage<String>> captor =
                    ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            Message<String> msg = captor.getValue().getMessages().get(0);
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getKeys()).isEqualTo("k1");
        }

        @Test
        @DisplayName("sendBatch 传入 null 抛 NullPointerException")
        void sendBatchNullThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", (List<String>) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("bodies");
        }

        @Test
        @DisplayName("sendBatch 传入空列表抛 IllegalArgumentException")
        void sendBatchEmptyThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", List.<String>of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty");
        }

        @Test
        @DisplayName("sendBatch(topic, tag, null) 抛 NullPointerException")
        void sendBatchWithTagNullThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", "tagA", (List<String>) null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("sendBatch(topic, tag, []) 抛 IllegalArgumentException")
        void sendBatchWithTagEmptyThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", "tagA", List.<String>of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("sendBatch(topic, null, metadata) 抛 NullPointerException")
        void sendBatchWithMetadataNullThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", (List<String>) null, null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("sendBatch(List<Message<T>>) 直接发送消息列表，取首条 Topic")
        void sendBatchMessageList() {
            when(template.syncSendBatch(any(BatchMessage.class)))
                    .thenReturn(List.of(okResult(), okResult()));

            List<Message<String>> messages =
                    List.of(
                            MessageBuilder.<String>withTopic("orders").body("a").build(),
                            MessageBuilder.<String>withTopic("orders").body("b").build());
            mqService.sendBatch(messages);

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            assertThat(captor.getValue().getTopic()).isEqualTo("orders");
            assertThat(captor.getValue().size()).isEqualTo(2);
        }

        @Test
        @DisplayName("sendBatch(List<Message<T>>) 传入 null 抛 NPE")
        void sendBatchMessageListNullThrows() {
            assertThatThrownBy(() -> mqService.sendBatch((List<Message<String>>) null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("sendBatch(List<Message<T>>) 传入空列表抛 IAE")
        void sendBatchMessageListEmptyThrows() {
            assertThatThrownBy(() -> mqService.sendBatch(List.<Message<String>>of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("sendBatch(String topic, Message<T>...) varargs 覆盖每条消息的 Topic")
        void sendBatchTopicVarargsOverride() {
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(List.of(okResult()));

            Message<String> m1 = MessageBuilder.<String>withTopic("old-topic").body("a").build();
            mqService.sendBatch("new-topic", m1);

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            BatchMessage<String> batch = captor.getValue();
            assertThat(batch.getTopic()).isEqualTo("new-topic");
            assertThat(batch.getMessages().get(0).getTopic()).isEqualTo("new-topic");
            assertThat(batch.getMessages().get(0).getBody()).isEqualTo("a");
        }

        @Test
        @DisplayName("sendBatch(String topic, Message<T>...) topic 一致时不重建消息")
        void sendBatchTopicVarargsSameTopic() {
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(List.of(okResult()));

            Message<String> msg = MessageBuilder.<String>withTopic("orders").body("a").build();
            mqService.sendBatch("orders", msg);

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            // topic 一致时应直接复用原消息对象
            assertThat(captor.getValue().getMessages().get(0)).isSameAs(msg);
        }

        @Test
        @DisplayName("sendBatch(Message<T>...) varargs 发送")
        void sendBatchVarargs() {
            when(template.syncSendBatch(any(BatchMessage.class)))
                    .thenReturn(List.of(okResult(), okResult()));

            Message<String> m1 = MessageBuilder.<String>withTopic("orders").body("a").build();
            Message<String> m2 = MessageBuilder.<String>withTopic("orders").body("b").build();
            mqService.sendBatch(m1, m2);

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            assertThat(captor.getValue().size()).isEqualTo(2);
        }

        @Test
        @DisplayName("sendBatch(Message<T>...) 空数组抛 IAE")
        void sendBatchVarargsEmptyThrows() {
            Message<String>[] empty = (Message<String>[]) new Message[0];
            assertThatThrownBy(() -> mqService.sendBatch(empty))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("sendBatch(String topic, Message<T>...) 空数组抛 IAE")
        void sendBatchTopicVarargsEmptyThrows() {
            Message<String>[] empty = (Message<String>[]) new Message[0];
            assertThatThrownBy(() -> mqService.sendBatch("orders", empty))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("sendBatch(String topic, timeout, retry, Message<T>...) varargs + 超时重试")
        void sendBatchTopicTimeoutRetryVarargs() {
            when(template.syncSendBatch(any(BatchMessage.class), anyLong(), anyInt()))
                    .thenReturn(List.of(okResult()));

            Message<String> m1 = MessageBuilder.<String>withTopic("orders").body("a").build();
            mqService.sendBatch("orders", 1000L, 2, m1);

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture(), eq(1000L), eq(2));
            assertThat(captor.getValue().size()).isEqualTo(1);
        }

        @Test
        @DisplayName("sendBatch(List<Message<T>>, timeout, retry) 透传超时与重试参数")
        void sendBatchMessageListTimeoutRetry() {
            when(template.syncSendBatch(any(BatchMessage.class), anyLong(), anyInt()))
                    .thenReturn(List.of(okResult()));

            List<Message<String>> messages =
                    List.of(MessageBuilder.<String>withTopic("orders").body("a").build());
            mqService.sendBatch(messages, 1000L, 2);

            verify(template).syncSendBatch(any(BatchMessage.class), eq(1000L), eq(2));
        }

        @Test
        @DisplayName(
                "sendBatch(String topic, Message<T>...) 保留消息所有属性（Tag/Keys/ShardingKey/Properties）")
        void sendBatchTopicVarargs_preservesAllFields() {
            when(template.syncSendBatch(any(BatchMessage.class))).thenReturn(List.of(okResult()));

            Message<String> original =
                    MessageBuilder.<String>withTopic("old")
                            .tag("tagA")
                            .keys("k1")
                            .shardingKey("shard-1")
                            .body("payload")
                            .withUserProperty("traceId", "t-001")
                            .build();
            mqService.sendBatch("new-topic", original);

            ArgumentCaptor<BatchMessage<String>> captor =
                    ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            Message<String> sent = captor.getValue().getMessages().get(0);
            assertThat(sent.getTopic()).isEqualTo("new-topic");
            assertThat(sent.getTag()).isEqualTo("tagA");
            assertThat(sent.getKeys()).isEqualTo("k1");
            assertThat(sent.getShardingKey()).isEqualTo("shard-1");
            assertThat(sent.getBody()).isEqualTo("payload");
            assertThat(sent.getUserProperties()).containsEntry("traceId", "t-001");
            // 原始消息不应被修改
            assertThat(original.getTopic()).isEqualTo("old");
        }
    }

    @Nested
    @DisplayName("延时发送 sendDelay")
    class DelaySend {

        @Test
        @DisplayName("sendDelay(topic, body, DelayLevel) 设置 delayLevel")
        void sendDelayWithLevel() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            mqService.sendDelay("orders", "payload", DelayLevel.SECOND_5);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTopic()).isEqualTo("orders");
            assertThat(msg.getBody()).isEqualTo("payload");
            assertThat(msg.getDelayLevel()).isEqualTo(DelayLevel.SECOND_5);
        }

        @Test
        @DisplayName("sendDelay(topic, body, delayTimeMillis) 设置 delayTimeMillis")
        void sendDelayWithMillis() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            mqService.sendDelay("orders", "payload", 3000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getDelayTimeMillis()).isEqualTo(3000L);
        }

        @Test
        @DisplayName("sendDelay(topic, body, tag, DelayLevel) 设置 tag + delayLevel")
        void sendDelayWithTagAndLevel() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            mqService.sendDelay("orders", "p", "tagA", DelayLevel.SECOND_5);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getDelayLevel()).isEqualTo(DelayLevel.SECOND_5);
        }

        @Test
        @DisplayName("sendDelay(topic, body, tag, delayTimeMillis) 设置 tag + delayTimeMillis")
        void sendDelayWithTagAndMillis() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            mqService.sendDelay("orders", "p", "tagA", 3000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getDelayTimeMillis()).isEqualTo(3000L);
        }

        @Test
        @DisplayName("sendDelay(topic, body, metadata) 应用元数据中的延时设置")
        void sendDelayWithMetadata() {
            when(template.syncSend(any(Message.class))).thenReturn(okResult());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("tagA").delayLevel(DelayLevel.SECOND_5);

            mqService.sendDelay("orders", "p", metadata);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture());
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getDelayLevel()).isEqualTo(DelayLevel.SECOND_5);
        }

        @Test
        @DisplayName("sendDelay(topic, body, metadata, timeout) 应用元数据 + 超时")
        void sendDelayWithMetadataAndTimeout() {
            when(template.syncSend(any(Message.class), eq(2000L))).thenReturn(okResult());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().delayTimeMillis(5000L);

            mqService.sendDelay("orders", "p", metadata, 2000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), eq(2000L));
            assertThat(captor.getValue().getDelayTimeMillis()).isEqualTo(5000L);
        }
    }

    @Nested
    @DisplayName("事务消息 sendTransaction")
    class TransactionSend {

        @Test
        @DisplayName("sendTransaction(topic, body, callback) 委派 executeInTransaction")
        void sendTransactionTopicBody() {
            SendResult expected = okResult();
            when(template.executeInTransaction(any(Message.class), any(TransactionCallback.class)))
                    .thenReturn(expected);
            TransactionCallback<String> cb = (msg, ctx) -> LocalTransactionState.COMMIT_MESSAGE;

            SendResult result = mqService.sendTransaction("orders", "p", cb);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).executeInTransaction(captor.capture(), eq(cb));
            assertThat(captor.getValue().getTopic()).isEqualTo("orders");
            assertThat(captor.getValue().getBody()).isEqualTo("p");
            assertThat(result).isSameAs(expected);
        }

        @Test
        @DisplayName("sendTransaction(topic, body, tag, callback) 设置 tag")
        void sendTransactionWithTag() {
            when(template.executeInTransaction(any(Message.class), any(TransactionCallback.class)))
                    .thenReturn(okResult());
            TransactionCallback<String> cb = (msg, ctx) -> LocalTransactionState.COMMIT_MESSAGE;

            mqService.sendTransaction("orders", "p", "tagA", cb);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).executeInTransaction(captor.capture(), eq(cb));
            assertThat(captor.getValue().getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("sendTransaction(topic, body, metadata, callback) 应用元数据")
        void sendTransactionWithMetadata() {
            when(template.executeInTransaction(any(Message.class), any(TransactionCallback.class)))
                    .thenReturn(okResult());
            TransactionCallback<String> cb = (msg, ctx) -> LocalTransactionState.COMMIT_MESSAGE;
            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().tag("tagA").keys("k1");

            mqService.sendTransaction("orders", "p", metadata, cb);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).executeInTransaction(captor.capture(), eq(cb));
            Message msg = captor.getValue();
            assertThat(msg.getTag()).isEqualTo("tagA");
            assertThat(msg.getKeys()).isEqualTo("k1");
        }
    }
}
