package io.github.streammq.core.service;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link StreamMessageService} 单元测试，验证各便捷发送方法正确构造 {@link Message} 并委派给 {@link StreamMessageTemplate}。
 *
 * <p>使用 Mockito 模拟 {@link StreamMessageTemplate}，通过 {@link ArgumentCaptor} 捕获传入的消息，
 * 校验 topic / body / tag / keys / shardingKey / 延时参数是否正确设置。
 */
@DisplayName("StreamMqService 便捷发送服务测试")
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"rawtypes", "unchecked"})
class StreamMessageServiceTest {

    @Mock
    private StreamMessageTemplate template;

    private StreamMessageService mqService;

    @BeforeEach
    void setUp() {
        mqService = new StreamMessageService(template);
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
            when(template.syncSend(any(Message.class), org.mockito.ArgumentMatchers.eq(5000L)))
                .thenReturn(expected);

            SendResult result = mqService.send("orders", "payload", 5000L);

            ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
            verify(template).syncSend(captor.capture(), org.mockito.ArgumentMatchers.eq(5000L));
            assertThat(captor.getValue().getTopic()).isEqualTo("orders");
            assertThat(captor.getValue().getBody()).isEqualTo("payload");
            assertThat(result).isSameAs(expected);
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
    }

    @Nested
    @DisplayName("单向发送 sendOneway")
    class OnewaySend {

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
            when(template.syncSendBatch(any(BatchMessage.class)))
                .thenReturn(List.of(okResult()));

            mqService.sendBatch("orders", "tagA", List.of("a"));

            ArgumentCaptor<BatchMessage> captor = ArgumentCaptor.forClass(BatchMessage.class);
            verify(template).syncSendBatch(captor.capture());
            BatchMessage batch = captor.getValue();
            List<Message> messages = batch.getMessages();
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).getTag()).isEqualTo("tagA");
        }

        @Test
        @DisplayName("sendBatch 传入 null 抛 NullPointerException")
        void sendBatchNullThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bodies");
        }

        @Test
        @DisplayName("sendBatch 传入空列表抛 IllegalArgumentException")
        void sendBatchEmptyThrows() {
            assertThatThrownBy(() -> mqService.sendBatch("orders", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
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
    }
}
