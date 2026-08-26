/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendOptions;
import io.github.streammq.core.message.SendResult;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * {@link DefaultStreamMessageService} 单元测试（0.1.0 收敛后的门面）。
 *
 * <p>验证 Topic+Metadata 形态到 Message 的装配正确性，以及 Message 形态/批量/事务对
 * {@code StreamMessageTemplate} 的透传。
 */
@DisplayName("DefaultStreamMessageService 收敛门面测试")
@SuppressWarnings("unchecked")
class StreamMessageServiceTest {

    private io.github.streammq.core.template.StreamMessageTemplate template;
    private DefaultStreamMessageService service;

    @BeforeEach
    void setUp() {
        template = mock(io.github.streammq.core.template.StreamMessageTemplate.class);
        service = new DefaultStreamMessageService(template);
    }

    private static SendResult ok() {
        return new SendResult(new io.github.streammq.core.message.MessageId("1-0"), "t", null, 0L);
    }

    @Nested
    @DisplayName("Topic + Metadata 形态")
    class TopicForm {

        @Test
        @DisplayName("send(topic, body) 装配 Message 并按默认参数发送")
        @SuppressWarnings("unchecked")
        void sendTopicBody() {
            when(template.syncSend(any(), any())).thenReturn(ok());

            service.send("order-topic", "body-1");

            @SuppressWarnings("unchecked")
            ArgumentCaptor<Message<String>> msgCaptor =
                    ArgumentCaptor.forClass((Class) Message.class);
            verify(template).syncSend(msgCaptor.capture(), same(SendOptions.defaults()));
            assertThat(msgCaptor.getValue().getTopic()).isEqualTo("order-topic");
            assertThat(msgCaptor.getValue().getBody()).isEqualTo("body-1");
        }

        @Test
        @DisplayName("metadata 的 Tag/Keys/ShardingKey/延时/用户属性全部落到 Message")
        @SuppressWarnings("unchecked")
        void sendTopicBodyWithMetadata() {
            when(template.syncSend(any(), any())).thenReturn(ok());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create()
                            .tag("created")
                            .keys("order-1")
                            .shardingKey("order-1")
                            .delayLevel(DelayLevel.MINUTE_5)
                            .userProperty("traceId", "t-1");
            service.send("order-topic", "body-2", metadata);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<Message<String>> msgCaptor =
                    ArgumentCaptor.forClass((Class) Message.class);
            verify(template).syncSend(msgCaptor.capture(), any());
            Message<String> msg = msgCaptor.getValue();
            assertThat(msg.getTag()).isEqualTo("created");
            assertThat(msg.getKeys()).isEqualTo("order-1");
            assertThat(msg.getShardingKey()).isEqualTo("order-1");
            assertThat(msg.isDelayMessage()).isTrue();
            assertThat(msg.getUserProperties()).containsEntry("traceId", "t-1");
        }

        @Test
        @DisplayName("metadata 的超时/重试转换为 SendOptions 生效")
        void sendTopicBodyWithTimeoutAndRetry() {
            when(template.syncSend(any(), any())).thenReturn(ok());

            MessageMetadataBuilder metadata =
                    MessageMetadataBuilder.create().timeoutMillis(5000).retryTimes(3);
            service.send("t", "b", metadata);

            ArgumentCaptor<SendOptions> optCaptor = ArgumentCaptor.forClass(SendOptions.class);
            verify(template).syncSend(any(), optCaptor.capture());
            assertThat(optCaptor.getValue().effectiveTimeoutMillis()).isEqualTo(5000);
            assertThat(optCaptor.getValue().effectiveRetryTimes()).isEqualTo(3);
        }

        @Test
        @DisplayName("asyncSend(topic, body, metadata) 透传模板")
        void asyncSendTopicForm() {
            CompletableFuture<SendResult> future =
                    CompletableFuture.completedFuture(ok());
            when(template.asyncSend(any(Message.class), any(SendOptions.class)))
                    .thenReturn(future);

            assertThat(service.asyncSend("t", "b").join()).isNotNull();

            verify(template)
                    .asyncSend(
                            any(Message.class),
                            any(io.github.streammq.core.message.SendOptions.class));
        }

        @Test
        @DisplayName("topic 为空抛出 NullPointerException")
        void nullTopicRejected() {
            assertThatThrownBy(() -> service.send(null, "body"))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("Message 形态与透传")
    class MessageForm {

        @Test
        @DisplayName("send(message, options) 原样透传")
        void sendMessageWithOptions() {
            when(template.syncSend(any(), any())).thenReturn(ok());
            Message<String> msg = MessageBuilder.<String>withTopic("t").body("b").build();
            SendOptions options = SendOptions.of(1000L, 0);

            service.send(msg, options);

            verify(template).syncSend(same(msg), same(options));
        }

        @Test
        @DisplayName("oneway / batch / 事务均透传模板")
        void passthroughs() {
            Message<String> msg = MessageBuilder.<String>withTopic("t").body("b").build();
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("t").add(msg).build();
            List<SendResult> results = List.of(ok());
            when(template.syncSendBatch(any(), any())).thenReturn(results);

            service.sendOneway(msg);
            assertThat(service.sendBatch(batch)).isEqualTo(results);

            @SuppressWarnings("unchecked")
            io.github.streammq.core.transaction.TransactionCallback<String> callback =
                    mock(io.github.streammq.core.transaction.TransactionCallback.class);
            service.executeInTransaction(msg, callback);
            verify(template).executeInTransaction(same(msg), same(callback));
        }
    }
}
