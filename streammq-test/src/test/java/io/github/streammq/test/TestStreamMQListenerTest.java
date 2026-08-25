/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TestStreamMQListener 单元测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
class TestStreamMQListenerTest {

    private TestStreamMQListener<String> listener;
    private ConsumeContext mockContext;

    @BeforeEach
    void setUp() {
        listener = new TestStreamMQListener<>();
        mockContext = mock(ConsumeContext.class);
    }

    @Test
    void onMessage_receiveMessage() throws Exception {
        Message<String> message = createTestMessage("key1", "body1");

        ConsumeAction result = listener.onMessage(message, mockContext);

        assertThat(result).isEqualTo(ConsumeAction.SUCCESS);
        assertThat(listener.getReceivedCount()).isEqualTo(1);
        assertThat(listener.getSuccessCount()).isEqualTo(1);
        assertThat(listener.getFailCount()).isEqualTo(0);
        assertThat(listener.getReceivedMessages()).hasSize(1);
        assertThat(listener.getReceivedMessages().get(0).getBody()).isEqualTo("body1");
    }

    @Test
    void onMessage_multipleMessages() throws Exception {
        listener.onMessage(createTestMessage("key1", "body1"), mockContext);
        listener.onMessage(createTestMessage("key2", "body2"), mockContext);
        listener.onMessage(createTestMessage("key3", "body3"), mockContext);

        assertThat(listener.getReceivedCount()).isEqualTo(3);
        assertThat(listener.getSuccessCount()).isEqualTo(3);
        assertThat(listener.getReceivedMessages()).hasSize(3);
    }

    @Test
    void onMessage_customAction() throws Exception {
        listener.setNextAction(ConsumeAction.RECONSUME_LATER);
        Message<String> message = createTestMessage("key1", "body1");

        ConsumeAction result = listener.onMessage(message, mockContext);

        assertThat(result).isEqualTo(ConsumeAction.RECONSUME_LATER);
        assertThat(listener.getSuccessCount()).isEqualTo(1);
    }

    @Test
    void onMessage_failAfterCount() throws Exception {
        listener.setShouldFail(true);
        listener.setFailAfterCount(2);

        listener.onMessage(createTestMessage("key1", "body1"), mockContext);
        listener.onMessage(createTestMessage("key2", "body2"), mockContext);

        assertThatThrownBy(
                        () -> listener.onMessage(createTestMessage("key3", "body3"), mockContext))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Intentional test failure");

        assertThat(listener.getSuccessCount()).isEqualTo(2);
        assertThat(listener.getFailCount()).isEqualTo(1);
        assertThat(listener.getExceptions()).hasSize(1);
    }

    @Test
    void onMessage_failImmediately() throws Exception {
        listener.setShouldFail(true);
        listener.setFailAfterCount(0);

        assertThatThrownBy(
                        () -> listener.onMessage(createTestMessage("key1", "body1"), mockContext))
                .isInstanceOf(RuntimeException.class);

        assertThat(listener.getFailCount()).isEqualTo(1);
        assertThat(listener.getSuccessCount()).isEqualTo(0);
    }

    @Test
    void reset_clearAll() throws Exception {
        listener.onMessage(createTestMessage("key1", "body1"), mockContext);
        listener.onMessage(createTestMessage("key2", "body2"), mockContext);
        listener.setShouldFail(true);
        listener.setFailAfterCount(0);

        try {
            listener.onMessage(createTestMessage("key3", "body3"), mockContext);
        } catch (Exception ignored) {
        }

        listener.reset();

        assertThat(listener.getReceivedCount()).isEqualTo(0);
        assertThat(listener.getSuccessCount()).isEqualTo(0);
        assertThat(listener.getFailCount()).isEqualTo(0);
        assertThat(listener.getExceptions()).isEmpty();
        assertThat(listener.getReceivedMessages()).isEmpty();
    }

    @Test
    void reset_clearAction() throws Exception {
        listener.setNextAction(ConsumeAction.RECONSUME_LATER);
        listener.setShouldFail(true);
        listener.setFailAfterCount(5);

        listener.reset();

        Message<String> message = createTestMessage("key1", "body1");
        ConsumeAction result = listener.onMessage(message, mockContext);

        assertThat(result).isEqualTo(ConsumeAction.SUCCESS);
        assertThat(listener.getSuccessCount()).isEqualTo(1);
    }

    @Test
    void awaitMessages_success() throws Exception {
        new Thread(
                        () -> {
                            try {
                                Thread.sleep(100);
                                listener.onMessage(createTestMessage("key1", "body1"), mockContext);
                            } catch (Exception e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .start();

        listener.awaitMessages(1, 5000);

        assertThat(listener.getReceivedCount()).isEqualTo(1);
    }

    @Test
    void awaitMessages_timeout() throws Exception {
        assertThatThrownBy(() -> listener.awaitMessages(1, 100))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Timeout waiting for 1 messages");
    }

    @Test
    void awaitMessages_multiple() throws Exception {
        new Thread(
                        () -> {
                            try {
                                Thread.sleep(100);
                                listener.onMessage(createTestMessage("key1", "body1"), mockContext);
                                Thread.sleep(50);
                                listener.onMessage(createTestMessage("key2", "body2"), mockContext);
                            } catch (Exception e) {
                                Thread.currentThread().interrupt();
                            }
                        })
                .start();

        listener.awaitMessages(2, 5000);

        assertThat(listener.getReceivedCount()).isEqualTo(2);
    }

    @Test
    void getReceivedMessages_returnsCopy() throws Exception {
        listener.onMessage(createTestMessage("key1", "body1"), mockContext);
        List<Message<String>> messages = listener.getReceivedMessages();

        listener.onMessage(createTestMessage("key2", "body2"), mockContext);

        assertThat(messages).hasSize(1);
        assertThat(listener.getReceivedMessages()).hasSize(2);
    }

    @Test
    void getExceptions_returnsCopy() throws Exception {
        listener.setShouldFail(true);
        listener.setFailAfterCount(0);

        try {
            listener.onMessage(createTestMessage("key1", "body1"), mockContext);
        } catch (Exception ignored) {
        }
        List<Exception> exceptions = listener.getExceptions();

        try {
            listener.onMessage(createTestMessage("key2", "body2"), mockContext);
        } catch (Exception ignored) {
        }

        assertThat(exceptions).hasSize(1);
        assertThat(listener.getExceptions()).hasSize(2);
    }

    private Message<String> createTestMessage(String keys, String body) {
        return MessageBuilder.<String>withTopic("test-topic").keys(keys).body(body).build();
    }
}
