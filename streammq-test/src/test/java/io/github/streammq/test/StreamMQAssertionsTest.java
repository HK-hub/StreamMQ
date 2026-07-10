package io.github.streammq.test;

import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * StreamMQAssertions 单元测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
class StreamMQAssertionsTest {

    @Test
    void assertThatSendResult_isSuccess() {
        SendResult result = new SendResult(new MessageId("123-0"), "test-topic", "tag", System.currentTimeMillis());
        StreamMQAssertions.assertThat(result).isSuccess();
    }

    @Test
    void assertThatSendResult_isFailed() {
        SendResult result = new SendResult(new MessageId("123-0"), "test-topic", "tag",
                SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "error");
        StreamMQAssertions.assertThat(result).isFailed();
    }

    @Test
    void assertThatSendResult_hasTopic() {
        SendResult result = new SendResult(new MessageId("123-0"), "order-topic", "tag", System.currentTimeMillis());
        StreamMQAssertions.assertThat(result).hasTopic("order-topic");
    }

    @Test
    void assertThatSendResult_hasTag() {
        SendResult result = new SendResult(new MessageId("123-0"), "topic", "created", System.currentTimeMillis());
        StreamMQAssertions.assertThat(result).hasTag("created");
    }

    @Test
    void assertThatSendResult_hasSendStatus() {
        SendResult result = new SendResult(new MessageId("123-0"), "topic", "tag", System.currentTimeMillis());
        StreamMQAssertions.assertThat(result).hasSendStatus(SendStatus.SEND_OK);
    }

    @Test
    void assertThatSendResult_hasErrorMessage() {
        SendResult result = new SendResult(new MessageId("123-0"), "topic", "tag",
                SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "timeout");
        StreamMQAssertions.assertThat(result).hasErrorMessage("timeout");
    }

    @Test
    void assertThatSendResult_hasMessageId() {
        SendResult result = new SendResult(new MessageId("123-0"), "topic", "tag", System.currentTimeMillis());
        StreamMQAssertions.assertThat(result).hasMessageId();
    }

    @Test
    void assertThatSendResult_isSuccess_failWhenNotSuccess() {
        SendResult result = new SendResult(new MessageId("123-0"), "topic", "tag",
                SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "error");
        assertThatThrownBy(() -> StreamMQAssertions.assertThat(result).isSuccess())
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void assertThatSendResult_isFailed_failWhenNotFailed() {
        SendResult result = new SendResult(new MessageId("123-0"), "topic", "tag", System.currentTimeMillis());
        assertThatThrownBy(() -> StreamMQAssertions.assertThat(result).isFailed())
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void assertThatMessage_hasTopic() {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic").body("test").build();
        StreamMQAssertions.assertThat(message).hasTopic("order-topic");
    }

    @Test
    void assertThatMessage_hasTag() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").tag("created").body("test").build();
        StreamMQAssertions.assertThat(message).hasTag("created");
    }

    @Test
    void assertThatMessage_hasKeys() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").keys("order-123").body("test").build();
        StreamMQAssertions.assertThat(message).hasKeys("order-123");
    }

    @Test
    void assertThatMessage_hasShardingKey() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").shardingKey("shard-1").body("test").build();
        StreamMQAssertions.assertThat(message).hasShardingKey("shard-1");
    }

    @Test
    void assertThatMessage_hasBody() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").body("hello").build();
        StreamMQAssertions.assertThat(message).hasBody("hello");
    }

    @Test
    void assertThatMessage_hasMessageId() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").body("test").build();
        message.setMessageId(new MessageId("123-0"));
        StreamMQAssertions.assertThat(message).hasMessageId();
    }

    @Test
    void assertThatMessage_hasUserProperty() {
        Message<String> message = MessageBuilder.<String>withTopic("topic")
                .userProperty("source", "sample")
                .body("test").build();
        StreamMQAssertions.assertThat(message).hasUserProperty("source", "sample");
    }

    @Test
    void assertThatMessage_isDelayMessage() {
        Message<String> message = MessageBuilder.<String>withTopic("topic")
                .delayLevel(DelayLevel.SECOND_5)
                .body("test").build();
        StreamMQAssertions.assertThat(message).isDelayMessage();
    }

    @Test
    void assertThatMessage_isDelayMessage_customDelay() {
        Message<String> message = MessageBuilder.<String>withTopic("topic")
                .delayTimeMillis(1000)
                .body("test").build();
        StreamMQAssertions.assertThat(message).isDelayMessage();
    }

    @Test
    void assertThatMessage_isTransactionMessage() {
        Message<String> message = MessageBuilder.<String>withTopic("topic")
                .body("test").build();
        message.setTransactionId("tx-123");
        StreamMQAssertions.assertThat(message).isTransactionMessage();
    }

    @Test
    void assertThatMessage_hasReconsumeTimes() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").body("test").build();
        StreamMQAssertions.assertThat(message).hasReconsumeTimes(0);
    }

    @Test
    void assertThatMessage_isDelayMessage_failWhenNotDelay() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").body("test").build();
        assertThatThrownBy(() -> StreamMQAssertions.assertThat(message).isDelayMessage())
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void assertThatMessage_isTransactionMessage_failWhenNotTransaction() {
        Message<String> message = MessageBuilder.<String>withTopic("topic").body("test").build();
        assertThatThrownBy(() -> StreamMQAssertions.assertThat(message).isTransactionMessage())
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void assertSendResultsSuccess_allSuccess() {
        List<SendResult> results = List.of(
                new SendResult(new MessageId("1-0"), "topic", "tag", System.currentTimeMillis()),
                new SendResult(new MessageId("2-0"), "topic", "tag", System.currentTimeMillis())
        );
        StreamMQAssertions.assertSendResultsSuccess(results);
    }

    @Test
    void assertSendResultsSuccess_failWhenContainsFailure() {
        List<SendResult> results = List.of(
                new SendResult(new MessageId("1-0"), "topic", "tag", System.currentTimeMillis()),
                new SendResult(new MessageId("2-0"), "topic", "tag",
                        SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "error")
        );
        assertThatThrownBy(() -> StreamMQAssertions.assertSendResultsSuccess(results))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void assertSendResultsFailed_allFailed() {
        List<SendResult> results = List.of(
                new SendResult(new MessageId("1-0"), "topic", "tag",
                        SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "error"),
                new SendResult(new MessageId("2-0"), "topic", "tag",
                        SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "error")
        );
        StreamMQAssertions.assertSendResultsFailed(results);
    }

    @Test
    void assertSendResultsFailed_failWhenContainsSuccess() {
        List<SendResult> results = List.of(
                new SendResult(new MessageId("1-0"), "topic", "tag",
                        SendStatus.SEND_FAILED, System.currentTimeMillis(), null, "error"),
                new SendResult(new MessageId("2-0"), "topic", "tag", System.currentTimeMillis())
        );
        assertThatThrownBy(() -> StreamMQAssertions.assertSendResultsFailed(results))
                .isInstanceOf(AssertionError.class);
    }
}