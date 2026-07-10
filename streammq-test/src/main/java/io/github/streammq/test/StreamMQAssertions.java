package io.github.streammq.test;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.List;

/**
 * StreamMQ 断言工具类，提供针对消息相关对象的断言方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class StreamMQAssertions {

    private StreamMQAssertions() {
    }

    public static SendResultAssert assertThat(SendResult actual) {
        return new SendResultAssert(actual);
    }

    public static <T> MessageAssert<T> assertThat(Message<T> actual) {
        return new MessageAssert<>(actual);
    }

    public static class SendResultAssert extends AbstractAssert<SendResultAssert, SendResult> {

        protected SendResultAssert(SendResult actual) {
            super(actual, SendResultAssert.class);
        }

        public SendResultAssert isSuccess() {
            isNotNull();
            Assertions.assertThat(actual.isSuccess())
                    .as("Expected send result to be SUCCESS, but was %s", actual.getSendStatus())
                    .isTrue();
            return this;
        }

        public SendResultAssert isFailed() {
            isNotNull();
            Assertions.assertThat(actual.isSuccess())
                    .as("Expected send result to be FAILED, but was %s", actual.getSendStatus())
                    .isFalse();
            return this;
        }

        public SendResultAssert hasTopic(String expectedTopic) {
            isNotNull();
            Assertions.assertThat(actual.getTopic())
                    .as("Expected topic to be %s, but was %s", expectedTopic, actual.getTopic())
                    .isEqualTo(expectedTopic);
            return this;
        }

        public SendResultAssert hasTag(String expectedTag) {
            isNotNull();
            Assertions.assertThat(actual.getTag())
                    .as("Expected tag to be %s, but was %s", expectedTag, actual.getTag())
                    .isEqualTo(expectedTag);
            return this;
        }

        public SendResultAssert hasSendStatus(SendStatus expectedStatus) {
            isNotNull();
            Assertions.assertThat(actual.getSendStatus())
                    .as("Expected send status to be %s, but was %s", expectedStatus, actual.getSendStatus())
                    .isEqualTo(expectedStatus);
            return this;
        }

        public SendResultAssert hasErrorMessage(String expectedErrorMessage) {
            isNotNull();
            Assertions.assertThat(actual.getErrorMessage())
                    .as("Expected error message to be %s, but was %s", expectedErrorMessage, actual.getErrorMessage())
                    .isEqualTo(expectedErrorMessage);
            return this;
        }

        public SendResultAssert hasMessageId() {
            isNotNull();
            Assertions.assertThat(actual.getMessageId())
                    .as("Expected message ID to be present")
                    .isNotNull();
            return this;
        }
    }

    public static class MessageAssert<T> extends AbstractAssert<MessageAssert<T>, Message<T>> {

        protected MessageAssert(Message<T> actual) {
            super(actual, MessageAssert.class);
        }

        public MessageAssert<T> hasTopic(String expectedTopic) {
            isNotNull();
            Assertions.assertThat(actual.getTopic())
                    .as("Expected topic to be %s, but was %s", expectedTopic, actual.getTopic())
                    .isEqualTo(expectedTopic);
            return this;
        }

        public MessageAssert<T> hasTag(String expectedTag) {
            isNotNull();
            Assertions.assertThat(actual.getTag())
                    .as("Expected tag to be %s, but was %s", expectedTag, actual.getTag())
                    .isEqualTo(expectedTag);
            return this;
        }

        public MessageAssert<T> hasKeys(String expectedKeys) {
            isNotNull();
            Assertions.assertThat(actual.getKeys())
                    .as("Expected keys to be %s, but was %s", expectedKeys, actual.getKeys())
                    .isEqualTo(expectedKeys);
            return this;
        }

        public MessageAssert<T> hasShardingKey(String expectedShardingKey) {
            isNotNull();
            Assertions.assertThat(actual.getShardingKey())
                    .as("Expected shardingKey to be %s, but was %s", expectedShardingKey, actual.getShardingKey())
                    .isEqualTo(expectedShardingKey);
            return this;
        }

        public MessageAssert<T> hasBody(T expectedBody) {
            isNotNull();
            Assertions.assertThat(actual.getBody())
                    .as("Expected body to be %s, but was %s", expectedBody, actual.getBody())
                    .isEqualTo(expectedBody);
            return this;
        }

        public MessageAssert<T> hasMessageId() {
            isNotNull();
            Assertions.assertThat(actual.getMessageId())
                    .as("Expected message ID to be present")
                    .isNotNull();
            return this;
        }

        public MessageAssert<T> hasUserProperty(String key, String expectedValue) {
            isNotNull();
            Assertions.assertThat(actual.getUserProperties().get(key))
                    .as("Expected user property %s to be %s", key, expectedValue)
                    .isEqualTo(expectedValue);
            return this;
        }

        public MessageAssert<T> isDelayMessage() {
            isNotNull();
            Assertions.assertThat(actual.isDelayMessage())
                    .as("Expected message to be a delay message")
                    .isTrue();
            return this;
        }

        public MessageAssert<T> isTransactionMessage() {
            isNotNull();
            Assertions.assertThat(actual.isTransactionMessage())
                    .as("Expected message to be a transaction message")
                    .isTrue();
            return this;
        }

        public MessageAssert<T> hasReconsumeTimes(int expectedTimes) {
            isNotNull();
            Assertions.assertThat(actual.getReconsumeTimes())
                    .as("Expected reconsume times to be %d, but was %d", expectedTimes, actual.getReconsumeTimes())
                    .isEqualTo(expectedTimes);
            return this;
        }
    }

    public static void assertSendResultsSuccess(List<SendResult> results) {
        Assertions.assertThat(results)
                .as("Expected all send results to be SUCCESS")
                .allSatisfy(result -> assertThat(result).isSuccess());
    }

    public static void assertSendResultsFailed(List<SendResult> results) {
        Assertions.assertThat(results)
                .as("Expected all send results to be FAILED")
                .allSatisfy(result -> assertThat(result).isFailed());
    }
}