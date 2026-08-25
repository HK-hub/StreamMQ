/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import org.junit.jupiter.api.Test;

/**
 * StreamMQMockUtils 单元测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
class StreamMQMockUtilsTest {

    @Test
    void createMockMessage_allParams() {
        Message<String> message =
                StreamMQMockUtils.createMockMessage("test-topic", "tag", "keys", "body");

        assertThat(message.getTopic()).isEqualTo("test-topic");
        assertThat(message.getTag()).isEqualTo("tag");
        assertThat(message.getKeys()).isEqualTo("keys");
        assertThat(message.getBody()).isEqualTo("body");
    }

    @Test
    void createMockMessage_minimalParams() {
        Message<String> message = StreamMQMockUtils.createMockMessage("test-topic", "body");

        assertThat(message.getTopic()).isEqualTo("test-topic");
        assertThat(message.getBody()).isEqualTo("body");
        assertThat(message.getTag()).isNull();
        assertThat(message.getKeys()).isNull();
    }

    @Test
    void createMockStringMessage() {
        Message<String> message = StreamMQMockUtils.createMockStringMessage("topic", "content");

        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getBody()).isEqualTo("content");
    }

    @Test
    void createSuccessResult_withTag() {
        SendResult result = StreamMQMockUtils.createSuccessResult("topic", "tag");

        assertThat(result.getMessageId()).isNotNull();
        assertThat(result.getTopic()).isEqualTo("topic");
        assertThat(result.getTag()).isEqualTo("tag");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
    }

    @Test
    void createSuccessResult_withoutTag() {
        SendResult result = StreamMQMockUtils.createSuccessResult("topic");

        assertThat(result.getMessageId()).isNotNull();
        assertThat(result.getTopic()).isEqualTo("topic");
        assertThat(result.getTag()).isNull();
        assertThat(result.isSuccess()).isTrue();
    }

    @Test
    void createFailedResult_withTag() {
        SendResult result = StreamMQMockUtils.createFailedResult("topic", "tag", "error message");

        assertThat(result.getMessageId()).isNotNull();
        assertThat(result.getTopic()).isEqualTo("topic");
        assertThat(result.getTag()).isEqualTo("tag");
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
        assertThat(result.getErrorMessage()).isEqualTo("error message");
    }

    @Test
    void createFailedResult_withoutTag() {
        SendResult result = StreamMQMockUtils.createFailedResult("topic", "error");

        assertThat(result.getMessageId()).isNotNull();
        assertThat(result.getTopic()).isEqualTo("topic");
        assertThat(result.getTag()).isNull();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).isEqualTo("error");
    }

    @Test
    void createMockMessageId_default() {
        MessageId messageId = StreamMQMockUtils.createMockMessageId();

        assertThat(messageId).isNotNull();
        assertThat(messageId.toString()).matches("\\d+-0");
    }

    @Test
    void createMockMessageId_withTimestamp() {
        long timestamp = 1234567890L;
        MessageId messageId = StreamMQMockUtils.createMockMessageId(timestamp);

        assertThat(messageId).isNotNull();
        assertThat(messageId.toString()).isEqualTo("1234567890-0");
    }

    @Test
    void createMockMessageId_unique() {
        MessageId id1 = StreamMQMockUtils.createMockMessageId(1234567890L);
        MessageId id2 = StreamMQMockUtils.createMockMessageId(9876543210L);

        assertThat(id1).isNotEqualTo(id2);
    }
}
