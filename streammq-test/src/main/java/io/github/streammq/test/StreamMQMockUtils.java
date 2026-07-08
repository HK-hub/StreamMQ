package io.github.streammq.test;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;

/**
 * StreamMQ Mock 工具类，提供测试所需的模拟对象。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class StreamMQMockUtils {

    private StreamMQMockUtils() {
    }

    public static <T> Message<T> createMockMessage(String topic, String tag, String keys, T body) {
        return MessageBuilder.<T>withTopic(topic)
                .tag(tag)
                .keys(keys)
                .body(body)
                .build();
    }

    public static <T> Message<T> createMockMessage(String topic, T body) {
        return createMockMessage(topic, null, null, body);
    }

    public static Message<String> createMockStringMessage(String topic, String body) {
        return createMockMessage(topic, body);
    }

    public static SendResult createSuccessResult(String topic, String tag) {
        return new SendResult(new MessageId("1234567890-0"), topic, tag, System.currentTimeMillis());
    }

    public static SendResult createSuccessResult(String topic) {
        return createSuccessResult(topic, null);
    }

    public static SendResult createFailedResult(String topic, String tag, String errorMessage) {
        return new SendResult(new MessageId("1234567890-0"), topic, tag,
                SendStatus.SEND_FAILED, System.currentTimeMillis(), null, errorMessage);
    }

    public static SendResult createFailedResult(String topic, String errorMessage) {
        return createFailedResult(topic, null, errorMessage);
    }

    public static MessageId createMockMessageId() {
        return new MessageId(System.currentTimeMillis() + "-0");
    }

    public static MessageId createMockMessageId(long timestamp) {
        return new MessageId(timestamp + "-0");
    }
}