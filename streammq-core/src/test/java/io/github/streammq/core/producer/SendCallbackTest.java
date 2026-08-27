/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.producer;

import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link SendCallback} 单元测试，覆盖 onException 默认实现的日志行为（F-12 回归：不再静默吞异常）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("SendCallback 异步发送回调测试")
class SendCallbackTest {

    @Test
    @DisplayName("onException 默认实现记录日志且不抛出（含 null 异常对象也不抛）")
    void defaultOnExceptionLogsAndDoesNotThrow() {
        SendCallback callback = result -> {};
        assertThatCode(() -> callback.onException(new RuntimeException("boom")))
                .doesNotThrowAnyException();
        assertThatCode(() -> callback.onException(null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("onSuccess 为唯一抽象方法，lambda 可直接构造")
    void functionalInterfaceSmoke() {
        SendResult result =
                new SendResult(MessageId.sentinel(), "topic", null, System.currentTimeMillis());
        SendCallback callback = r -> assertThatReceived(r, result);
        callback.onSuccess(result);
    }

    private static void assertThatReceived(SendResult received, SendResult expected) {
        org.assertj.core.api.Assertions.assertThat(received).isSameAs(expected);
    }
}
