package io.github.streammq.adapter.redisson.container;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.DlqFailureHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * {@link LogAndDropDlqFailureHandler} 单元测试。
 */
@DisplayName("LogAndDropDlqFailureHandler 测试")
class LogAndDropDlqFailureHandlerTest {

    private final DlqFailureHandler handler = new LogAndDropDlqFailureHandler();

    @Test
    @DisplayName("cause 非 null 时不抛异常")
    void handleFailure_withCause_noThrow() {
        Message<?> msg = MessageBuilder.withTopic("t").body("b").build();
        ListenerRegistration<?> reg = ListenerRegistration.builder()
            .topic("t").group("g").dlqMode(true).build();
        assertThatNoException().isThrownBy(() ->
            handler.handleFailure(msg, reg, new RuntimeException("boom")));
    }

    @Test
    @DisplayName("cause 为 null 时不抛异常")
    void handleFailure_nullCause_noThrow() {
        Message<?> msg = MessageBuilder.withTopic("t").body("b").build();
        ListenerRegistration<?> reg = ListenerRegistration.builder()
            .topic("t").group("g").dlqMode(true).build();
        assertThatNoException().isThrownBy(() ->
            handler.handleFailure(msg, reg, null));
    }

    @Test
    @DisplayName("name 返回 log-and-drop")
    void name() {
        assertThat(handler.name()).isEqualTo("log-and-drop");
    }
}
