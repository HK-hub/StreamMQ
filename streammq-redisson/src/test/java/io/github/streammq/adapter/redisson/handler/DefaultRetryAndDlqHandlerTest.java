/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureDecision;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.slf4j.LoggerFactory;

/**
 * {@link DefaultRetryAndDlqHandler} DLQ 失败策略契约单元测试。
 *
 * <p>回归 B-07：{@code decide()} 返回 null 时框架必须按 {@link DlqFailureDecision#drop()} 兜底 （见 {@link
 * DlqFailureStrategy#decide} 的接口契约）。旧实现先求值 {@code decision.type()} 再判空， 策略返回 null 必然 NPE——null
 * 兜底是死代码，NPE 被外层 catch 吞成 ERROR 后消息滞留 PEL， 表现为"DLQ 消息既不被丢弃也不被重试，且无一条决策日志"。
 */
@DisplayName("DefaultRetryAndDlqHandler 空决策兜底")
class DefaultRetryAndDlqHandlerTest {

    private static final MessageId MESSAGE_ID = MessageId.fromStreamEntry("1700000000000-0");

    private MessageConverter messageConverter;
    private ListenerRegistration<?> reg;
    private StreamMQListener listener;
    private DlqFailureStrategy strategy;
    private DefaultRetryAndDlqHandler handler;
    private Logger handlerLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        messageConverter = mock(MessageConverter.class);
        reg = mock(ListenerRegistration.class);
        listener = mock(StreamMQListener.class);
        strategy = mock(DlqFailureStrategy.class);

        when(reg.isDlqMode()).thenReturn(true);
        when(reg.getTopic()).thenReturn("dlq-topic");
        when(reg.getGroup()).thenReturn("dlq-group");
        when(messageConverter.toStreamFields(any())).thenReturn(Map.of("body", "cGF5bG9hZA=="));
        when(strategy.name()).thenReturn("TestStrategy");

        handler =
                new DefaultRetryAndDlqHandler(
                        mock(RedissonClient.class),
                        messageConverter,
                        mock(RetryPolicy.class),
                        mock(ConsumerInterceptorChain.class),
                        strategy,
                        DlqConfig.builder().build());

        handlerLogger = (Logger) LoggerFactory.getLogger(DefaultRetryAndDlqHandler.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        handlerLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        if (handlerLogger != null && logAppender != null) {
            handlerLogger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    @Test
    @DisplayName("策略返回 null：不抛 NPE，按 DROP 兜底（ACK 且 WARN 留痕）")
    void nullDecision_fallsBackToDropInsteadOfNpe() {
        when(strategy.decide(any(), any())).thenReturn(null);

        assertThatCode(
                        () ->
                                handler.handleAction(
                                        ConsumeAction.RECONSUME_LATER,
                                        message(),
                                        reg,
                                        listener,
                                        new RuntimeException("dlq handler failed")))
                .as("策略返回 null 不得抛出 NPE（旧实现即在此处炸掉整个兜底分支）")
                .doesNotThrowAnyException();

        // null → drop 的既有兜底语义：ACK 该条死信，不重试、不转投二级死信
        verify(listener, times(1)).ack(MESSAGE_ID);
        verify(strategy, times(1)).decide(any(), any());
        assertThat(eventsOf(Level.WARN))
                .as("null 决策必须留下 WARN（可观测），而不是静默 NPE")
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("returned null decision")
                                        .contains("falling back to DROP"));
        assertThat(eventsOf(Level.ERROR))
                .as("兜底路径不得再走异常 catch 分支（旧实现会在这里留下策略异常 ERROR）")
                .noneSatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("DLQ failure strategy error"));
    }

    @Test
    @DisplayName("策略正常返回 DROP：与 null 兜底行为一致（回归对照）")
    void explicitDrop_behavesLikeNullFallback() {
        when(strategy.decide(any(), any())).thenReturn(DlqFailureDecision.drop());

        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(),
                reg,
                listener,
                new RuntimeException("dlq handler failed"));

        verify(listener, times(1)).ack(MESSAGE_ID);
        verify(listener, never()).close();
        assertThat(eventsOf(Level.WARN))
                .as("显式 DROP 不应出现 null 兜底告警")
                .noneSatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("returned null decision"));
    }

    private static Message<String> message() {
        return MessageBuilder.<String>withTopic("dlq-topic")
                .body("payload")
                .messageId(MESSAGE_ID)
                .build();
    }

    private List<ILoggingEvent> eventsOf(Level level) {
        return logAppender.list.stream().filter(event -> level.equals(event.getLevel())).toList();
    }
}
