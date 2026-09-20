/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.adapter.redisson.dlq.DefaultDlqFailureContext;
import io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureContext;
import io.github.streammq.core.policy.DlqFailureDecision;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RStream;
import org.redisson.api.RStreamAsync;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.slf4j.LoggerFactory;

/**
 * 重试调度及 DLQ 分派契约测试（第六轮红队 R3-3 / R3-4 / R3-5 / R3-6）。
 *
 * <ul>
 *   <li>R3-3：重试延迟超 7 天时夹取到上界 + payload TTL = max(7d, 延迟+1h 宽限)，杜绝"payload 先过期 → 进隔离区"
 *   <li>R3-4：接线 {@link RetryPolicy#shouldStopRetry}；delay-array 长度不再充当预算上限
 *   <li>R3-5：handler 将生效 DlqConfig 随 {@link DefaultDlqFailureContext} 交给策略
 *   <li>R3-6：secondary-dlq-enabled=false 时二级路由不生效（按 drop 处理）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("重试调度与 DLQ 分派契约（R3-3/4/5/6）")
class RetrySchedulingAndDlqGatingTest {

    private static final MessageId MESSAGE_ID = MessageId.fromStreamEntry("1700000000000-0");
    private static final String TOPIC = "retry-topic";
    private static final String GROUP = "retry-group";
    private static final String NAMESPACE = "ns";

    private RedissonClient redisson;
    private MessageConverter messageConverter;
    private ListenerRegistration<?> reg;
    private StreamMQListener listener;
    private RBatch batch;
    private RMapAsync<String, String> payloadMap;
    private RScoredSortedSetAsync<String> retryZset;
    private RStream<String, String> dlqStream;
    private RStream<String, String> dlq2Stream;

    private Logger handlerLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    void setUp() {
        redisson = mock(RedissonClient.class);
        messageConverter = mock(MessageConverter.class);
        reg = mock(ListenerRegistration.class);
        listener = mock(StreamMQListener.class);
        batch = mock(RBatch.class);
        payloadMap = mockMapAsync();
        retryZset = mockZsetAsync();
        dlqStream = mockStream();
        dlq2Stream = mockStream();

        when(reg.getTopic()).thenReturn(TOPIC);
        when(reg.getGroup()).thenReturn(GROUP);
        when(reg.getNamespace()).thenReturn(NAMESPACE);
        when(messageConverter.toStreamFields(any()))
                .thenAnswer(invocation -> new HashMap<>(Map.of("body", "cGF5bG9hZA==")));

        doReturn(batch).when(redisson).createBatch(any(BatchOptions.class));
        doReturn(payloadMap).when(batch).getMap(anyString(), any(StringCodec.class));
        doReturn(retryZset).when(batch).getScoredSortedSet(anyString(), any(StringCodec.class));
        doReturn(mockAsyncStream()).when(batch).getStream(anyString(), any(StringCodec.class));
        doReturn(dlqStream).when(redisson).getStream(anyString(), any(StringCodec.class));

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

    // ===================== R3-3：延迟上界与 payload TTL =====================

    @Test
    @DisplayName("重试延迟 8 天：夹取到 7 天 + WARN，payload TTL >= 延迟+1h 宽限（失败即红）")
    void retryDelayBeyondSevenDays_isClampedAndTtlCoversGrace() {
        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getMaxReconsumeTimes()).thenReturn(16);
        RetryPolicy policy = mock(RetryPolicy.class);
        when(policy.name()).thenReturn("test-policy");
        when(policy.shouldStopRetry(anyInt(), any())).thenReturn(false);
        when(policy.nextRetryDelay(anyInt(), any())).thenReturn(Duration.ofDays(8));
        DefaultRetryAndDlqHandler handler = newHandler(policy, DlqConfig.builder().build());

        long before = System.currentTimeMillis();
        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(0),
                reg,
                listener,
                new IllegalStateException("business failed"));

        ArgumentCaptor<Double> score = ArgumentCaptor.forClass(Double.class);
        verify(retryZset).addAsync(score.capture(), eq(MESSAGE_ID.getStreamEntryId()));
        double maxDelay = StreamMQConstants.MAX_DELAY_TIME_MILLIS;
        assertThat(score.getValue())
                .as("8 天延迟必须被夹取到 7 天上界")
                .isBetween(before + maxDelay, System.currentTimeMillis() + maxDelay + 1_000);

        ArgumentCaptor<Duration> ttl = ArgumentCaptor.forClass(Duration.class);
        verify(payloadMap).expireAsync(ttl.capture());
        assertThat(ttl.getValue())
                .as("TTL 必须覆盖 延迟+宽限（7d + 1h）")
                .isGreaterThanOrEqualTo(
                        Duration.ofMillis(
                                StreamMQConstants.MAX_DELAY_TIME_MILLIS
                                        + StreamMQConstants.DEFAULT_DELAY_PAYLOAD_TTL_GRACE_MS));

        verify(listener).ack(MESSAGE_ID);
        assertThat(warnLogs())
                .as("夹取必须留下 WARN 便于定位策略配置问题")
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("MAX_DELAY_TIME_MILLIS"));
    }

    @Test
    @DisplayName("DEFER 8 天：延迟不夹取（业务节奏），TTL 仍覆盖 延迟+1h（失败即红）")
    void deferBeyondSevenDays_keepsDelayButTtlCoversIt() {
        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getMaxReconsumeTimes()).thenReturn(16);
        DefaultRetryAndDlqHandler handler =
                newHandler(mock(RetryPolicy.class), DlqConfig.builder().build());

        long before = System.currentTimeMillis();
        handler.handleDefer(message(0), reg, listener, MESSAGE_ID, Duration.ofDays(8));

        ArgumentCaptor<Double> score = ArgumentCaptor.forClass(Double.class);
        verify(retryZset).addAsync(score.capture(), eq(MESSAGE_ID.getStreamEntryId()));
        double deferMillis = Duration.ofDays(8).toMillis();
        assertThat(score.getValue())
                .as("DEFER 延迟语义不变（不夹取）")
                .isGreaterThanOrEqualTo(before + deferMillis)
                .isLessThanOrEqualTo(System.currentTimeMillis() + deferMillis + 1_000);

        ArgumentCaptor<Duration> ttl = ArgumentCaptor.forClass(Duration.class);
        verify(payloadMap).expireAsync(ttl.capture());
        assertThat(ttl.getValue())
                .as("延迟超过 7 天时 payload TTL 必须 = 延迟 + 宽限，否则到期扫描读不到 payload")
                .isGreaterThanOrEqualTo(Duration.ofDays(8).plusHours(1));
    }

    // ===================== R3-4：预算与 shouldStopRetry =====================

    @Test
    @DisplayName("delay-array 长度=2 + max-reconsume-times=16：重试到第 16 次仍调度，第 17 次才进 DLQ（失败即红）")
    void shortDelayArray_withBudget16_stillSchedulesRetry() {
        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getMaxReconsumeTimes()).thenReturn(16);
        FixedArrayRetryPolicy policy = new FixedArrayRetryPolicy(new long[] {1_000L, 2_000L});
        DefaultRetryAndDlqHandler handler = newHandler(policy, DlqConfig.builder().build());

        // reconsumeTimes 0..15（共 16 次重试）全部调度；旧实现第 2 次（数组长度）就会因
        // nextRetryDelay=null 直接进 DLQ
        for (int reconsumeTimes = 0; reconsumeTimes < 16; reconsumeTimes++) {
            handler.handleAction(
                    ConsumeAction.RECONSUME_LATER,
                    message(reconsumeTimes),
                    reg,
                    listener,
                    new IllegalStateException("business failed"));
        }
        verify(batch, times(16)).execute();
        verify(retryZset, times(16)).addAsync(anyDouble(), eq(MESSAGE_ID.getStreamEntryId()));
        verify(dlqStream, never()).add(anyStreamAddArgs());

        // 第 17 次（reconsumeTimes=16 >= max-reconsume-times）由框架预算收口进 DLQ
        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(16),
                reg,
                listener,
                new IllegalStateException("business failed"));
        verify(dlqStream, times(1)).add(anyStreamAddArgs());
        verify(listener, times(17)).ack(MESSAGE_ID);
    }

    @Test
    @DisplayName("shouldStopRetry=true：不再调用 nextRetryDelay，直接进 DLQ（reason=MAX_RETRY）")
    void shouldStopRetryTrue_routesToDlqWithoutCallingNextDelay() {
        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getMaxReconsumeTimes()).thenReturn(16);
        RetryPolicy policy = mock(RetryPolicy.class);
        when(policy.name()).thenReturn("stop-now");
        when(policy.shouldStopRetry(anyInt(), any())).thenReturn(true);
        DefaultRetryAndDlqHandler handler = newHandler(policy, DlqConfig.builder().build());

        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(0),
                reg,
                listener,
                new IllegalStateException("business failed"));

        verify(dlqStream, times(1)).add(anyStreamAddArgs());
        verify(listener).ack(MESSAGE_ID);
        verify(policy, never()).nextRetryDelay(anyInt(), any());
        verify(redisson, never()).createBatch(any(BatchOptions.class));
    }

    @Test
    @DisplayName("shouldStopRetry 抛异常：按不停止处理并 WARN，重试继续（防御坏策略）")
    void shouldStopRetryThrows_fallsBackToNextRetryDelay() {
        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getMaxReconsumeTimes()).thenReturn(16);
        RetryPolicy policy = mock(RetryPolicy.class);
        when(policy.name()).thenReturn("broken-policy");
        when(policy.shouldStopRetry(anyInt(), any()))
                .thenThrow(new IllegalStateException("broken shouldStopRetry"));
        when(policy.nextRetryDelay(anyInt(), any())).thenReturn(Duration.ofSeconds(1));
        DefaultRetryAndDlqHandler handler = newHandler(policy, DlqConfig.builder().build());

        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(0),
                reg,
                listener,
                new IllegalStateException("business failed"));

        verify(batch, times(1)).execute();
        verify(dlqStream, never()).add(anyStreamAddArgs());
        assertThat(warnLogs())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("shouldStopRetry threw"));
    }

    // ===================== R3-5 / R3-6：DLQ 配置真源与二级开关 =====================

    @Test
    @DisplayName("handler 把生效 DlqConfig 随上下文交给策略（全局 max-dlq-retry-attempts=5）")
    void handlerCarriesEffectiveDlqConfigIntoContext() {
        when(reg.isDlqMode()).thenReturn(true);
        DlqFailureStrategy strategy = mock(DlqFailureStrategy.class);
        when(strategy.name()).thenReturn("capture-strategy");
        when(strategy.decide(any(), any())).thenReturn(DlqFailureDecision.drop());
        DlqConfig dlqConfig = DlqConfig.builder().maxDlqRetryAttempts(5).build();
        DefaultRetryAndDlqHandler handler =
                newHandler(mock(RetryPolicy.class), strategy, dlqConfig);

        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(0),
                reg,
                listener,
                new IllegalStateException("dlq consumer failed"));

        ArgumentCaptor<DlqFailureContext> ctx = ArgumentCaptor.forClass(DlqFailureContext.class);
        verify(strategy).decide(any(), ctx.capture());
        assertThat(ctx.getValue()).isInstanceOf(DefaultDlqFailureContext.class);
        DefaultDlqFailureContext captured = (DefaultDlqFailureContext) ctx.getValue();
        DlqConfig effective = captured.effectiveDlqConfig();
        assertThat(effective).as("策略必须能从上下文读到全局配置（不再依赖反射实例的 builder 默认）").isNotNull();
        assertThat(effective.getMaxDlqRetryAttempts()).isEqualTo(5);
        assertThat(captured.maxDlqRetryAttempts()).isEqualTo(5);
    }

    @Test
    @DisplayName("secondary-dlq-enabled=false：策略返回 SECONDARY_DLQ 也按 drop 处理，不写 dlq2（失败即红）")
    void secondaryDisabled_dropsInsteadOfWritingDlq2() {
        when(reg.isDlqMode()).thenReturn(true);
        DlqFailureStrategy strategy = mock(DlqFailureStrategy.class);
        when(strategy.name()).thenReturn("secondary-strategy");
        when(strategy.decide(any(), any())).thenReturn(DlqFailureDecision.secondaryDlq());
        DlqConfig dlqConfig = DlqConfig.builder().secondaryDlqEnabled(false).build();
        DefaultRetryAndDlqHandler handler =
                newHandler(mock(RetryPolicy.class), strategy, dlqConfig);

        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(0),
                reg,
                listener,
                new IllegalStateException("dlq consumer failed"));

        String dlq2Key = StreamMQKeys.secondaryDlqStream(NAMESPACE, GROUP, "dlq2");
        verify(redisson, never()).getStream(eq(dlq2Key), any(StringCodec.class));
        verify(dlq2Stream, never()).add(anyStreamAddArgs());
        verify(listener).ack(MESSAGE_ID);
        assertThat(warnLogs())
                .as("开关关闭导致的丢弃必须可观测")
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("secondary-dlq-enabled=false"));
    }

    @Test
    @DisplayName("secondary-dlq-enabled=true：策略返回 SECONDARY_DLQ 时写入 dlq2 并 ACK")
    void secondaryEnabled_writesDlq2() {
        when(reg.isDlqMode()).thenReturn(true);
        DlqFailureStrategy strategy = mock(DlqFailureStrategy.class);
        when(strategy.name()).thenReturn("secondary-strategy");
        when(strategy.decide(any(), any())).thenReturn(DlqFailureDecision.secondaryDlq());
        DlqConfig dlqConfig = DlqConfig.builder().secondaryDlqEnabled(true).build();
        DefaultRetryAndDlqHandler handler =
                newHandler(mock(RetryPolicy.class), strategy, dlqConfig);

        String dlq2Key = StreamMQKeys.secondaryDlqStream(NAMESPACE, GROUP, "dlq2");
        doReturn(dlq2Stream).when(redisson).getStream(eq(dlq2Key), any(StringCodec.class));

        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(0),
                reg,
                listener,
                new IllegalStateException("dlq consumer failed"));

        verify(dlq2Stream, times(1)).add(anyStreamAddArgs());
        verify(listener).ack(MESSAGE_ID);
    }

    // ===================== 工具 =====================

    private DefaultRetryAndDlqHandler newHandler(RetryPolicy policy, DlqConfig dlqConfig) {
        return newHandler(policy, mock(DlqFailureStrategy.class), dlqConfig);
    }

    private DefaultRetryAndDlqHandler newHandler(
            RetryPolicy policy, DlqFailureStrategy strategy, DlqConfig dlqConfig) {
        return new DefaultRetryAndDlqHandler(
                redisson,
                messageConverter,
                policy,
                mock(ConsumerInterceptorChain.class),
                strategy,
                dlqConfig);
    }

    private static Message<String> message(int reconsumeTimes) {
        return MessageBuilder.<String>withTopic(TOPIC)
                .body("payload")
                .messageId(MESSAGE_ID)
                .reconsumeTimes(reconsumeTimes)
                .build();
    }

    private List<ILoggingEvent> warnLogs() {
        return logAppender.list.stream()
                .filter(event -> Level.WARN.equals(event.getLevel()))
                .toList();
    }

    private static StreamAddArgs<String, String> anyStreamAddArgs() {
        return org.mockito.ArgumentMatchers.any();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RStream<String, String> mockStream() {
        return mock(RStream.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RStreamAsync<String, String> mockAsyncStream() {
        return mock(RStreamAsync.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RMapAsync<String, String> mockMapAsync() {
        return mock(RMapAsync.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RScoredSortedSetAsync<String> mockZsetAsync() {
        return mock(RScoredSortedSetAsync.class);
    }
}
