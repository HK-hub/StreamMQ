/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.interceptor.ConsumerInterceptorChain;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.policy.DlqConfig;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;

/**
 * 顺序消费失败路由回归测试（发布前红队审查 R5）。
 *
 * <p>缺陷背景：顺序消费者没有 retry 消费循环（{@code DefaultConsumeLoopSupervisor} 只为 AUTO_ACK 提交 retry
 * 循环），因此任何"未被处理"的 ORDERLY 消息若走到 {@code handleReconsumeLater}，只会被写入 retry ZSet → 由 {@code
 * RetryScheduler} 转投进 retry Stream，而没有任何循环读取该 Stream——消息静默沉没。
 *
 * <p>可达路径：过滤器求值异常、{@code processMessage} 的 {@code Throwable} 兜底。修复后统一在 {@code handleAction} 收口为"直接进
 * DLQ + ACK"。
 */
@DisplayName("ORDERLY 失败路由统一收口到 DLQ")
class OrderlyFailureRoutingTest {

    private static final MessageId MESSAGE_ID = MessageId.fromStreamEntry("1700000000000-0");

    private RedissonClient redisson;
    private MessageConverter messageConverter;
    private ListenerRegistration<?> reg;
    private StreamMQListener listener;
    private RStream<String, String> dlqStream;
    private DefaultRetryAndDlqHandler handler;

    @BeforeEach
    void setUp() {
        redisson = mock(RedissonClient.class);
        messageConverter = mock(MessageConverter.class);
        reg = mock(ListenerRegistration.class);
        listener = mock(StreamMQListener.class);
        dlqStream = mockStream();

        when(reg.isDlqMode()).thenReturn(false);
        when(reg.getType()).thenReturn(ListenerType.ORDERLY);
        when(reg.getTopic()).thenReturn("orderly-topic");
        when(reg.getGroup()).thenReturn("orderly-group");
        when(reg.getNamespace()).thenReturn("ns");
        when(messageConverter.toStreamFields(any())).thenReturn(Map.of("body", "cGF5bG9hZA=="));
        when(redisson.<String, String>getStream(anyString(), any(StringCodec.class)))
                .thenReturn(dlqStream);

        handler =
                new DefaultRetryAndDlqHandler(
                        redisson,
                        messageConverter,
                        mock(RetryPolicy.class),
                        mock(ConsumerInterceptorChain.class),
                        mock(DlqFailureStrategy.class),
                        DlqConfig.builder().build());
    }

    @Test
    @DisplayName("ORDERLY + RECONSUME_LATER：直接进 DLQ 并 ACK，绝不写 retry 调度")
    void orderlyReconsumeLater_routesToDlq_neverSchedulesRetry() {
        handler.handleAction(
                ConsumeAction.RECONSUME_LATER,
                message(),
                reg,
                listener,
                new IllegalStateException());

        verify(dlqStream, times(1)).add(anyStreamAddArgs());
        verify(listener, times(1)).ack(MESSAGE_ID);
        // createBatch 是 scheduleRetry / scheduleDlqRetry 的唯一入口：
        // 一旦被调用，说明消息又走进了"无人消费的 retry Stream"。
        verify(redisson, never()).createBatch(any());
    }

    @Test
    @DisplayName("ORDERLY + DEFER：同样收口到 DLQ（DEFER 对 ORDERLY 无投递路径）")
    void orderlyDefer_routesToDlq() {
        handler.handleAction(
                ConsumeAction.defer(java.time.Duration.ofSeconds(1)),
                message(),
                reg,
                listener,
                null);

        verify(dlqStream, times(1)).add(anyStreamAddArgs());
        verify(listener, times(1)).ack(MESSAGE_ID);
        verify(redisson, never()).createBatch(any());
    }

    /** 泛型匹配器：避免 {@code any(StreamAddArgs.class)} 的原始类型告警（{@code -Werror} 下致命）。 */
    private static StreamAddArgs<String, String> anyStreamAddArgs() {
        return org.mockito.ArgumentMatchers.any();
    }

    /**
     * {@code mock(RStream.class)} 的原始类型返回值需要一次显式泛型转换（集中在助手内）。
     *
     * <p>{@code rawtypes} 必须显式列出：它是 javac 默认开启的告警类别，{@code -Werror} 下为致命， 且不被 {@code unchecked} 覆盖。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RStream<String, String> mockStream() {
        return mock(RStream.class);
    }

    private static Message<String> message() {
        return MessageBuilder.<String>withTopic("orderly-topic")
                .body("payload")
                .messageId(MESSAGE_ID)
                .build();
    }
}
