/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.message.Message;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@link StreamMQTracing} 单元测试，验证 W3C TraceContext 的注入、提取与 Span 生命周期。 */
@DisplayName("StreamMQTracing 上下文传播测试")
class StreamMQTracingTest {

    private static final String TRACEPARENT_REGEX = "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}";

    private SdkTracerProvider tracerProvider;
    private OpenTelemetrySdk openTelemetry;
    private StreamMQTracing tracing;

    @BeforeEach
    void setUp() {
        tracerProvider = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).build();
        openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
        tracing = new StreamMQTracing(openTelemetry);
    }

    @AfterEach
    void tearDown() {
        if (java.util.Objects.nonNull(tracerProvider)) {
            tracerProvider.shutdown();
        }
    }

    @Test
    @DisplayName("injectProducerSpan 应将合法 W3C traceparent 注入派生消息属性")
    void injectProducerSpan_shouldInjectTraceparent() {
        Message<String> message = buildMessage("order-topic");

        Message<?> enriched = tracing.injectProducerSpan(message);

        String traceparent = enriched.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
        assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);

        // 通过消息配对结束生产者 Span（注册表按派生消息引用查找）
        tracing.endProducerSpan(enriched, true);
    }

    @Test
    @DisplayName("startConsumerSpan 应从派生消息属性提取父级上下文，复用同一 traceId")
    void startConsumerSpan_shouldExtractParentContext() {
        Message<String> message = buildMessage("order-topic");
        Message<?> enriched = tracing.injectProducerSpan(message);
        String producerTraceparent =
                enriched.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
        String producerTraceId = producerTraceparent.split("-", 4)[1];

        // 消费端拿到的是注入后的消息（Redis 往返后即该形态）
        @SuppressWarnings("unchecked")
        Message<String> consumed = (Message<String>) enriched;
        Span consumerSpan = tracing.startConsumerSpan(consumed, buildContext("order-group", 0));

        assertThat(consumerSpan).isNotNull();
        assertThat(consumerSpan.getSpanContext().isValid()).isTrue();
        // 消费者 Span 应继承生产者的 traceId，证明上下文提取成功
        assertThat(consumerSpan.getSpanContext().getTraceId()).isEqualTo(producerTraceId);

        tracing.endSpan(consumerSpan, true);
        tracing.endProducerSpan(enriched, true);
    }

    @Test
    @DisplayName("无 traceparent 时 startConsumerSpan 应启动独立 trace")
    void startConsumerSpan_shouldStartNewTraceWhenAbsent() {
        Message<String> message = buildMessage("order-topic");

        Span consumerSpan = tracing.startConsumerSpan(message, buildContext("order-group", 1));

        assertThat(consumerSpan).isNotNull();
        assertThat(consumerSpan.getSpanContext().isValid()).isTrue();
        tracing.endSpan(consumerSpan, false, "消费失败模拟");
    }

    @Test
    @DisplayName("endSpan 失败重载应安全处理 null Span；endProducerSpan 未配对时静默跳过")
    void endSpan_shouldHandleNullSpan() {
        tracing.endSpan(null, false);
        tracing.endSpan(null, false, "error");
        tracing.endProducerSpan(null, true);
        // 无配对消息引用：静默跳过，无异常即通过
        tracing.endProducerSpan(buildMessage("t"), true);
    }

    @Test
    @DisplayName("no-op OpenTelemetry 时注入应跳过 traceparent（优雅降级）")
    void injectProducerSpan_shouldSkipWhenNoop() {
        StreamMQTracing noopTracing = new StreamMQTracing(OpenTelemetry.noop());
        Message<String> message = buildMessage("order-topic");

        Message<?> result = noopTracing.injectProducerSpan(message);

        assertThat(result.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY)).isNull();
        // 无有效 Span 时 endProducerSpan 应为安全 no-op
        noopTracing.endProducerSpan(result, true);
    }

    private Message<String> buildMessage(String topic) {
        return new Message<>(
                topic,
                "created",
                "order-123",
                null,
                null,
                null,
                "payload",
                null,
                null,
                System.currentTimeMillis(),
                "host:8080",
                null,
                0);
    }

    private ConsumeContext buildContext(String group, int reconsumeTimes) {
        return new ConsumeContext() {
            @Override
            public String topic() {
                return "order-topic";
            }

            @Override
            public String consumerGroup() {
                return group;
            }

            @Override
            public String consumerName() {
                return "OrderConsumer";
            }

            @Override
            public int reconsumeTimes() {
                return reconsumeTimes;
            }

            @Override
            public long bornTimestamp() {
                return System.currentTimeMillis();
            }

            @Override
            public String bornHost() {
                return "host:8080";
            }

            @Override
            public Map<String, String> messageTrack() {
                return Collections.emptyMap();
            }

            @Override
            public String ext(String key) {
                return null;
            }
        };
    }
}
