package io.github.streammq.tracing;

import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.message.Message;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link StreamMQTracing} 单元测试，验证 W3C TraceContext 的注入、提取与 Span 生命周期。
 */
@DisplayName("StreamMQTracing 上下文传播测试")
class StreamMQTracingTest {

    private static final String TRACEPARENT_REGEX = "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}";

    private SdkTracerProvider tracerProvider;
    private OpenTelemetrySdk openTelemetry;
    private StreamMQTracing tracing;

    @BeforeEach
    void setUp() {
        tracerProvider = SdkTracerProvider.builder()
            .setSampler(Sampler.alwaysOn())
            .build();
        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .build();
        tracing = new StreamMQTracing(openTelemetry);
    }

    @AfterEach
    void tearDown() {
        if (java.util.Objects.nonNull(tracerProvider)) {
            tracerProvider.shutdown();
        }
        tracing.clearCurrentProducerSpan();
    }

    @Test
    @DisplayName("injectProducerSpan 应将合法 W3C traceparent 注入消息属性")
    void injectProducerSpan_shouldInjectTraceparent() {
        Message<String> message = buildMessage("order-topic");

        tracing.injectProducerSpan(message);

        String traceparent = message.getProperties().get(StreamMQTracing.TRACEPARENT_KEY);
        assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);

        Span producerSpan = tracing.getCurrentProducerSpan();
        assertThat(producerSpan).isNotNull();
        SpanContext ctx = producerSpan.getSpanContext();
        assertThat(ctx.isValid()).isTrue();
        // traceparent 中的 traceId / spanId 应与生产者 Span 一致
        String[] parts = traceparent.split("-", 4);
        assertThat(parts[1]).isEqualTo(ctx.getTraceId());
        assertThat(parts[2]).isEqualTo(ctx.getSpanId());

        tracing.endSpan(producerSpan, true);
        tracing.clearCurrentProducerSpan();
    }

    @Test
    @DisplayName("startConsumerSpan 应从消息属性提取父级上下文，复用同一 traceId")
    void startConsumerSpan_shouldExtractParentContext() {
        Message<String> message = buildMessage("order-topic");
        tracing.injectProducerSpan(message);
        Span producerSpan = tracing.getCurrentProducerSpan();
        String producerTraceId = producerSpan.getSpanContext().getTraceId();

        Span consumerSpan = tracing.startConsumerSpan(message, buildContext("order-group", 0));

        assertThat(consumerSpan).isNotNull();
        assertThat(consumerSpan.getSpanContext().isValid()).isTrue();
        // 消费者 Span 应继承生产者的 traceId，证明上下文提取成功
        assertThat(consumerSpan.getSpanContext().getTraceId()).isEqualTo(producerTraceId);

        tracing.endSpan(consumerSpan, true);
        tracing.endSpan(producerSpan, true);
        tracing.clearCurrentProducerSpan();
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
    @DisplayName("endSpan 失败重载应安全处理 null Span")
    void endSpan_shouldHandleNullSpan() {
        tracing.endSpan(null, false);
        tracing.endSpan(null, false, "error");
        // 无异常即通过
        assertThat(tracing.getCurrentProducerSpan()).isNull();
    }

    @Test
    @DisplayName("no-op OpenTelemetry 时注入应跳过 traceparent（优雅降级）")
    void injectProducerSpan_shouldSkipWhenNoop() {
        StreamMQTracing noopTracing = new StreamMQTracing(OpenTelemetry.noop());
        Message<String> message = buildMessage("order-topic");

        noopTracing.injectProducerSpan(message);

        assertThat(message.getProperties().get(StreamMQTracing.TRACEPARENT_KEY)).isNull();
        Span span = noopTracing.getCurrentProducerSpan();
        assertThat(span).isNotNull();
        noopTracing.endSpan(span, true);
        noopTracing.clearCurrentProducerSpan();
    }

    private Message<String> buildMessage(String topic) {
        Message<String> message = new Message<>();
        message.setTopic(topic);
        message.setTag("created");
        message.setKeys("order-123");
        message.setBody("payload");
        return message;
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
