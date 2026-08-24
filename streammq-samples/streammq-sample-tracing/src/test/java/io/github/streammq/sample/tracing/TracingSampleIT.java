package io.github.streammq.sample.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.tracing.StreamMQTracing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Tracing 示例集成测试。
 *
 * <p>验证发送消息后消费者能收到带 W3C traceparent 属性的消息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = {TracingApplication.class, TracingSampleIT.TestConfig.class})
@Import(TracingSampleIT.TestEventCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("Tracing 示例集成测试")
class TracingSampleIT {

    private static final String TEST_CONSUMER_GROUP = "tracing-test-consumer";
    private static final String TRACEPARENT_REGEX = "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}";

    @Autowired private EventProducer eventProducer;

    @Autowired private TestEventCollector testCollector;

    @BeforeEach
    void clear() {
        testCollector.receivedMessages.clear();
        testCollector.lastTraceparent.set(null);
    }

    @Test
    @DisplayName("发送事件后消费者应收到带 traceparent 的消息")
    void shouldPropagateTraceContext() {
        SendResult result = eventProducer.emitEvent("EVT-001", "event-payload");
        assertThat(result.isSuccess()).isTrue();

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testCollector.receivedMessages).hasSize(1);
                            String traceparent = testCollector.lastTraceparent.get();
                            assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);
                        });
    }

    /** 测试配置类，提供真实 OpenTelemetry SDK 实例。 */
    static class TestConfig {

        @Bean
        public OpenTelemetry streamMQOpenTelemetry() {
            SdkTracerProvider tracerProvider =
                    SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).build();
            return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
        }
    }

    /** 测试专用消息收集器，记录 traceparent 属性。 */
    @StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = TEST_CONSUMER_GROUP)
    static class TestEventCollector implements StreamMessageConcurrentlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();
        final AtomicReference<String> lastTraceparent = new AtomicReference<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            receivedMessages.add(message);
            String tp = message.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
            if (tp != null) {
                lastTraceparent.set(tp);
            }
            return ConsumeAction.SUCCESS;
        }
    }
}
