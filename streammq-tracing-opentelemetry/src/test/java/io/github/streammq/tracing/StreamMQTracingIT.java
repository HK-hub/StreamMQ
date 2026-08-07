package io.github.streammq.tracing;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.tracing.model.TopologyGraph;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * StreamMQ Tracing 模块真实集成测试。
 *
 * <p>使用本地 Redis（127.0.0.1:6379）验证 OpenTelemetry 追踪拦截器在真实消息收发链路中的行为：
 * <ul>
 *   <li>生产者拦截器在发送前注入 W3C {@code traceparent} 到消息属性</li>
 *   <li>消费者拦截器在消费前提取 {@code traceparent} 并建立远程父级关系</li>
 *   <li>核心追踪服务记录 TraceRecord 到 Redis，{@link StreamMQTopologyService} 据此构建拓扑图</li>
 * </ul>
 *
 * <p>同时启用核心追踪（{@code streammq.tracing.enabled=true}）与 OpenTelemetry 追踪
 * （{@code streammq.tracing.otel.enabled=true}），验证两套追踪体系协同工作。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@SpringBootTest(classes = StreamMQTracingIT.TestApplication.class,
        properties = {
                "spring.application.name=streammq-tracing-opentelemetry-it",
                "streammq.enabled=true",
                "streammq.namespace=tracing-it",
                "streammq.producer.group=tracing-it-producer",
                "streammq.tracing.enabled=true",
                "streammq.tracing.otel.enabled=true",
                "streammq.trace.enabled=true",
                "streammq.trace.storage=redis",
                "redisson.singleServerConfig.address=redis://127.0.0.1:6379"
        })
@DirtiesContext
@DisplayName("StreamMQ Tracing 真实集成测试")
class StreamMQTracingIT {

    @DynamicPropertySource
    static void redisPassword(DynamicPropertyRegistry registry) {
        String password = System.getProperty("test.redis.password",
                System.getenv().getOrDefault("STREAMMQ_TEST_REDIS_PASSWORD", ""));
        if (!password.isEmpty()) {
            registry.add("redisson.singleServerConfig.password", () -> password);
        }
    }

    private static final String TOPIC = "tracing-it-topic";
    private static final String CONSUMER_GROUP = "tracing-it-cg";
    private static final String TRACEPARENT_REGEX = "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}";

    @Autowired
    private StreamMessageTemplate template;

    @Autowired
    private StreamMQTracing tracing;

    @Autowired
    private StreamMQTopologyService topologyService;

    @Autowired
    private TracingTestConsumer testConsumer;

    @BeforeEach
    void clearState() {
        testConsumer.clear();
    }

    @Test
    @DisplayName("发送消息后消费者应收到完整内容，且消息属性包含 W3C traceparent")
    void shouldSendAndReceiveWithTraceContext() {
        template.syncSend(io.github.streammq.core.message.MessageBuilder.<String>withTopic(TOPIC)
                .tag("tracing-test")
                .keys("trace-key-1")
                .body("hello-tracing")
                .build());

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(testConsumer.getReceived()).hasSize(1));

        // 验证消息内容
        assertThat(testConsumer.getReceived().get(0)).isEqualTo("hello-tracing");

        // 验证消息属性中包含 W3C traceparent（由 OpenTelemetryProducerInterceptor 注入）
        String traceparent = testConsumer.getLastTraceparent();
        assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);
    }

    @Test
    @DisplayName("StreamMQTracing 应被自动装配并注入有效 Tracer")
    void shouldAutoConfigureTracing() {
        assertThat(tracing).isNotNull();
        assertThat(tracing.getOpenTelemetry()).isNotNull();
        assertThat(tracing.getTracer()).isNotNull();
    }

    @Test
    @DisplayName("StreamMQTopologyService 应被自动装配且能查询拓扑图")
    void shouldAutoConfigureTopologyService() {
        assertThat(topologyService).isNotNull();

        // 先发送一条消息，确保有追踪记录
        template.syncSend(io.github.streammq.core.message.MessageBuilder.<String>withTopic(TOPIC)
                .tag("topology-test")
                .keys("topology-key")
                .body("topology-msg")
                .build());

        // 等待消息被消费
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(testConsumer.getReceived()).hasSize(1));

        // 查询拓扑图（即使时间窗口内无记录，也应返回空拓扑而非 null）
        TopologyGraph topology = topologyService.getTopicTopology(TOPIC);
        assertThat(topology).isNotNull();
        assertThat(topology.topic()).isEqualTo(TOPIC);
    }

    /**
     * 测试消费者，记录收到的消息和 traceparent 属性。
     */
    @Component
    @StreamMQConsumer(
            topic = TOPIC,
            consumerGroup = CONSUMER_GROUP,
            maxReconsumeTimes = 3
    )
    static class TracingTestConsumer implements StreamMessageConcurrentlyConsumer<String> {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicReference<String> lastTraceparent = new AtomicReference<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            received.add(message.getBody());
            // 提取 traceparent（由生产者拦截器注入到 userProperties，经 Redis Stream 透传）
            String traceparent = message.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
            if (traceparent != null) {
                lastTraceparent.set(traceparent);
            }
            return ConsumeAction.SUCCESS;
        }

        List<String> getReceived() {
            return received;
        }

        String getLastTraceparent() {
            return lastTraceparent.get();
        }

        void clear() {
            received.clear();
            lastTraceparent.set(null);
        }
    }

    /**
     * 测试 Spring Boot 应用。
     *
     * <p>提供真实 {@link OpenTelemetrySdk} Bean（而非 no-op 默认实例），
     * 确保 {@link StreamMQTracing#injectProducerSpan} 能创建有效 Span 并注入 W3C traceparent。
     */
    @SpringBootApplication
    static class TestApplication {

        @Bean
        public TracingTestConsumer tracingTestConsumer() {
            return new TracingTestConsumer();
        }

        /**
         * 真实 OpenTelemetry SDK 实例（alwaysOn 采样器），覆盖自动配置的 no-op 默认实例。
         *
         * @return OpenTelemetry SDK 实例
         */
        @Bean
        public OpenTelemetry streamMQOpenTelemetry() {
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setSampler(Sampler.alwaysOn())
                .build();
            return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        }
    }
}
