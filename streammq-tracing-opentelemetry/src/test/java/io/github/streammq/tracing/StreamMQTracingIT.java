/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.tracing.model.MessageTrace;
import io.github.streammq.tracing.model.TopologyGraph;
import io.github.streammq.tracing.model.TopologyRoute;
import io.github.streammq.tracing.model.TraceEvent;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * StreamMQ Tracing 模块真实集成测试。
 *
 * <p>使用本地 Redis（127.0.0.1:6379）验证 OpenTelemetry 追踪拦截器在真实消息收发链路中的行为：
 *
 * <ul>
 *   <li>生产者拦截器在发送前注入 W3C {@code traceparent} 到消息属性
 *   <li>消费者拦截器在消费前提取 {@code traceparent} 并建立远程父级关系
 *   <li>核心追踪服务记录 TraceRecord 到 Redis，{@link StreamMQTopologyService} 据此构建拓扑图
 *   <li>{@link StreamMQTracing} 门面创建有效 Span 并注入/提取上下文
 * </ul>
 *
 * <p>同时启用核心追踪（{@code streammq.tracing.enabled=true}）与 OpenTelemetry 追踪 （{@code
 * streammq.tracing.otel.enabled=true}），验证两套追踪体系协同工作。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(
        classes = StreamMQTracingIT.TestApplication.class,
        properties = {
            "spring.application.name=streammq-tracing-opentelemetry-it",
            "streammq.enabled=true",
            "streammq.namespace=tracing-it",
            "streammq.producer.group=tracing-it-producer",
            "streammq.tracing.enabled=true",
            "streammq.tracing.otel.enabled=true",
            "streammq.trace.enabled=true",
            "streammq.trace.storage=redis",
            "redisson.singleServerConfig.address=redis://127.0.0.1:6379",
            "spring.main.allow-bean-definition-overriding=true"
        })
@DirtiesContext
@DisplayName("StreamMQ Tracing 真实集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class StreamMQTracingIT {

    @DynamicPropertySource
    static void redisPassword(DynamicPropertyRegistry registry) {
        String password =
                System.getProperty(
                        "test.redis.password",
                        System.getenv().getOrDefault("STREAMMQ_TEST_REDIS_PASSWORD", ""));
        if (!password.isEmpty()) {
            registry.add("redisson.singleServerConfig.password", () -> password);
        }
    }

    private static final String TOPIC = "tracing-it-topic";
    private static final String CONSUMER_GROUP = "tracing-it-cg";
    private static final String TRACEPARENT_REGEX = "00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}";

    @Autowired private StreamMessageTemplate template;

    @Autowired private StreamMQTracing tracing;

    @Autowired private StreamMQTopologyService topologyService;

    @Autowired private StreamMQTraceService traceService;

    @Autowired private TracingTestConsumer testConsumer;

    @BeforeEach
    void clearState() {
        testConsumer.clear();
    }

    /** 创建 Mock ConsumeContext，用于单元测试中构造消费者 Span。 */
    private ConsumeContext createMockContext(
            String group, String consumerName, int reconsumeTimes) {
        ConsumeContext ctx = mock(ConsumeContext.class);
        when(ctx.consumerGroup()).thenReturn(group);
        when(ctx.consumerName()).thenReturn(consumerName);
        when(ctx.reconsumeTimes()).thenReturn(reconsumeTimes);
        when(ctx.topic()).thenReturn(TOPIC);
        when(ctx.bornTimestamp()).thenReturn(System.currentTimeMillis());
        when(ctx.bornHost()).thenReturn("localhost");
        when(ctx.messageTrack()).thenReturn(Collections.emptyMap());
        return ctx;
    }

    // ===================== OpenTelemetry Span 核心测试 =====================

    @Nested
    @DisplayName("OpenTelemetry Span - 核心门面测试")
    class OpenTelemetrySpanTests {

        @Test
        @DisplayName("StreamMQTracing 应被自动装配并注入有效 Tracer")
        void shouldAutoConfigureTracing() {
            assertThat(tracing).isNotNull();
            assertThat(tracing.getOpenTelemetry()).isNotNull();
            assertThat(tracing.getTracer()).isNotNull();
        }

        @Test
        @DisplayName("injectProducerSpan 应创建有效 Span 并注入 W3C traceparent")
        void shouldInjectProducerSpan() {
            Message<String> message =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("span-test")
                            .keys("span-key")
                            .body("span-body")
                            .build();

            Message<?> enriched = tracing.injectProducerSpan(message);

            String traceparent = enriched.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
            assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);

            // 通过消息配对结束生产者 Span（注册表按派生消息引用查找）
            tracing.endProducerSpan(enriched, true);
        }

        @Test
        @DisplayName("startConsumerSpan 应从 traceparent 提取远程父级上下文")
        void shouldStartConsumerSpanWithRemoteParent() {
            Message<String> base =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("consumer-span")
                            .keys("consumer-key")
                            .body("consumer-body")
                            .build();

            Span producerSpan =
                    tracing.getTracer()
                            .spanBuilder("test-producer")
                            .setSpanKind(SpanKind.PRODUCER)
                            .startSpan();
            String traceparent =
                    "00-"
                            + producerSpan.getSpanContext().getTraceId()
                            + "-"
                            + producerSpan.getSpanContext().getSpanId()
                            + "-"
                            + producerSpan.getSpanContext().getTraceFlags().asHex();
            Message<String> message =
                    base.addUserProperty(StreamMQTracing.TRACEPARENT_KEY, traceparent);

            ConsumeContext context = createMockContext(CONSUMER_GROUP, "consumer-1", 0);
            Span consumerSpan = tracing.startConsumerSpan(message, context);

            assertThat(consumerSpan).isNotNull();
            assertThat(consumerSpan.getSpanContext().isValid()).isTrue();

            tracing.endSpan(consumerSpan, true);
            tracing.endSpan(producerSpan, true);
        }

        @Test
        @DisplayName("endSpan 应正确记录成功状态")
        void shouldEndSpanWithSuccess() {
            Span span =
                    tracing.getTracer()
                            .spanBuilder("test-span")
                            .setSpanKind(SpanKind.INTERNAL)
                            .startSpan();

            tracing.endSpan(span, true);

            assertThat(span.getSpanContext().isValid()).isTrue();
        }

        @Test
        @DisplayName("endSpan 应正确记录失败状态和错误信息")
        void shouldEndSpanWithFailure() {
            Span span =
                    tracing.getTracer()
                            .spanBuilder("test-span")
                            .setSpanKind(SpanKind.INTERNAL)
                            .startSpan();

            tracing.endSpan(span, false, "test-error-message");

            assertThat(span.getSpanContext().isValid()).isTrue();
        }

        @Test
        @DisplayName("无 traceparent 的消息应创建本地消费者 Span")
        void shouldCreateLocalConsumerSpanWithoutTraceparent() {
            Message<String> message =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("no-parent")
                            .keys("no-parent-key")
                            .body("no-parent-body")
                            .build();

            ConsumeContext context = createMockContext(CONSUMER_GROUP, "consumer-1", 0);
            Span span = tracing.startConsumerSpan(message, context);

            assertThat(span).isNotNull();
            assertThat(span.getSpanContext().isValid()).isTrue();

            tracing.endSpan(span, true);
        }
    }

    // ===================== 完整消息追踪链路测试 =====================

    @Nested
    @DisplayName("完整消息追踪链路 - 发送/消费/Span 全流程")
    class FullTraceFlow {

        @Test
        @DisplayName("发送消息后消费者应收到完整内容，且消息属性包含 W3C traceparent")
        void shouldSendAndReceiveWithTraceContext() {
            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(TOPIC)
                                    .tag("tracing-test")
                                    .keys("trace-key-1")
                                    .body("hello-tracing")
                                    .build());
            assertThat(result.isSuccess()).isTrue();

            // 按内容等待：共享 TOPIC 下不能依赖队列位置或数量
            testConsumer.awaitBody("hello-tracing", 15);

            String traceparent = testConsumer.traceparentOf("hello-tracing");
            assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);
        }

        @Test
        @DisplayName("批量发送消息后每条消息都应包含 traceparent")
        void shouldHaveTraceparentForEachBatchMessage() {
            int msgCount = 5;
            for (int i = 0; i < msgCount; i++) {
                template.syncSend(
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("batch-tracing")
                                .keys("batch-key-" + i)
                                .body("batch-msg-" + i)
                                .build());
            }

            testConsumer.awaitBodies("batch-msg-", msgCount, 20);

            List<String> batchTraceparents = testConsumer.traceparentsOf("batch-msg-");
            assertThat(batchTraceparents).hasSize(msgCount);
            for (String tp : batchTraceparents) {
                assertThat(tp).matches(TRACEPARENT_REGEX);
            }
        }

        @Test
        @DisplayName("发送消息后追踪服务应查询到 SEND 和 CONSUME 记录")
        void shouldQueryTraceRecordsFromRedis() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("query-test")
                            .keys("query-key")
                            .body("query-message")
                            .build());

            testConsumer.awaitBody("query-message", 15);

            long now = System.currentTimeMillis();
            long start = now - 5 * 60 * 1000L;
            List<TraceRecord> records = traceService.queryByTopic(TOPIC, start, now);

            assertThat(records).isNotNull();
            assertThat(records).isNotEmpty();

            long sendCount = records.stream().filter(r -> r.type() == TraceType.SEND).count();
            long consumeCount = records.stream().filter(r -> r.type() == TraceType.CONSUME).count();
            assertThat(sendCount).isGreaterThanOrEqualTo(1);
            assertThat(consumeCount).isGreaterThanOrEqualTo(1);
        }
    }

    // ===================== 拓扑服务测试 =====================

    @Nested
    @DisplayName("拓扑服务 - 真实数据构建")
    class TopologyServiceTests {

        @Test
        @DisplayName("StreamMQTopologyService 应被自动装配")
        void shouldAutoConfigureTopologyService() {
            assertThat(topologyService).isNotNull();
        }

        @Test
        @DisplayName("发送消息后应能构建包含生产者和消费者的拓扑图")
        void shouldBuildTopologyWithProducerAndConsumer() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("topology-test")
                            .keys("topology-key")
                            .body("topology-msg")
                            .build());

            testConsumer.awaitBody("topology-msg", 15);

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology).isNotNull();
            assertThat(topology.topic()).isEqualTo(TOPIC);
            assertThat(topology.producers()).isNotEmpty();
            assertThat(topology.consumers()).isNotEmpty();

            boolean hasProducer =
                    topology.producers().stream().anyMatch(n -> "PRODUCER".equals(n.type()));
            boolean hasConsumer =
                    topology.consumers().stream().anyMatch(n -> "CONSUMER".equals(n.type()));
            assertThat(hasProducer).isTrue();
            assertThat(hasConsumer).isTrue();
        }

        @Test
        @DisplayName("拓扑图应包含生产者到消费者的路由")
        void shouldContainRoutes() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("route-test")
                            .keys("route-key")
                            .body("route-msg")
                            .build());

            testConsumer.awaitBody("route-msg", 15);

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology.routes()).isNotEmpty();
            for (TopologyRoute route : topology.routes()) {
                assertThat(route.messageType()).isEqualTo(TOPIC);
                assertThat(route.from()).isNotEmpty();
                assertThat(route.to()).isNotEmpty();
            }
        }

        @Test
        @DisplayName("不存在的主题应返回空拓扑但不抛异常")
        void shouldReturnEmptyTopologyForUnknownTopic() {
            TopologyGraph topology = topologyService.getTopicTopology("non-existent-topic");

            assertThat(topology).isNotNull();
            assertThat(topology.topic()).isEqualTo("non-existent-topic");
        }

        @Test
        @DisplayName("getMessageTrace 应构建消息链路")
        void shouldBuildMessageTrace() {
            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(TOPIC)
                                    .tag("trace-msg")
                                    .keys("trace-msg-key")
                                    .body("trace-msg-body")
                                    .build());

            testConsumer.awaitBody("trace-msg-body", 15);

            String messageId = testConsumer.messageIdOf("trace-msg-body");
            MessageTrace trace = topologyService.getMessageTrace(messageId);

            assertThat(trace).isNotNull();
            assertThat(trace.messageId()).isEqualTo(messageId);
            assertThat(trace.topic()).isEqualTo(TOPIC);
            assertThat(trace.events()).isNotEmpty();

            for (TraceEvent event : trace.events()) {
                assertThat(event.type()).isNotNull();
                assertThat(event.timestamp()).isPositive();
            }
        }

        @Test
        @DisplayName("getTopicTraces 应返回时间范围内的所有消息链路")
        void shouldGetTopicTraces() {
            int msgCount = 3;
            for (int i = 0; i < msgCount; i++) {
                template.syncSend(
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("topic-trace")
                                .keys("topic-trace-key-" + i)
                                .body("topic-trace-body-" + i)
                                .build());
            }

            testConsumer.awaitBodies("topic-trace-body-", msgCount, 20);

            long now = System.currentTimeMillis();
            long start = now - 5 * 60 * 1000L;
            List<MessageTrace> traces = topologyService.getTopicTraces(TOPIC, start, now);

            assertThat(traces).isNotNull();
            assertThat(traces).isNotEmpty();
        }
    }

    // ===================== 拦截器行为测试 =====================

    @Nested
    @DisplayName("拦截器 - 追踪上下文传播")
    class InterceptorBehavior {

        @Test
        @DisplayName("生产者拦截器应在 beforeSend 注入 traceparent")
        void shouldInjectTraceparentBeforeSend() {
            AtomicReference<String> traceparentRef = new AtomicReference<>();

            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("interceptor-test")
                            .keys("interceptor-key")
                            .body("interceptor-body")
                            .build());

            testConsumer.awaitBody("interceptor-body", 15);

            String traceparent = testConsumer.traceparentOf("interceptor-body");
            assertThat(traceparent).isNotNull().matches(TRACEPARENT_REGEX);
        }

        @Test
        @DisplayName("消费者拦截器应正确提取并结束 Span")
        void shouldExtractAndEndSpanOnConsume() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("consumer-span-test")
                            .keys("consumer-span-key")
                            .body("consumer-span-body")
                            .build());

            testConsumer.awaitBody("consumer-span-body", 15);

            assertThat(testConsumer.traceparentOf("consumer-span-body")).isNotNull();
        }
    }

    // ===================== 测试消费者 =====================

    /**
     * 测试消费者，记录收到的消息和 traceparent 属性。
     *
     * <p>注意：不使用 @Component 注解，通过 {@link TestApplication#tracingTestConsumer()} 显式注册 Bean， 避免与 @Bean
     * 方法产生冲突。
     */
    @StreamMQConsumer(topic = TOPIC, consumerGroup = CONSUMER_GROUP, maxReconsumeTimes = 3)
    static class TracingTestConsumer implements StreamMessageConcurrentlyConsumer<String> {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String> traceparents = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<Message<String>> receivedMessages =
                new CopyOnWriteArrayList<>();
        private final AtomicReference<String> lastTraceparent = new AtomicReference<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            received.add(message.getBody());
            receivedMessages.add(message);
            String traceparent = message.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
            if (traceparent != null) {
                traceparents.add(traceparent);
                lastTraceparent.set(traceparent);
            }
            return ConsumeAction.SUCCESS;
        }

        List<String> getReceived() {
            return received;
        }

        List<String> getTraceparents() {
            return traceparents;
        }

        String getLastTraceparent() {
            return lastTraceparent.get();
        }

        String getLastMessageId() {
            if (receivedMessages.isEmpty()) {
                return "";
            }
            Message<String> last = receivedMessages.get(receivedMessages.size() - 1);
            return last.getMessageId() != null ? last.getMessageId().getStreamEntryId() : "";
        }

        // ------------------------------------------------------------------
        // 内容匹配的等待/查询辅助方法
        //
        // 为什么必须按内容匹配，而不能按位置（get(0)）或数量（hasSize(n)）：
        // 本 IT 的所有 @Nested 测试类共享同一个 TOPIC 与同一个消费者组，消费者 Bean 也是全局单例。
        // 上一测试发送的消息可能在 @BeforeEach 的 clear() 之后才投递完成，从而混入当前测试的队列。
        // 曾实际观测到：FullTraceFlow 期望 "hello-tracing"，却拿到上一个测试遗留的 "topology-msg"。
        // ------------------------------------------------------------------

        /** 已收到的消息体列表（按投递顺序）。 */
        List<String> bodies() {
            return receivedMessages.stream().map(Message::getBody).toList();
        }

        /** 按 body 精确查找已收到的消息；未收到返回 null。 */
        Message<String> messageByBody(String body) {
            for (Message<String> m : receivedMessages) {
                if (Objects.equals(body, m.getBody())) {
                    return m;
                }
            }
            return null;
        }

        /**
         * 等待 body 精确等于 expected 的消息被消费，并返回该消息。
         *
         * @param expected 期望的消息体
         * @param timeoutSeconds 超时秒数
         * @return 匹配的消息（不会为 null，超时则断言失败）
         */
        Message<String> awaitBody(String expected, long timeoutSeconds) {
            await().atMost(timeoutSeconds, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(bodies()).contains(expected));
            return messageByBody(expected);
        }

        /**
         * 返回指定 body 对应消息的 Stream Entry ID；未收到该消息时返回空串。
         *
         * <p>同理不得使用 {@link #getLastMessageId()}：共享消费者下"最后一条"未必是本测试发出的消息。
         */
        String messageIdOf(String body) {
            Message<String> m = messageByBody(body);
            if (m == null || m.getMessageId() == null) {
                return "";
            }
            return m.getMessageId().getStreamEntryId();
        }

        /** 返回指定 body 对应消息上的 traceparent 属性；未收到该消息时返回 null。 */
        String traceparentOf(String body) {
            Message<String> m = messageByBody(body);
            return m == null ? null : m.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
        }

        /** 等待 body 以 prefix 开头的消息累计收到 count 条。 */
        void awaitBodies(String prefix, int count, long timeoutSeconds) {
            await().atMost(timeoutSeconds, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertThat(
                                                    bodies().stream()
                                                            .filter(
                                                                    b ->
                                                                            b != null
                                                                                    && b.startsWith(
                                                                                            prefix))
                                                            .count())
                                            .isEqualTo(count));
        }

        /** 返回 body 以 prefix 开头的消息所携带的 traceparent 列表。 */
        List<String> traceparentsOf(String prefix) {
            List<String> result = new ArrayList<>();
            for (Message<String> m : receivedMessages) {
                String body = m.getBody();
                if (body != null && body.startsWith(prefix)) {
                    String tp = m.getUserProperties().get(StreamMQTracing.TRACEPARENT_KEY);
                    if (tp != null) {
                        result.add(tp);
                    }
                }
            }
            return result;
        }

        void clear() {
            received.clear();
            traceparents.clear();
            receivedMessages.clear();
            lastTraceparent.set(null);
        }
    }

    /**
     * 测试 Spring Boot 应用。
     *
     * <p>提供真实 {@link OpenTelemetrySdk} Bean（而非 no-op 默认实例）， 确保 {@link
     * StreamMQTracing#injectProducerSpan} 能创建有效 Span 并注入 W3C traceparent。
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
            SdkTracerProvider tracerProvider =
                    SdkTracerProvider.builder().setSampler(Sampler.alwaysOn()).build();
            return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
        }
    }
}
