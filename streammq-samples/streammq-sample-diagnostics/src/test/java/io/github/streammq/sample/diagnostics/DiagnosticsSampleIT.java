/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.sample.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.diagnostics.StreamMQDiagnosticsService;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Diagnostics 示例集成测试。
 *
 * <p>验证发送订单消息后，诊断服务能够生成慢消费报告、积压报告和消息画像。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = DiagnosticsApplication.class)
@ActiveProfiles("it")
@Import(DiagnosticsSampleIT.TestMessageCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("Diagnostics 示例集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class DiagnosticsSampleIT {

    private static final String TEST_CONSUMER_GROUP = "diagnostics-test-consumer";

    /** 每次运行的唯一后缀：命名空间与载荷均带此后缀，避免跨运行残留数据污染断言 */
    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    /** 本次运行的专属命名空间（覆写 streammq.namespace），配合 {@link #cleanupNamespace()} 实现跨运行隔离 */
    private static final String IT_NAMESPACE = "diagnostics-it-" + RUN_ID;

    /** 覆写全局命名空间，避免与历史运行/其它示例共享 streammq:diagnostics-sample:* 键 */
    @DynamicPropertySource
    static void overrideNamespace(DynamicPropertyRegistry registry) {
        registry.add("streammq.namespace", () -> IT_NAMESPACE);
    }

    /**
     * 清理本次运行命名空间下的全部键。
     *
     * <p>使用独立客户端：{@code @DirtiesContext(AFTER_EACH_TEST_METHOD)} 在每个方法后关闭上下文， 注入的 RedissonClient 在
     * {@code @AfterAll} 阶段已被 shutdown，无法复用于清理。
     */
    @AfterAll
    static void cleanupNamespace() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setDatabase(0);
        config.setCodec(StringCodec.INSTANCE);
        RedissonClient cleanupClient = Redisson.create(config);
        try {
            cleanupClient.getKeys().deleteByPattern("streammq:" + IT_NAMESPACE + ":*");
        } finally {
            cleanupClient.shutdown();
        }
    }

    @Autowired private OrderProducer orderProducer;

    @Autowired private StreamMQDiagnosticsService diagnosticsService;

    @Autowired private TestMessageCollector testCollector;

    @BeforeEach
    void clear() {
        testCollector.receivedMessages.clear();
    }

    @Test
    @DisplayName("发送订单后应能生成慢消费报告")
    void shouldDiagnoseSlowConsume() {
        SendResult result =
                orderProducer.createOrder("ORD-001-" + RUN_ID, "order-content-" + RUN_ID);
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(testCollector.receivedMessages).hasSize(1));

        SlowConsumeReport report =
                diagnosticsService.diagnoseSlowConsume("order-events", TEST_CONSUMER_GROUP);
        assertThat(report).isNotNull();
        assertThat(report.topic()).isEqualTo("order-events");
    }

    @Test
    @DisplayName("发送订单后应能生成积压报告")
    void shouldDiagnoseBacklog() {
        orderProducer.createOrder("ORD-002-" + RUN_ID, "order-content-" + RUN_ID);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(testCollector.receivedMessages).hasSize(1));

        BacklogReport report =
                diagnosticsService.diagnoseBacklog("order-events", TEST_CONSUMER_GROUP);
        assertThat(report).isNotNull();
        assertThat(report.topic()).isEqualTo("order-events");
    }

    /** 测试专用消息收集器，使用独立消费者组避免干扰。 */
    @StreamMQConsumer(topic = SampleConstants.TOPIC, consumerGroup = TEST_CONSUMER_GROUP)
    static class TestMessageCollector implements StreamMessageConcurrentlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            receivedMessages.add(message);
            return ConsumeAction.SUCCESS;
        }
    }
}
