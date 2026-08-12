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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

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
class DiagnosticsSampleIT {

    private static final String TEST_CONSUMER_GROUP = "diagnostics-test-consumer";

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
        SendResult result = orderProducer.createOrder("ORD-001", "order-content");
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
        orderProducer.createOrder("ORD-002", "order-content");

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(testCollector.receivedMessages).hasSize(1));

        BacklogReport report =
                diagnosticsService.diagnoseBacklog("order-events", TEST_CONSUMER_GROUP);
        assertThat(report).isNotNull();
        assertThat(report.topic()).isEqualTo("order-events");
    }

    /** 测试专用消息收集器，使用独立消费者组避免干扰。 */
    @StreamMQConsumer(topic = "order-events", consumerGroup = TEST_CONSUMER_GROUP)
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
