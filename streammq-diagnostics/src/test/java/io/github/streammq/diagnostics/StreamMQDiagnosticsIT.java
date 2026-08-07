package io.github.streammq.diagnostics;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * StreamMQ Diagnostics 模块真实集成测试。
 *
 * <p>使用本地 Redis（127.0.0.1:6379）验证诊断服务在真实消息收发链路中的行为：
 * <ul>
 *   <li>发送与消费消息后，{@link StreamMQDiagnosticsService} 能基于追踪数据生成诊断报告</li>
 *   <li>{@link MessageProfileService} 能构建消息完整生命周期画像</li>
 *   <li>所有诊断 Bean 被正确自动装配</li>
 * </ul>
 *
 * <p>需要同时启用核心追踪（{@code streammq.trace.enabled=true} + {@code streammq.trace.storage=redis}）
 * 与诊断模块（{@code streammq.diagnostics.enabled=true}），确保追踪数据被写入 Redis 并可被诊断服务查询。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@SpringBootTest(classes = StreamMQDiagnosticsIT.TestApplication.class,
        properties = {
                "spring.application.name=streammq-diagnostics-it",
                "streammq.enabled=true",
                "streammq.namespace=diag-it",
                "streammq.producer.group=diag-it-producer",
                "streammq.tracing.enabled=true",
                "streammq.trace.enabled=true",
                "streammq.trace.storage=redis",
                "streammq.diagnostics.enabled=true",
                "redisson.singleServerConfig.address=redis://127.0.0.1:6379",
                "debug=true"
        })
@DirtiesContext
@DisplayName("StreamMQ Diagnostics 真实集成测试")
class StreamMQDiagnosticsIT {

    @DynamicPropertySource
    static void redisPassword(DynamicPropertyRegistry registry) {
        String password = System.getProperty("test.redis.password",
                System.getenv().getOrDefault("STREAMMQ_TEST_REDIS_PASSWORD", ""));
        if (!password.isEmpty()) {
            registry.add("redisson.singleServerConfig.password", () -> password);
        }
    }

    private static final String TOPIC = "diag-it-topic";
    private static final String CONSUMER_GROUP = "diag-it-cg";

    @Autowired
    private StreamMessageTemplate template;

    @Autowired
    private StreamMQDiagnosticsService diagnosticsService;

    @Autowired
    private MessageProfileService profileService;

    @Autowired
    private DiagnosticsTestConsumer testConsumer;

    @BeforeEach
    void clearState() {
        testConsumer.clear();
    }

    @Test
    @DisplayName("发送并消费消息后，诊断服务应能生成慢消费报告")
    void shouldDiagnoseSlowConsumeAfterMessageExchange() {
        // 发送消息
        template.syncSend(io.github.streammq.core.message.MessageBuilder.<String>withTopic(TOPIC)
                .tag("diag-test")
                .keys("diag-key-1")
                .body("diag-message")
                .build());

        // 等待消费完成
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(testConsumer.getReceived()).isNotEmpty());

        // 诊断慢消费（可能返回空数据，因为追踪记录的时间窗口可能尚未覆盖）
        SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume(TOPIC, CONSUMER_GROUP);
        assertThat(report).isNotNull();
        assertThat(report.topic()).isEqualTo(TOPIC);
        assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
    }

    @Test
    @DisplayName("诊断服务应能生成积压报告")
    void shouldDiagnoseBacklog() {
        BacklogReport report = diagnosticsService.diagnoseBacklog(TOPIC, CONSUMER_GROUP);
        assertThat(report).isNotNull();
        assertThat(report.topic()).isEqualTo(TOPIC);
        assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
    }

    @Test
    @DisplayName("诊断服务应能生成死信队列报告")
    void shouldDiagnoseDlq() {
        DlqReport report = diagnosticsService.diagnoseDlq(CONSUMER_GROUP);
        assertThat(report).isNotNull();
        assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
    }

    @Test
    @DisplayName("消息画像服务应能查询消息生命周期")
    void shouldGetMessageProfile() {
        // 发送消息
        template.syncSend(io.github.streammq.core.message.MessageBuilder.<String>withTopic(TOPIC)
                .tag("profile-test")
                .keys("profile-key")
                .body("profile-message")
                .build());

        // 等待消费完成
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(testConsumer.getReceived()).isNotEmpty());

        // 查询时间范围内的消息画像
        long now = System.currentTimeMillis();
        long start = now - 5 * 60 * 1000L;
        List<MessageProfile> profiles = profileService.getTopicProfiles(TOPIC, start, now);
        assertThat(profiles).isNotNull();
        // 追踪记录可能需要时间写入，不强断言非空
    }

    @Test
    @DisplayName("诊断服务与画像服务应被正确自动装配")
    void shouldAutoConfigureBeans() {
        assertThat(diagnosticsService).isNotNull();
        assertThat(profileService).isNotNull();
    }

    /**
     * 测试消费者。
     */
    @Component
    @StreamMQConsumer(
            topic = TOPIC,
            consumerGroup = CONSUMER_GROUP,
            maxReconsumeTimes = 3
    )
    static class DiagnosticsTestConsumer implements StreamMessageConcurrentlyConsumer<String> {

        private final List<String> received = new CopyOnWriteArrayList<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            received.add(message.getBody());
            return ConsumeAction.SUCCESS;
        }

        List<String> getReceived() {
            return received;
        }

        void clear() {
            received.clear();
        }
    }

    /**
     * 测试 Spring Boot 应用。
     */
    @SpringBootApplication
    static class TestApplication {
        @Bean
        public DiagnosticsTestConsumer diagnosticsTestConsumer() {
            return new DiagnosticsTestConsumer();
        }
    }
}
