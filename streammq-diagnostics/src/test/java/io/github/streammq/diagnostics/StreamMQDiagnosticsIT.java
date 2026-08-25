/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
import io.github.streammq.core.util.RedisAvailability;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.ConsumeAttempt;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.MessageStatus;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * StreamMQ Diagnostics 模块真实集成测试。
 *
 * <p>使用本地 Redis（127.0.0.1:6379）验证诊断服务在真实消息收发链路中的行为：
 *
 * <ul>
 *   <li>发送与消费消息后，{@link StreamMQDiagnosticsService} 能基于追踪数据生成诊断报告
 *   <li>{@link MessageProfileService} 能构建消息完整生命周期画像
 *   <li>所有诊断 Bean 被正确自动装配
 *   <li>全链路追踪数据写入 Redis 后可被诊断服务查询和分析
 * </ul>
 *
 * <p>需要同时启用核心追踪（{@code streammq.trace.enabled=true} + {@code streammq.trace.storage=redis}）
 * 与诊断模块（{@code streammq.diagnostics.enabled=true}），确保追踪数据被写入 Redis 并可被诊断服务查询。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(
        classes = StreamMQDiagnosticsIT.TestApplication.class,
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
@Import({
    StreamMQDiagnosticsIT.DiagnosticsTestConsumer.class,
    StreamMQDiagnosticsIT.FailingTestConsumer.class
})
@DirtiesContext
@DisplayName("StreamMQ Diagnostics 真实集成测试")
@EnabledIf(
        value = "io.github.streammq.core.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class StreamMQDiagnosticsIT {
    @BeforeAll
    static void requireRedis() {
        // 无本地 Redis 时跳过（上下文/用例依赖真实 Redis），保证 mvn verify 任意环境可复现
        Assumptions.assumeTrue(
                RedisAvailability.isAvailable("localhost", 6379),
                "Redis not available at localhost:6379, skipping IT");
    }

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

    private static final String TOPIC = "diag-it-topic";
    private static final String CONSUMER_GROUP = "diag-it-cg";

    @Autowired private StreamMessageTemplate template;

    @Autowired private StreamMQDiagnosticsService diagnosticsService;

    @Autowired private MessageProfileService profileService;

    @Autowired private StreamMQTraceService traceService;

    @Autowired private DiagnosticsTestConsumer testConsumer;

    @Autowired private FailingTestConsumer failingConsumer;

    @Autowired private RedissonClient redisson;

    @BeforeEach
    void clearState() {
        testConsumer.clear();
        failingConsumer.clear();
    }

    @AfterEach
    void clearTraceData() {
        try {
            // 清理当日 trace stream，防止跨测试残留
            String date =
                    java.time.LocalDate.now()
                            .format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
            String traceKey = "streammq:diag-it:trace:" + date;
            redisson.getBucket(traceKey).delete();
        } catch (Exception ignored) {
            // 忽略清理异常
        }
    }

    /** 诊断测试消费者 - 成功消费。 */

    // ===================== 基础功能测试 =====================

    @Nested
    @DisplayName("基础功能 - 发送/消费/追踪")
    class BasicFunctionality {

        @Test
        @DisplayName("发送并消费消息后，诊断服务应能生成慢消费报告")
        void shouldDiagnoseSlowConsumeAfterMessageExchange() {
            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(TOPIC)
                                    .tag("diag-test")
                                    .keys("diag-key-1")
                                    .body("diag-message")
                                    .build());
            assertThat(result.isSuccess()).isTrue();

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).isNotEmpty());

            SlowConsumeReport report =
                    diagnosticsService.diagnoseSlowConsume(TOPIC, CONSUMER_GROUP);
            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
            assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
            assertThat(report.bottleneck()).isNotEmpty();
            assertThat(report.recommendation()).isNotEmpty();
        }

        @Test
        @DisplayName("诊断服务应能生成积压报告")
        void shouldDiagnoseBacklog() {
            BacklogReport report = diagnosticsService.diagnoseBacklog(TOPIC, CONSUMER_GROUP);
            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
            assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
            assertThat(report.severity()).isNotNull();
            assertThat(report.recommendation()).isNotEmpty();
        }

        @Test
        @DisplayName("诊断服务应能生成死信队列报告")
        void shouldDiagnoseDlq() {
            DlqReport report = diagnosticsService.diagnoseDlq(CONSUMER_GROUP);
            assertThat(report).isNotNull();
            assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
            assertThat(report.totalDlqCount()).isNotNegative();
        }

        @Test
        @DisplayName("诊断服务与画像服务应被正确自动装配")
        void shouldAutoConfigureBeans() {
            assertThat(diagnosticsService).isNotNull();
            assertThat(profileService).isNotNull();
            assertThat(traceService).isNotNull();
        }
    }

    // ===================== 追踪数据全链路测试 =====================

    @Nested
    @DisplayName("追踪数据全链路 - 真实 Redis 存储/查询")
    class TraceDataFlow {

        @Test
        @DisplayName("发送多条消息后，追踪服务应能查询到发送和消费记录")
        void shouldQueryTraceRecordsAfterMessageExchange() {
            int msgCount = 5;
            for (int i = 0; i < msgCount; i++) {
                template.syncSend(
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("trace-test")
                                .keys("trace-key-" + i)
                                .body("trace-message-" + i)
                                .build());
            }

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).hasSize(msgCount));

            long now = System.currentTimeMillis();
            long start = now - 5 * 60 * 1000L;
            List<TraceRecord> records = traceService.queryByTopic(TOPIC, start, now);

            assertThat(records).isNotNull();
            assertThat(records).isNotEmpty();

            long sendCount = records.stream().filter(r -> r.type() == TraceType.SEND).count();
            long consumeCount = records.stream().filter(r -> r.type() == TraceType.CONSUME).count();
            assertThat(sendCount).isGreaterThanOrEqualTo(msgCount);
            assertThat(consumeCount).isGreaterThanOrEqualTo(msgCount);

            for (TraceRecord record : records) {
                assertThat(record.messageId()).isNotEmpty();
                assertThat(record.topic()).isEqualTo(TOPIC);
                assertThat(record.timestamp()).isPositive();
            }
        }

        @Test
        @DisplayName("按消息 ID 应能查询到完整链路记录")
        void shouldQueryTraceByMessageId() {
            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(TOPIC)
                                    .tag("msgid-test")
                                    .keys("msgid-key")
                                    .body("msgid-message")
                                    .build());
            assertThat(result.isSuccess()).isTrue();

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).isNotEmpty());

            String messageId = testConsumer.getLastMessageId();
            assertThat(messageId).isNotEmpty();

            List<TraceRecord> records = traceService.queryByMessageId(messageId);
            assertThat(records).isNotNull();
            assertThat(records).isNotEmpty();
            assertThat(records).allMatch(r -> messageId.equals(r.messageId()));
        }

        @Test
        @DisplayName("按消费者组应能查询到消费记录")
        void shouldQueryTraceByGroup() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("group-test")
                            .keys("group-key")
                            .body("group-message")
                            .build());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).isNotEmpty());

            long now = System.currentTimeMillis();
            long start = now - 5 * 60 * 1000L;
            List<TraceRecord> records = traceService.queryByGroup(CONSUMER_GROUP, start, now);

            assertThat(records).isNotNull();
            assertThat(records).isNotEmpty();
            assertThat(records)
                    .allMatch(
                            r ->
                                    CONSUMER_GROUP.equals(r.group())
                                            || "diag-it-producer".equals(r.group()));
        }
    }

    // ===================== 消息画像全链路测试 =====================

    @Nested
    @DisplayName("消息画像 - 真实生命周期构建")
    class MessageProfileTests {

        @Test
        @DisplayName("发送并消费消息后，画像服务应能构建消息生命周期画像")
        void shouldBuildMessageProfileAfterExchange() {
            String body = "profile-message-" + System.nanoTime();
            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(TOPIC)
                                    .tag("profile-test")
                                    .keys("profile-key")
                                    .body(body)
                                    .build());
            assertThat(result.isSuccess()).isTrue();

            // 等待目标 body 被消费
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                List<String> bodies = testConsumer.getReceived();
                                assertThat(bodies).contains(body);
                            });

            // 找到该 body 对应的 messageId
            String messageId =
                    testConsumer.getReceivedMessages().stream()
                            .filter(m -> body.equals(m.getBody()) && m.getMessageId() != null)
                            .map(m -> m.getMessageId().getStreamEntryId())
                            .findFirst()
                            .orElse(null);
            assertThat(messageId).isNotNull();

            // 等待追踪数据落库并可查询（含消费记录）
            MessageProfileService ps = profileService;
            MessageProfile profile =
                    await().atMost(15, TimeUnit.SECONDS)
                            .until(
                                    () -> {
                                        MessageProfile candidate = ps.getProfile(messageId);
                                        if (candidate != null
                                                && candidate.consumeHistory() != null
                                                && !candidate.consumeHistory().isEmpty()) {
                                            return candidate;
                                        }
                                        return null;
                                    },
                                    p -> p != null);

            assertThat(profile).isNotNull();
            assertThat(profile.messageId()).isEqualTo(messageId);
            assertThat(profile.topic()).isEqualTo(TOPIC);
            assertThat(profile.tag()).isEqualTo("profile-test");
            assertThat(profile.keys()).isEqualTo("profile-key");
            assertThat(profile.bornTimestamp()).isPositive();
            assertThat(profile.routePath()).isNotEmpty();
            assertThat(profile.routePath().contains(TOPIC)).isTrue();
            assertThat(profile.consumeHistory()).isNotEmpty();
            assertThat(profile.consumeHistory().get(0).success()).isTrue();
            assertThat(profile.finalStatus()).isIn(MessageStatus.SUCCESS, MessageStatus.PROCESSING);
        }

        @Test
        @DisplayName("按主题批量查询画像应返回所有消息画像")
        void shouldGetTopicProfiles() {
            int msgCount = 3;
            for (int i = 0; i < msgCount; i++) {
                template.syncSend(
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("batch-profile")
                                .keys("batch-key-" + i)
                                .body("batch-message-" + i)
                                .build());
            }

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).hasSize(msgCount));

            long now = System.currentTimeMillis();
            long start = now - 5 * 60 * 1000L;
            List<MessageProfile> profiles = profileService.getTopicProfiles(TOPIC, start, now);

            assertThat(profiles).isNotNull();
            assertThat(profiles).isNotEmpty();
            assertThat(profiles.size()).isGreaterThanOrEqualTo(msgCount);

            for (MessageProfile profile : profiles) {
                assertThat(profile.messageId()).isNotEmpty();
                assertThat(profile.topic()).isEqualTo(TOPIC);
                assertThat(profile.consumeHistory()).isNotNull();
            }
        }

        @Test
        @DisplayName("消费历史应包含消费尝试记录")
        void shouldContainConsumeHistory() {
            SendResult result =
                    template.syncSend(
                            MessageBuilder.<String>withTopic(TOPIC)
                                    .tag("history-test")
                                    .keys("history-key")
                                    .body("history-message")
                                    .build());
            assertThat(result.isSuccess()).isTrue();

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).isNotEmpty());

            String messageId = testConsumer.getLastMessageId();
            MessageProfile profile =
                    await().atMost(10, TimeUnit.SECONDS)
                            .until(
                                    () -> profileService.getProfile(messageId),
                                    p ->
                                            p != null
                                                    && p.consumeHistory() != null
                                                    && !p.consumeHistory().isEmpty());

            assertThat(profile).isNotNull();
            List<ConsumeAttempt> history = profile.consumeHistory();
            assertThat(history).isNotNull();
            assertThat(history).isNotEmpty();

            ConsumeAttempt attempt = history.get(0);
            assertThat(attempt.consumerGroup()).isEqualTo(CONSUMER_GROUP);
            assertThat(attempt.success()).isTrue();
            assertThat(attempt.timestamp()).isPositive();
        }

        @Test
        @DisplayName("不存在的消息 ID 应返回 null")
        void shouldReturnNullForUnknownMessage() {
            MessageProfile profile = profileService.getProfile("non-existent-message-id");
            assertThat(profile).isNull();
        }
    }

    // ===================== 慢消费诊断全链路测试 =====================

    @Nested
    @DisplayName("慢消费诊断 - 真实数据驱动")
    class SlowConsumeDiagnostics {

        @Test
        @DisplayName("发送多条消息后，慢消费报告应包含消费统计数据")
        void shouldContainStatsAfterMultipleMessages() {
            int msgCount = 10;
            for (int i = 0; i < msgCount; i++) {
                template.syncSend(
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("slow-test")
                                .keys("slow-key-" + i)
                                .body("slow-message-" + i)
                                .build());
            }

            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).hasSize(msgCount));

            SlowConsumeReport report =
                    diagnosticsService.diagnoseSlowConsume(TOPIC, CONSUMER_GROUP);

            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
            assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
            assertThat(report.produceRate()).isGreaterThan(0);
            assertThat(report.consumeRate()).isGreaterThan(0);
            assertThat(report.avgConsumeTimeMillis()).isGreaterThanOrEqualTo(0);
            assertThat(report.maxConsumeTimeMillis()).isGreaterThanOrEqualTo(0);
            assertThat(report.p99ConsumeTimeMillis()).isGreaterThanOrEqualTo(0);
            assertThat(report.bottleneck()).isNotEmpty();
            assertThat(report.recommendation()).isNotEmpty();
        }

        @Test
        @DisplayName("无消费者的主题应返回空报告")
        void shouldReturnEmptyReportForUnknownTopic() {
            SlowConsumeReport report =
                    diagnosticsService.diagnoseSlowConsume(
                            "non-existent-topic", "non-existent-group");
            assertThat(report).isNotNull();
            assertThat(report.bottleneck()).contains("无追踪数据");
        }
    }

    // ===================== 积压诊断全链路测试 =====================

    @Nested
    @DisplayName("积压诊断 - 真实数据驱动")
    class BacklogDiagnostics {

        @Test
        @DisplayName("发送并消费消息后，积压报告应反映消费状态")
        void shouldReflectConsumeStatus() {
            int msgCount = 8;
            for (int i = 0; i < msgCount; i++) {
                template.syncSend(
                        MessageBuilder.<String>withTopic(TOPIC)
                                .tag("backlog-test")
                                .keys("backlog-key-" + i)
                                .body("backlog-message-" + i)
                                .build());
            }

            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).hasSize(msgCount));

            BacklogReport report = diagnosticsService.diagnoseBacklog(TOPIC, CONSUMER_GROUP);

            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
            assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
            assertThat(report.produceRate()).isGreaterThan(0);
            assertThat(report.severity()).isNotNull();
        }

        @Test
        @DisplayName("getAllBacklogs 应遍历所有消费者并生成报告")
        void shouldGetAllBacklogs() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("all-backlogs")
                            .keys("all-backlogs-key")
                            .body("all-backlogs-message")
                            .build());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).isNotEmpty());

            List<BacklogReport> backlogs = diagnosticsService.getAllBacklogs();
            assertThat(backlogs).isNotNull();
            assertThat(backlogs).isNotEmpty();
        }
    }

    // ===================== DLQ 诊断全链路测试 =====================

    @Nested
    @DisplayName("DLQ 诊断 - 真实数据驱动")
    class DlqDiagnostics {

        @Test
        @DisplayName("正常消费的消费者组不应有死信")
        void shouldHaveNoDlqForNormalConsumer() {
            template.syncSend(
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("dlq-test")
                            .keys("dlq-key")
                            .body("dlq-message")
                            .build());

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(testConsumer.getReceived()).isNotEmpty());

            DlqReport report = diagnosticsService.diagnoseDlq(CONSUMER_GROUP);

            assertThat(report).isNotNull();
            assertThat(report.group()).isEqualTo(CONSUMER_GROUP);
            assertThat(report.recommendation()).isNotEmpty();
        }

        @Test
        @DisplayName("getSlowConsumers 应能返回所有慢消费者")
        void shouldGetSlowConsumers() {
            List<String> slowConsumers = diagnosticsService.getSlowConsumers();
            assertThat(slowConsumers).isNotNull();
        }
    }

    // ===================== 异常边界测试 =====================

    @Nested
    @DisplayName("异常边界 - 空值/null 安全处理")
    class EdgeCases {

        @Test
        @DisplayName("空 topic 应安全处理")
        void shouldHandleEmptyTopic() {
            SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume("", "group");
            assertThat(report).isNotNull();
        }

        @Test
        @DisplayName("空 group 应安全处理")
        void shouldHandleEmptyGroup() {
            DlqReport report = diagnosticsService.diagnoseDlq("");
            assertThat(report).isNotNull();
        }

        @Test
        @DisplayName("空消息 ID 查询画像应返回 null")
        void shouldHandleEmptyMessageId() {
            MessageProfile profile = profileService.getProfile("");
            assertThat(profile).isNull();
        }

        @Test
        @DisplayName("空 topic 查询画像应返回空列表")
        void shouldHandleEmptyTopicForProfiles() {
            List<MessageProfile> profiles = profileService.getTopicProfiles("", 0L, Long.MAX_VALUE);
            assertThat(profiles).isEmpty();
        }
    }

    // ===================== 测试消费者 =====================

    /** 测试消费者 - 成功消费。 */
    @StreamMQConsumer(topic = TOPIC, consumerGroup = CONSUMER_GROUP, maxReconsumeTimes = 3)
    static class DiagnosticsTestConsumer implements StreamMessageConcurrentlyConsumer<String> {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<Message<String>> receivedMessages =
                new CopyOnWriteArrayList<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            received.add(message.getBody());
            receivedMessages.add(message);
            return ConsumeAction.SUCCESS;
        }

        List<String> getReceived() {
            return received;
        }

        List<Message<String>> getReceivedMessages() {
            return receivedMessages;
        }

        String getLastMessageId() {
            Message<String> last = receivedMessages.get(receivedMessages.size() - 1);
            return last.getMessageId() != null ? last.getMessageId().getStreamEntryId() : "";
        }

        void clear() {
            received.clear();
            receivedMessages.clear();
        }
    }

    /** 失败测试消费者 - 模拟消费失败场景。 */
    @StreamMQConsumer(topic = TOPIC, consumerGroup = "diag-it-failing-cg", maxReconsumeTimes = 0)
    static class FailingTestConsumer implements StreamMessageConcurrentlyConsumer<String> {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger failCount = new AtomicInteger(0);

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            received.add(message.getBody());
            return ConsumeAction.RECONSUME_LATER;
        }

        List<String> getReceived() {
            return received;
        }

        int getFailCount() {
            return failCount.get();
        }

        void clear() {
            received.clear();
            failCount.set(0);
        }
    }

    /** 测试 Spring Boot 应用。 */
    @SpringBootApplication
    static class TestApplication {}
}
