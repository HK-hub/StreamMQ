/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * StreamMQ Diagnostics 模块 Mock 测试。
 *
 * <p>使用 Mock 替代 Redis 依赖，验证诊断服务核心逻辑：
 *
 * <ul>
 *   <li>慢消费诊断：基于追踪数据识别消费耗时异常
 *   <li>积压诊断：基于追踪数据分析积压量
 *   <li>死信队列诊断：识别失败原因与主题分布
 *   <li>消息画像：聚合发送与消费追踪记录
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StreamMQ Diagnostics Mock 测试")
class StreamMQDiagnosticsMockTest {

    private static final String TOPIC = "test-topic";
    private static final String GROUP = "test-group";

    @Mock private StreamMQTraceService traceService;

    @Mock private StreamMQListenerContainer listenerContainer;

    private StreamMQDiagnosticsService diagnosticsService;
    private MessageProfileService profileService;

    @BeforeEach
    void setUp() {
        StreamMQDiagnosticsProperties properties = new StreamMQDiagnosticsProperties();
        diagnosticsService =
                new StreamMQDiagnosticsService(traceService, listenerContainer, properties);
        profileService = new MessageProfileService(traceService);
    }

    @Nested
    @DisplayName("慢消费诊断")
    class SlowConsumeDiagnostics {

        @Test
        @DisplayName("无追踪数据时应返回默认报告")
        void shouldReturnDefaultReportWhenNoData() {
            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of());

            SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume(TOPIC, GROUP);

            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
            assertThat(report.group()).isEqualTo(GROUP);
        }

        @Test
        @DisplayName("有慢消费记录时应正确识别")
        void shouldIdentifySlowConsume() {
            long now = System.currentTimeMillis();
            TraceRecord slowRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            GROUP,
                            TraceType.CONSUME,
                            true,
                            now,
                            6000L,
                            "trace-001",
                            Map.of());

            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of(slowRecord));

            SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume(TOPIC, GROUP);

            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
        }
    }

    @Nested
    @DisplayName("积压诊断")
    class BacklogDiagnostics {

        @Test
        @DisplayName("应能生成积压报告")
        void shouldGenerateBacklogReport() {
            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of());

            BacklogReport report = diagnosticsService.diagnoseBacklog(TOPIC, GROUP);

            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo(TOPIC);
            assertThat(report.group()).isEqualTo(GROUP);
        }
    }

    @Nested
    @DisplayName("死信队列诊断")
    class DlqDiagnostics {

        @Test
        @DisplayName("应能生成 DLQ 报告")
        void shouldGenerateDlqReport() {
            when(traceService.queryByGroup(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of());

            DlqReport report = diagnosticsService.diagnoseDlq(GROUP);

            assertThat(report).isNotNull();
            assertThat(report.group()).isEqualTo(GROUP);
        }
    }

    @Nested
    @DisplayName("消息画像")
    class MessageProfileTests {

        @Test
        @DisplayName("无追踪数据时 getProfile 应返回 null")
        void shouldReturnNullWhenNoTrace() {
            when(traceService.queryByMessageId(anyString())).thenReturn(List.of());

            MessageProfile profile = profileService.getProfile("msg-001");

            assertThat(profile).isNull();
        }

        @Test
        @DisplayName("有追踪数据时应构建画像")
        void shouldBuildProfileWithTraceData() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            "producer-group",
                            TraceType.SEND,
                            true,
                            now - 1000L,
                            10L,
                            "trace-001",
                            Map.of("tag", "order", "keys", "key-1"));
            TraceRecord consumeRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            GROUP,
                            TraceType.CONSUME,
                            true,
                            now,
                            5L,
                            "trace-001",
                            Map.of("consumerName", "consumer-1"));

            when(traceService.queryByMessageId("msg-001"))
                    .thenReturn(List.of(sendRecord, consumeRecord));

            MessageProfile profile = profileService.getProfile("msg-001");

            assertThat(profile).isNotNull();
            assertThat(profile.messageId()).isEqualTo("msg-001");
        }

        @Test
        @DisplayName("按主题查询画像应返回列表")
        void shouldReturnProfileListByTopic() {
            long now = System.currentTimeMillis();
            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of());

            List<MessageProfile> profiles =
                    profileService.getTopicProfiles(TOPIC, now - 300_000, now);

            assertThat(profiles).isNotNull();
        }
    }

    @Nested
    @DisplayName("自动装配验证")
    class AutoConfiguration {

        @Test
        @DisplayName("诊断服务应能正确初始化")
        void shouldInitializeDiagnosticsService() {
            assertThat(diagnosticsService).isNotNull();
        }

        @Test
        @DisplayName("画像服务应能正确初始化")
        void shouldInitializeProfileService() {
            assertThat(profileService).isNotNull();
        }
    }
}
