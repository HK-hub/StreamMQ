package io.github.streammq.diagnostics;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.Severity;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * {@link StreamMQDiagnosticsService} 单元测试，验证慢消费、积压、DLQ 诊断逻辑。
 *
 * <p>使用 Mockito 模拟 {@link StreamMQTraceService} 与 {@link StreamMQListenerContainer}，
 * 验证各种追踪数据场景下的诊断报告生成行为。
 */
@DisplayName("诊断服务测试")
@ExtendWith(MockitoExtension.class)
class StreamMQDiagnosticsServiceTest {

    @Mock
    private StreamMQTraceService traceService;

    @Mock
    private StreamMQListenerContainer listenerContainer;

    private StreamMQDiagnosticsService diagnosticsService;

    @BeforeEach
    void setUp() {
        diagnosticsService = new StreamMQDiagnosticsService(traceService, listenerContainer);
    }

    @Nested
    @DisplayName("diagnoseSlowConsume - 慢消费诊断")
    class DiagnoseSlowConsume {

        @Test
        @DisplayName("正常消费数据 -> 返回完整报告")
        void shouldReturnReportWithStats() {
            List<TraceRecord> records = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
            }
            for (int i = 0; i < 8; i++) {
                records.add(consumeRecord("consume-" + i, "test-topic", "test-group", true, 2000L + i, 100L));
            }

            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume("test-topic", "test-group");

            assertThat(report).isNotNull();
            assertThat(report.topic()).isEqualTo("test-topic");
            assertThat(report.group()).isEqualTo("test-group");
            assertThat(report.consumeRate()).isGreaterThan(0);
            assertThat(report.produceRate()).isGreaterThan(0);
            assertThat(report.avgConsumeTimeMillis()).isEqualTo(100.0);
            assertThat(report.maxConsumeTimeMillis()).isEqualTo(100L);
        }

        @Test
        @DisplayName("无追踪数据 -> 返回空报告")
        void shouldReturnEmptyReportWhenNoData() {
            when(traceService.queryByTopic(eq("empty-topic"), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume("empty-topic", "test-group");

            assertThat(report).isNotNull();
            assertThat(report.consumeRate()).isEqualTo(0.0);
            assertThat(report.produceRate()).isEqualTo(0.0);
            assertThat(report.bottleneck()).contains("无追踪数据");
            assertThat(report.recommendation()).contains("追踪服务");
        }

        @Test
        @DisplayName("消费耗时过长 -> 建议优化消费逻辑")
        void shouldRecommendOptimizationWhenSlowConsume() {
            List<TraceRecord> records = new ArrayList<>();
            records.add(sendRecord("send-1", "test-topic", 1000L, 5L));
            records.add(consumeRecord("consume-1", "test-topic", "test-group", true, 2000L, 8000L));

            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            SlowConsumeReport report = diagnosticsService.diagnoseSlowConsume("test-topic", "test-group");

            assertThat(report.avgConsumeTimeMillis()).isEqualTo(8000.0);
            assertThat(report.bottleneck()).contains("消费耗时过长");
            assertThat(report.recommendation()).contains("优化消费逻辑");
        }
    }

    @Nested
    @DisplayName("diagnoseBacklog - 积压诊断")
    class DiagnoseBacklog {

        @Test
        @DisplayName("小积压 -> INFO 级别")
        void shouldReturnInfoSeverityForSmallBacklog() {
            List<TraceRecord> records = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
            }
            for (int i = 0; i < 8; i++) {
                records.add(consumeRecord("consume-" + i, "test-topic", "test-group", true, 2000L + i, 100L));
            }

            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

            BacklogReport report = diagnosticsService.diagnoseBacklog("test-topic", "test-group");

            assertThat(report).isNotNull();
            assertThat(report.currentBacklog()).isEqualTo(2);
            assertThat(report.severity()).isEqualTo(Severity.INFO);
            assertThat(report.recommendation()).contains("正常范围");
        }

        @Test
        @DisplayName("中等积压 -> WARNING 级别")
        void shouldReturnWarningSeverityForMediumBacklog() {
            List<TraceRecord> records = new ArrayList<>();
            for (int i = 0; i < 1001; i++) {
                records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
            }

            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

            BacklogReport report = diagnosticsService.diagnoseBacklog("test-topic", "test-group");

            assertThat(report.currentBacklog()).isEqualTo(1001);
            assertThat(report.severity()).isEqualTo(Severity.WARNING);
            assertThat(report.recommendation()).contains("增加消费者实例数");
        }

        @Test
        @DisplayName("严重积压 -> CRITICAL 级别")
        void shouldReturnCriticalSeverityForLargeBacklog() {
            List<TraceRecord> records = new ArrayList<>();
            for (int i = 0; i < 10001; i++) {
                records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
            }

            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

            BacklogReport report = diagnosticsService.diagnoseBacklog("test-topic", "test-group");

            assertThat(report.currentBacklog()).isEqualTo(10001);
            assertThat(report.severity()).isEqualTo(Severity.CRITICAL);
            assertThat(report.recommendation()).contains("立即扩容");
        }

        @Test
        @DisplayName("无追踪数据 -> 返回空报告")
        void shouldReturnEmptyReportWhenNoData() {
            when(traceService.queryByTopic(eq("empty-topic"), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());

            BacklogReport report = diagnosticsService.diagnoseBacklog("empty-topic", "test-group");

            assertThat(report).isNotNull();
            assertThat(report.currentBacklog()).isEqualTo(0);
            assertThat(report.severity()).isEqualTo(Severity.INFO);
            assertThat(report.recommendation()).contains("追踪服务");
        }
    }

    @Nested
    @DisplayName("diagnoseDlq - 死信队列诊断")
    class DiagnoseDlq {

        @Test
        @DisplayName("存在失败消费记录 -> 返回失败原因聚合报告")
        void shouldAggregateFailureReasons() {
            List<TraceRecord> records = new ArrayList<>();
            // 消息 fail-a 重试 3 次均失败（达到 DLQ 判定阈值），错误为 NullPointerException
            for (int i = 0; i < 3; i++) {
                records.add(consumeRecord("fail-a", "test-topic", "test-group", false,
                    1000L + i, 100L, Map.of("errorMessage", "NullPointerException")));
            }
            // 消息 fail-b 重试 3 次均失败，错误为 TimeoutException
            for (int i = 0; i < 3; i++) {
                records.add(consumeRecord("fail-b", "test-topic", "test-group", false,
                    2000L + i, 200L, Map.of("errorMessage", "TimeoutException")));
            }
            // 消息 fail-c 重试 2 次失败（未达到 DLQ 阈值，不应计入 DLQ）
            for (int i = 0; i < 2; i++) {
                records.add(consumeRecord("fail-c", "test-topic", "test-group", false,
                    3000L + i, 150L, Map.of("errorMessage", "NullPointerException")));
            }

            when(traceService.queryByGroup(eq("test-group"), anyLong(), anyLong())).thenReturn(records);

            DlqReport report = diagnosticsService.diagnoseDlq("test-group");

            assertThat(report).isNotNull();
            assertThat(report.group()).isEqualTo("test-group");
            assertThat(report.totalDlqCount()).isEqualTo(2);
            assertThat(report.topFailureReasons()).isNotEmpty();
            assertThat(report.topFailureReasons().get(0).reason()).isEqualTo("NullPointerException");
            assertThat(report.topFailedTopics()).isNotEmpty();
        }

        @Test
        @DisplayName("DLQ 主题记录 -> 正确识别死信消息")
        void shouldIdentifyDlqMessagesByTopic() {
            List<TraceRecord> records = new ArrayList<>();
            records.add(consumeRecord("dlq-1", "test-topic-dlq", "test-group", false, 1000L, 50L,
                Map.of("errorMessage", "DLQ error")));
            records.add(consumeRecord("dlq-2", "test-topic-dlq", "test-group", false, 2000L, 50L,
                Map.of("errorMessage", "DLQ error")));

            when(traceService.queryByGroup(eq("test-group"), anyLong(), anyLong())).thenReturn(records);

            DlqReport report = diagnosticsService.diagnoseDlq("test-group");

            assertThat(report.totalDlqCount()).isEqualTo(2);
            assertThat(report.oldestDlqMessageTimestamp()).isEqualTo(1000L);
        }

        @Test
        @DisplayName("无追踪数据 -> 返回空报告")
        void shouldReturnEmptyReportWhenNoData() {
            when(traceService.queryByGroup(eq("empty-group"), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());

            DlqReport report = diagnosticsService.diagnoseDlq("empty-group");

            assertThat(report).isNotNull();
            assertThat(report.totalDlqCount()).isEqualTo(0);
            assertThat(report.topFailureReasons()).isEmpty();
            assertThat(report.topFailedTopics()).isEmpty();
            assertThat(report.recommendation()).contains("追踪服务");
        }
    }

    @Nested
    @DisplayName("getSlowConsumers - 慢消费者识别")
    class GetSlowConsumers {

        @Test
        @DisplayName("存在慢消费者 -> 返回标识列表")
        void shouldReturnSlowConsumers() {
            StreamMQListenerContainer.ConsumerMetadata metadata =
                new StreamMQListenerContainer.ConsumerMetadata("test-topic", "test-group", Object.class, String.class);

            List<TraceRecord> records = new ArrayList<>();
            records.add(sendRecord("send-1", "test-topic", 1000L, 5L));
            records.add(consumeRecord("consume-1", "test-topic", "test-group", true, 2000L, 8000L));

            when(listenerContainer.getConsumers()).thenReturn(List.of(metadata));
            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

            List<String> slowConsumers = diagnosticsService.getSlowConsumers();

            assertThat(slowConsumers).hasSize(1);
            assertThat(slowConsumers.get(0)).isEqualTo("test-topic:test-group");
        }

        @Test
        @DisplayName("无消费者注册 -> 返回空列表")
        void shouldReturnEmptyWhenNoConsumers() {
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            List<String> slowConsumers = diagnosticsService.getSlowConsumers();

            assertThat(slowConsumers).isEmpty();
        }
    }

    @Nested
    @DisplayName("getAllBacklogs - 全量积压报告")
    class GetAllBacklogs {

        @Test
        @DisplayName("存在消费者 -> 返回积压报告列表")
        void shouldReturnBacklogReports() {
            StreamMQListenerContainer.ConsumerMetadata metadata =
                new StreamMQListenerContainer.ConsumerMetadata("test-topic", "test-group", Object.class, String.class);

            List<TraceRecord> records = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
            }
            for (int i = 0; i < 3; i++) {
                records.add(consumeRecord("consume-" + i, "test-topic", "test-group", true, 2000L + i, 100L));
            }

            when(listenerContainer.getConsumers()).thenReturn(List.of(metadata));
            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

            List<BacklogReport> backlogs = diagnosticsService.getAllBacklogs();

            assertThat(backlogs).hasSize(1);
            assertThat(backlogs.get(0).topic()).isEqualTo("test-topic");
            assertThat(backlogs.get(0).group()).isEqualTo("test-group");
            assertThat(backlogs.get(0).currentBacklog()).isEqualTo(2);
        }

        @Test
        @DisplayName("无消费者注册 -> 返回空列表")
        void shouldReturnEmptyWhenNoConsumers() {
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            List<BacklogReport> backlogs = diagnosticsService.getAllBacklogs();

            assertThat(backlogs).isEmpty();
        }
    }

    /**
     * 创建发送追踪记录。
     */
    private TraceRecord sendRecord(String messageId, String topic, long timestamp, long durationMillis) {
        return new TraceRecord(messageId, topic, "producer-group", TraceType.SEND,
            true, timestamp, durationMillis, "trace-" + messageId, Map.of());
    }

    /**
     * 创建消费追踪记录。
     */
    private TraceRecord consumeRecord(String messageId, String topic, String group, boolean success,
                                      long timestamp, long durationMillis, Map<String, String> attrs) {
        return new TraceRecord(messageId, topic, group, TraceType.CONSUME,
            success, timestamp, durationMillis, "trace-" + messageId, attrs);
    }

    /**
     * 创建消费追踪记录（无扩展属性）。
     */
    private TraceRecord consumeRecord(String messageId, String topic, String group, boolean success,
                                      long timestamp, long durationMillis) {
        return consumeRecord(messageId, topic, group, success, timestamp, durationMillis, Map.of());
    }
}
