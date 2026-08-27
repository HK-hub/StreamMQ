/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.diagnostics.model.DlqReport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * {@link DlqAnalyzer} 单元测试。
 *
 * <p>独立 mock {@link StreamMQTraceService}，验证死信消息识别、Top 失败原因聚合、DLQ 主题 marker 判定 等。
 */
@DisplayName("DlqAnalyzer 测试")
@ExtendWith(MockitoExtension.class)
class DlqAnalyzerTest {

    @Mock private StreamMQTraceService traceService;

    private DlqAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new DlqAnalyzer(traceService, new StreamMQDiagnosticsProperties());
    }

    @Test
    @DisplayName("失败消费达到重试阈值 -> 识别为死信 + 聚合 Top 失败原因")
    void shouldAggregateFailureReasons() {
        List<TraceRecord> records = new ArrayList<>();
        // fail-a 失败 3 次（达到 DLQ 阈值）
        for (int i = 0; i < 3; i++) {
            records.add(consumeRecord("fail-a", "test-topic", "test-group", 1000L + i, 100L, "NullPointerException"));
        }
        // fail-b 失败 3 次
        for (int i = 0; i < 3; i++) {
            records.add(consumeRecord("fail-b", "test-topic", "test-group", 2000L + i, 200L, "TimeoutException"));
        }

        when(traceService.queryByGroup(eq("test-group"), anyLong(), anyLong())).thenReturn(records);

        DlqReport report = analyzer.diagnose("test-group");

        assertThat(report.totalDlqCount()).isEqualTo(2);
        assertThat(report.topFailureReasons()).isNotEmpty();
        assertThat(report.topFailedTopics()).isNotEmpty();
    }

    @Test
    @DisplayName("主题含 DLQ 标识 -> 识别为死信")
    void shouldIdentifyDlqByTopic() {
        List<TraceRecord> records = new ArrayList<>();
        records.add(consumeRecord("dlq-1", "test-topic-dlq", "test-group", 1000L, 50L, "error"));
        records.add(consumeRecord("dlq-2", "test-topic-dlq", "test-group", 2000L, 50L, "error"));

        when(traceService.queryByGroup(eq("test-group"), anyLong(), anyLong())).thenReturn(records);

        DlqReport report = analyzer.diagnose("test-group");

        assertThat(report.totalDlqCount()).isEqualTo(2);
        assertThat(report.oldestDlqMessageTimestamp()).isEqualTo(1000L);
    }

    @Test
    @DisplayName("无追踪数据 -> 空报告 NO_TRACE_DATA")
    void shouldReturnEmptyReportWhenNoData() {
        when(traceService.queryByGroup(eq("empty-group"), anyLong(), anyLong()))
                .thenReturn(List.of());

        DlqReport report = analyzer.diagnose("empty-group");

        assertThat(report.totalDlqCount()).isEqualTo(0);
        assertThat(report.topFailureReasons()).isEmpty();
        assertThat(report.topFailedTopics()).isEmpty();
        assertThat(report.code()).isEqualTo(DiagnosticsCodes.NO_TRACE_DATA);
    }

    private TraceRecord consumeRecord(
            String messageId,
            String topic,
            String group,
            long timestamp,
            long durationMillis,
            String errorMessage) {
        return new TraceRecord(
                messageId,
                topic,
                group,
                TraceType.CONSUME,
                false,
                timestamp,
                durationMillis,
                "trace-" + messageId,
                Map.of("errorMessage", errorMessage));
    }
}
