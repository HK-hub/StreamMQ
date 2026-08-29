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
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.Severity;
import io.github.streammq.diagnostics.spi.BacklogProbe;
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
 * {@link BacklogAnalyzer} 单元测试。
 *
 * <p>独立 mock {@link StreamMQTraceService} 与可选的 {@link BacklogProbe}， 验证积压诊断报告的严重级别 / 真实探针 fallback
 * 行为。
 */
@DisplayName("BacklogAnalyzer 测试")
@ExtendWith(MockitoExtension.class)
class BacklogAnalyzerTest {

    @Mock private StreamMQTraceService traceService;

    private BacklogAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new BacklogAnalyzer(traceService, new StreamMQDiagnosticsProperties(), null);
    }

    @Test
    @DisplayName("小积压 -> INFO 级别 + 正常建议")
    void shouldReturnInfoSeverityForSmallBacklog() {
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
        }
        for (int i = 0; i < 8; i++) {
            records.add(consumeRecord("consume-" + i, "test-topic", "test-group", 2000L + i, 100L));
        }

        when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

        BacklogReport report = analyzer.diagnose("test-topic", "test-group");

        assertThat(report.currentBacklog()).isEqualTo(2);
        assertThat(report.severity()).isEqualTo(Severity.INFO);
        assertThat(report.recommendation()).contains("normal range");
    }

    @Test
    @DisplayName("中等积压 -> WARNING 级别")
    void shouldReturnWarningSeverity() {
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
        }
        when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);

        BacklogReport report = analyzer.diagnose("test-topic", "test-group");

        assertThat(report.currentBacklog()).isEqualTo(1001);
        assertThat(report.severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("无追踪数据 -> 空报告 INFO 级别")
    void shouldReturnEmptyReport() {
        when(traceService.queryByTopic(eq("empty-topic"), anyLong(), anyLong()))
                .thenReturn(List.of());

        BacklogReport report = analyzer.diagnose("empty-topic", "test-group");

        assertThat(report.currentBacklog()).isEqualTo(0);
        assertThat(report.severity()).isEqualTo(Severity.INFO);
        assertThat(report.code()).isEqualTo(DiagnosticsCodes.NO_TRACE_DATA);
    }

    @Test
    @DisplayName("BacklogProbe 优先于追踪窗口估算")
    void shouldPreferRealBacklogFromProbe() {
        BacklogProbe probe = (topic, group) -> new BacklogProbe.Result(0L, 50_000L);
        BacklogAnalyzer analyzerWithProbe =
                new BacklogAnalyzer(traceService, new StreamMQDiagnosticsProperties(), probe);

        when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong()))
                .thenReturn(List.of());

        BacklogReport report = analyzerWithProbe.diagnose("test-topic", "test-group");

        assertThat(report.currentBacklog()).isEqualTo(50_000L);
        assertThat(report.severity()).isEqualTo(Severity.CRITICAL);
    }

    private TraceRecord sendRecord(
            String messageId, String topic, long timestamp, long durationMillis) {
        return new TraceRecord(
                messageId,
                topic,
                "producer-group",
                TraceType.SEND,
                true,
                timestamp,
                durationMillis,
                "trace-" + messageId,
                Map.of());
    }

    private TraceRecord consumeRecord(
            String messageId, String topic, String group, long timestamp, long durationMillis) {
        return new TraceRecord(
                messageId,
                topic,
                group,
                TraceType.CONSUME,
                true,
                timestamp,
                durationMillis,
                "trace-" + messageId,
                Map.of());
    }
}
