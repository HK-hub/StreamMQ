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

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * {@link SlowConsumeAnalyzer} 单元测试。
 *
 * <p>独立 mock {@link StreamMQTraceService} 与 {@link StreamMQListenerContainer}， 验证慢消费诊断报告的生成逻辑。
 */
@DisplayName("SlowConsumeAnalyzer 测试")
@ExtendWith(MockitoExtension.class)
class SlowConsumeAnalyzerTest {

    @Mock private StreamMQTraceService traceService;

    @Mock private StreamMQListenerContainer listenerContainer;

    private SlowConsumeAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer =
                new SlowConsumeAnalyzer(
                        traceService, listenerContainer, new StreamMQDiagnosticsProperties());
    }

    @Test
    @DisplayName("正常消费数据 -> 返回完整报告")
    void shouldReturnReportWithStats() {
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(sendRecord("send-" + i, "test-topic", 1000L + i, 5L));
        }
        for (int i = 0; i < 12; i++) {
            records.add(
                    consumeRecord(
                            "consume-" + i, "test-topic", "test-group", true, 2000L + i, 100L));
        }

        when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);
        when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

        SlowConsumeReport report = analyzer.diagnose("test-topic", "test-group");

        assertThat(report).isNotNull();
        assertThat(report.topic()).isEqualTo("test-topic");
        assertThat(report.group()).isEqualTo("test-group");
        assertThat(report.avgConsumeTimeMillis()).isEqualTo(100.0);
        assertThat(report.maxConsumeTimeMillis()).isEqualTo(100L);
        assertThat(report.code()).isEqualTo(DiagnosticsCodes.HEALTHY);
    }

    @Test
    @DisplayName("无追踪数据 -> 返回空报告")
    void shouldReturnEmptyReportWhenNoData() {
        when(traceService.queryByTopic(eq("empty-topic"), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());
        when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

        SlowConsumeReport report = analyzer.diagnose("empty-topic", "test-group");

        assertThat(report).isNotNull();
        assertThat(report.consumeRate()).isEqualTo(0.0);
        assertThat(report.produceRate()).isEqualTo(0.0);
        assertThat(report.code()).isEqualTo(DiagnosticsCodes.NO_TRACE_DATA);
    }

    @Test
    @DisplayName("消费耗时超过阈值 -> SLOW_CONSUME 码 + 优化建议")
    void shouldClassifySlowConsume() {
        List<TraceRecord> records = new ArrayList<>();
        records.add(sendRecord("send-1", "test-topic", 1000L, 5L));
        records.add(consumeRecord("consume-1", "test-topic", "test-group", true, 2000L, 8000L));

        when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong())).thenReturn(records);
        when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

        SlowConsumeReport report = analyzer.diagnose("test-topic", "test-group");

        assertThat(report.code()).isEqualTo(DiagnosticsCodes.SLOW_CONSUME);
        assertThat(report.recommendation()).contains("Optimize consume logic");
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
            String messageId,
            String topic,
            String group,
            boolean success,
            long timestamp,
            long durationMillis) {
        return new TraceRecord(
                messageId,
                topic,
                group,
                TraceType.CONSUME,
                success,
                timestamp,
                durationMillis,
                "trace-" + messageId,
                Map.of());
    }
}
