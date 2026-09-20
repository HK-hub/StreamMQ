/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * 追踪自动装配的接线回归测试。
 *
 * <p>锁定两件事：查询上界属性 {@code streammq.tracing.otel.max-trace-query-size} 真的被应用（此前只有代码内默认值， 截断 WARN
 * 提示的"调大 maxTraceQuerySize"无任何属性入口）；用户自定义 {@link StreamMQTopologyService} Bean 优先于自动装配。
 */
@DisplayName("StreamMQ 追踪自动装配")
class StreamMQTracingAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(StreamMQTracingAutoConfiguration.class));

    @Test
    @DisplayName("max-trace-query-size 属性被应用到拓扑服务")
    void topologyService_appliesConfiguredMaxTraceQuerySize() {
        StreamMQTraceService traceService = mock(StreamMQTraceService.class);
        when(traceService.queryByTopic(eq("order-topic"), anyLong(), anyLong()))
                .thenReturn(records(5));

        runner.withPropertyValues(
                        "streammq.tracing.otel.enabled=true",
                        "streammq.tracing.otel.max-trace-query-size=2")
                .withBean(StreamMQTraceService.class, () -> traceService)
                .withBean(
                        StreamMQListenerContainer.class,
                        () -> mock(StreamMQListenerContainer.class))
                .run(
                        context -> {
                            assertThat(context).hasSingleBean(StreamMQTopologyService.class);
                            assertThat(
                                            context.getBean(StreamMQTopologyService.class)
                                                    .getTopicTraces("order-topic", 0L, 1000L))
                                    .hasSize(2);
                        });
    }

    @Test
    @DisplayName("用户自定义拓扑服务 Bean 优先于自动装配")
    void userDefinedTopologyService_wins() {
        StreamMQTopologyService custom =
                new StreamMQTopologyService(
                        mock(StreamMQTraceService.class), mock(StreamMQListenerContainer.class));

        runner.withPropertyValues("streammq.tracing.otel.enabled=true")
                .withBean(StreamMQTraceService.class, () -> mock(StreamMQTraceService.class))
                .withBean(
                        StreamMQListenerContainer.class,
                        () -> mock(StreamMQListenerContainer.class))
                .withBean(StreamMQTopologyService.class, () -> custom)
                .run(
                        context ->
                                assertThat(context.getBean(StreamMQTopologyService.class))
                                        .isSameAs(custom));
    }

    private static List<TraceRecord> records(int count) {
        List<TraceRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(
                    new TraceRecord(
                            "m" + i,
                            "order-topic",
                            "g1",
                            TraceType.SEND,
                            true,
                            1000L + i,
                            1L,
                            "t" + i,
                            Map.of()));
        }
        return records;
    }
}
