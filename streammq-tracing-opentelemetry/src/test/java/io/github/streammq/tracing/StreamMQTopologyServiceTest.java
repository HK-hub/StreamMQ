/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.listener.StreamMQListenerContainer.ConsumerMetadata;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.tracing.model.MessageTrace;
import io.github.streammq.tracing.model.TopologyGraph;
import io.github.streammq.tracing.model.TraceEventType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** {@link StreamMQTopologyService} 单元测试，使用 Mockito 模拟追踪查询服务与监听器容器。 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StreamMQTopologyService 拓扑与链路构建测试")
class StreamMQTopologyServiceTest {

    @Mock private StreamMQTraceService traceService;

    @Mock private StreamMQListenerContainer listenerContainer;

    @InjectMocks private StreamMQTopologyService topologyService;

    @Test
    @DisplayName("getMessageTrace 应构建完整消息链路")
    void getMessageTrace_shouldBuildCompleteTrace() {
        TraceRecord send =
                new TraceRecord(
                        "m1",
                        "order-topic",
                        "producer-group",
                        TraceType.SEND,
                        true,
                        1000L,
                        5L,
                        "trace-1",
                        Map.of());
        TraceRecord consume =
                new TraceRecord(
                        "m1",
                        "order-topic",
                        "order-group",
                        TraceType.CONSUME,
                        true,
                        1500L,
                        50L,
                        "trace-1",
                        Map.of());
        when(traceService.queryByMessageId("m1")).thenReturn(List.of(send, consume));

        MessageTrace trace = topologyService.getMessageTrace("m1");

        assertThat(trace.messageId()).isEqualTo("m1");
        assertThat(trace.topic()).isEqualTo("order-topic");
        assertThat(trace.events()).hasSize(2);
        assertThat(trace.events().get(0).type()).isEqualTo(TraceEventType.SEND);
        assertThat(trace.events().get(1).type()).isEqualTo(TraceEventType.CONSUME);
        assertThat(trace.totalDurationMillis()).isEqualTo(500L);
        assertThat(trace.routePath())
                .containsExactly(
                        "Producer",
                        "Topic:order-topic",
                        "Group:order-group",
                        "Consumer:order-group");
        assertThat(trace.finalStatus()).isEqualTo("SUCCESS");
    }

    @Test
    @DisplayName("getMessageTrace 失败链路 finalStatus 应为 FAILED")
    void getMessageTrace_failedTrace_shouldBeFailed() {
        TraceRecord send =
                new TraceRecord(
                        "m2",
                        "order-topic",
                        "producer-group",
                        TraceType.SEND,
                        true,
                        1000L,
                        5L,
                        "trace-2",
                        Map.of());
        TraceRecord consume =
                new TraceRecord(
                        "m2",
                        "order-topic",
                        "order-group",
                        TraceType.CONSUME,
                        false,
                        1500L,
                        50L,
                        "trace-2",
                        Map.of());
        when(traceService.queryByMessageId("m2")).thenReturn(List.of(send, consume));

        MessageTrace trace = topologyService.getMessageTrace("m2");

        assertThat(trace.finalStatus()).isEqualTo("FAILED");
        assertThat(trace.events()).hasSize(2);
    }

    @Test
    @DisplayName("getMessageTrace 无记录时应返回空事件链路且状态为 PROCESSING")
    void getMessageTrace_noRecords_shouldReturnEmpty() {
        when(traceService.queryByMessageId("missing")).thenReturn(List.of());

        MessageTrace trace = topologyService.getMessageTrace("missing");

        assertThat(trace.events()).isEmpty();
        assertThat(trace.finalStatus()).isEqualTo("PROCESSING");
    }

    @Test
    @DisplayName("getTopicTraces 应按消息 ID 聚合返回多条链路")
    void getTopicTraces_shouldGroupByMessageId() {
        TraceRecord r1 =
                new TraceRecord(
                        "m1", "order-topic", "g1", TraceType.SEND, true, 1000L, 1L, "t1", Map.of());
        TraceRecord r2 =
                new TraceRecord(
                        "m2", "order-topic", "g2", TraceType.SEND, true, 2000L, 1L, "t2", Map.of());
        when(traceService.queryByTopic(eq("order-topic"), anyLong(), anyLong()))
                .thenReturn(List.of(r1, r2));

        List<MessageTrace> traces = topologyService.getTopicTraces("order-topic", 0L, 5000L);

        assertThat(traces).hasSize(2);
        assertThat(traces)
                .extracting(MessageTrace::messageId)
                .containsExactlyInAnyOrder("m1", "m2");
    }

    @Test
    @DisplayName("getTopicTopology 应构建生产者、消费者与路由")
    void getTopicTopology_shouldBuildGraph() {
        TraceRecord send1 =
                new TraceRecord(
                        "m1",
                        "order-topic",
                        "producer-group",
                        TraceType.SEND,
                        true,
                        1000L,
                        5L,
                        "t1",
                        Map.of());
        TraceRecord send2 =
                new TraceRecord(
                        "m2",
                        "order-topic",
                        "producer-group",
                        TraceType.SEND,
                        true,
                        2000L,
                        5L,
                        "t2",
                        Map.of());
        when(traceService.queryByTopic(eq("order-topic"), anyLong(), anyLong()))
                .thenReturn(List.of(send1, send2));
        ConsumerMetadata metadata =
                new ConsumerMetadata("order-topic", "order-group", String.class, String.class);
        Collection<ConsumerMetadata> consumers = List.of(metadata);
        when(listenerContainer.getConsumers()).thenReturn(consumers);

        TopologyGraph graph = topologyService.getTopicTopology("order-topic");

        assertThat(graph.topic()).isEqualTo("order-topic");
        assertThat(graph.producers()).hasSize(1);
        assertThat(graph.producers().get(0).type()).isEqualTo("PRODUCER");
        assertThat(graph.producers().get(0).name()).isEqualTo("producer-group");
        assertThat(graph.consumers()).hasSize(1);
        assertThat(graph.consumers().get(0).type()).isEqualTo("CONSUMER");
        assertThat(graph.consumers().get(0).group()).isEqualTo("order-group");
        assertThat(graph.routes()).hasSize(1);
        assertThat(graph.routes().get(0).from()).isEqualTo("producer-group");
        assertThat(graph.routes().get(0).to()).isEqualTo("order-group");
        assertThat(graph.routes().get(0).rate()).isPositive();
        assertThat(graph.lastUpdated()).isPositive();
    }
}
