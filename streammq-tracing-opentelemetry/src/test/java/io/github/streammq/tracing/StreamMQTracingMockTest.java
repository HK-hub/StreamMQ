/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.tracing.model.MessageTrace;
import io.github.streammq.tracing.model.TopologyGraph;
import java.util.Collections;
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
 * StreamMQ Tracing 模块 Mock 测试。
 *
 * <p>使用 Mock 替代 Redis 依赖，验证拓扑服务与消息链路构建逻辑：
 *
 * <ul>
 *   <li>主题拓扑图构建：生产者/消费者节点识别、路由计算
 *   <li>消息链路构建：单条消息的完整生命周期追踪
 *   <li>时间范围查询：批量消息链路查询
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StreamMQ Tracing Mock 测试")
class StreamMQTracingMockTest {

    private static final String TOPIC = "test-topic";
    private static final String PRODUCER_GROUP = "producer-group";
    private static final String CONSUMER_GROUP = "consumer-group";

    @Mock private StreamMQTraceService traceService;

    @Mock private StreamMQListenerContainer listenerContainer;

    private StreamMQTopologyService topologyService;

    @BeforeEach
    void setUp() {
        topologyService = new StreamMQTopologyService(traceService, listenerContainer);
    }

    @Nested
    @DisplayName("主题拓扑图构建")
    class TopologyBuilding {

        @Test
        @DisplayName("无追踪数据时应返回默认拓扑")
        void shouldReturnDefaultTopologyWhenNoData() {
            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of());
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology).isNotNull();
            assertThat(topology.topic()).isEqualTo(TOPIC);
        }

        @Test
        @DisplayName("有生产者追踪记录时应识别生产者节点")
        void shouldIdentifyProducerNodes() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now,
                            10L,
                            "trace-001",
                            Map.of());

            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of(sendRecord));
            when(listenerContainer.getConsumers()).thenReturn(Collections.emptyList());

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology).isNotNull();
            assertThat(topology.producers()).isNotEmpty();
            assertThat(topology.producers().get(0).name()).isEqualTo(PRODUCER_GROUP);
        }

        @Test
        @DisplayName("有消费者注册时应构建消费者节点")
        void shouldBuildConsumerNodes() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now,
                            10L,
                            "trace-001",
                            Map.of());

            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of(sendRecord));
            when(listenerContainer.getConsumers())
                    .thenReturn(
                            List.of(
                                    new StreamMQListenerContainer.ConsumerMetadata(
                                            TOPIC, CONSUMER_GROUP, Object.class, String.class)));

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology).isNotNull();
            assertThat(topology.consumers()).isNotEmpty();
            assertThat(topology.consumers().get(0).name()).isEqualTo(CONSUMER_GROUP);
        }

        @Test
        @DisplayName("应构建生产者到消费者的路由")
        void shouldBuildRoutes() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now,
                            10L,
                            "trace-001",
                            Map.of());

            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of(sendRecord));
            when(listenerContainer.getConsumers())
                    .thenReturn(
                            List.of(
                                    new StreamMQListenerContainer.ConsumerMetadata(
                                            TOPIC, CONSUMER_GROUP, Object.class, String.class)));

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology).isNotNull();
            assertThat(topology.routes()).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("消息链路构建")
    class MessageTraceBuilding {

        @Test
        @DisplayName("无追踪数据时应返回空事件链路")
        void shouldReturnEmptyTraceWhenNoData() {
            when(traceService.queryByMessageId("msg-001")).thenReturn(List.of());

            MessageTrace trace = topologyService.getMessageTrace("msg-001");

            assertThat(trace).isNotNull();
            assertThat(trace.messageId()).isEqualTo("msg-001");
            assertThat(trace.events()).isEmpty();
        }

        @Test
        @DisplayName("有发送和消费记录时应构建完整链路")
        void shouldBuildCompleteTrace() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now - 1000L,
                            10L,
                            "trace-001",
                            Map.of());
            TraceRecord consumeRecord =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            CONSUMER_GROUP,
                            TraceType.CONSUME,
                            true,
                            now,
                            5L,
                            "trace-001",
                            Map.of());

            when(traceService.queryByMessageId("msg-001"))
                    .thenReturn(List.of(sendRecord, consumeRecord));

            MessageTrace trace = topologyService.getMessageTrace("msg-001");

            assertThat(trace).isNotNull();
            assertThat(trace.messageId()).isEqualTo("msg-001");
            assertThat(trace.events()).hasSize(2);
            assertThat(trace.finalStatus()).isEqualTo("SUCCESS");
        }

        @Test
        @DisplayName("消费失败时应标记为 FAILED 状态")
        void shouldMarkFailedStatus() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-002",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now - 1000L,
                            10L,
                            "trace-002",
                            Map.of());
            TraceRecord failRecord =
                    new TraceRecord(
                            "msg-002",
                            TOPIC,
                            CONSUMER_GROUP,
                            TraceType.CONSUME,
                            false,
                            now,
                            5L,
                            "trace-002",
                            Map.of("errorMessage", "消费失败"));

            when(traceService.queryByMessageId("msg-002"))
                    .thenReturn(List.of(sendRecord, failRecord));

            MessageTrace trace = topologyService.getMessageTrace("msg-002");

            assertThat(trace).isNotNull();
            assertThat(trace.finalStatus()).isEqualTo("FAILED");
        }

        @Test
        @DisplayName("DLQ 记录应标记为 DLQ 状态")
        void shouldMarkDlqStatus() {
            long now = System.currentTimeMillis();
            TraceRecord sendRecord =
                    new TraceRecord(
                            "msg-003",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now - 2000L,
                            10L,
                            "trace-003",
                            Map.of());
            TraceRecord dlqRecord =
                    new TraceRecord(
                            "msg-003",
                            TOPIC + "-dlq",
                            CONSUMER_GROUP,
                            TraceType.CONSUME,
                            false,
                            now,
                            5L,
                            "trace-003",
                            Map.of("dlqReason", "重试超限"));

            when(traceService.queryByMessageId("msg-003"))
                    .thenReturn(List.of(sendRecord, dlqRecord));

            MessageTrace trace = topologyService.getMessageTrace("msg-003");

            assertThat(trace).isNotNull();
            assertThat(trace.finalStatus()).isEqualTo("DLQ");
        }
    }

    @Nested
    @DisplayName("时间范围查询")
    class TimeRangeQuery {

        @Test
        @DisplayName("应按消息 ID 分组返回链路列表")
        void shouldGroupByMessageId() {
            long now = System.currentTimeMillis();
            TraceRecord record1 =
                    new TraceRecord(
                            "msg-001",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now - 2000L,
                            10L,
                            "trace-001",
                            Map.of());
            TraceRecord record2 =
                    new TraceRecord(
                            "msg-002",
                            TOPIC,
                            PRODUCER_GROUP,
                            TraceType.SEND,
                            true,
                            now - 1000L,
                            8L,
                            "trace-002",
                            Map.of());

            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of(record1, record2));

            List<MessageTrace> traces = topologyService.getTopicTraces(TOPIC, now - 3000L, now);

            assertThat(traces).isNotNull();
            assertThat(traces).hasSize(2);
        }

        @Test
        @DisplayName("无数据时应返回空列表")
        void shouldReturnEmptyListWhenNoData() {
            long now = System.currentTimeMillis();
            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(List.of());

            List<MessageTrace> traces = topologyService.getTopicTraces(TOPIC, now - 3000L, now);

            assertThat(traces).isNotNull().isEmpty();
        }
    }

    @Nested
    @DisplayName("异常容错")
    class ErrorHandling {

        @Test
        @DisplayName("追踪查询异常时应优雅降级")
        void shouldGracefullyHandleQueryException() {
            when(traceService.queryByTopic(anyString(), anyLong(), anyLong()))
                    .thenThrow(new RuntimeException("Redis connection failed"));

            TopologyGraph topology = topologyService.getTopicTopology(TOPIC);

            assertThat(topology).isNotNull();
            assertThat(topology.topic()).isEqualTo(TOPIC);
        }

        @Test
        @DisplayName("消息追踪查询异常时应返回空链路")
        void shouldReturnEmptyTraceOnException() {
            when(traceService.queryByMessageId("msg-error"))
                    .thenThrow(new RuntimeException("Redis connection failed"));

            MessageTrace trace = topologyService.getMessageTrace("msg-error");

            assertThat(trace).isNotNull();
            assertThat(trace.messageId()).isEqualTo("msg-error");
            assertThat(trace.events()).isEmpty();
        }
    }
}
