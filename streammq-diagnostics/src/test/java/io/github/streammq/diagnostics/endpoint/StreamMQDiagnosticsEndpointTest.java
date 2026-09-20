/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.endpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.diagnostics.MessageProfileService;
import io.github.streammq.diagnostics.StreamMQDiagnosticsService;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.Severity;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * 诊断端点行为回归测试（发布前红队审查 R5）。
 *
 * <ul>
 *   <li>参数校验：topic/group/messageId 与 Actuator 端点同口径校验，非法输入返回 400， 且<b>不得</b>把原始字符串透传给下游拼 Redis
 *       Key（key 结构混淆 / 越权读取其它逻辑 Stream）；
 *   <li>健康状态：此前 {@code /health} 的 {@code status} 恒为 {@code UP}，严重积压期间仍报健康—— 现按积压严重度与慢消费者推导 UP /
 *       DEGRADED / DOWN。
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("StreamMQ 诊断端点")
class StreamMQDiagnosticsEndpointTest {

    private static final String TOPIC = "order-topic";
    private static final String GROUP = "order-group";

    @Mock private StreamMQDiagnosticsService diagnosticsService;
    @Mock private MessageProfileService profileService;
    @Mock private ManagementAuthenticator authenticator;

    private StreamMQDiagnosticsEndpoint endpoint;

    @BeforeEach
    void setUp() {
        endpoint =
                new StreamMQDiagnosticsEndpoint(diagnosticsService, profileService, authenticator);
        // lenient：参数校验用例在触达鉴权之前就已返回 400，该 stub 不会被执行（严格模式会报
        // UnnecessaryStubbing）。
        lenient()
                .when(
                        authenticator.authenticate(
                                nullable(String.class), nullable(String.class), anyString()))
                .thenReturn(true);
    }

    @Test
    @DisplayName("非法 topic（含 ':'）→ 400，且不调用下游服务")
    void illegalTopic_returnsBadRequest_withoutTouchingDownstream() {
        assertThatThrownBy(() -> endpoint.diagnoseBacklog("a:b", GROUP))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(
                        ex ->
                                assertThat(((ResponseStatusException) ex).getStatusCode())
                                        .isEqualTo(HttpStatus.BAD_REQUEST));
        verify(diagnosticsService, never()).diagnoseBacklog(anyString(), anyString());
    }

    @Test
    @DisplayName("非法 group（含 '*'）→ 400")
    void illegalGroup_returnsBadRequest() {
        assertThatThrownBy(() -> endpoint.diagnoseDlq("g*roup"))
                .isInstanceOf(ResponseStatusException.class);
        verify(diagnosticsService, never()).diagnoseDlq(anyString());
    }

    @Test
    @DisplayName("合法参数被规范化后透传（首尾空白被 trim）")
    void legalInput_isTrimmed() {
        when(diagnosticsService.diagnoseSlowConsume(anyString(), anyString()))
                .thenReturn(mock(SlowConsumeReport.class));

        endpoint.diagnoseSlowConsume("  " + TOPIC + "  ", GROUP);

        verify(diagnosticsService).diagnoseSlowConsume(TOPIC, GROUP);
    }

    @Test
    @DisplayName("health：无积压无慢消费者 → UP")
    void health_up() {
        when(diagnosticsService.getSlowConsumers()).thenReturn(List.of());
        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of());

        Map<String, Object> summary = endpoint.health();

        assertThat(summary.get("status")).isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_UP);
    }

    @Test
    @DisplayName("health：存在 WARNING 积压 → DEGRADED")
    void health_degradedOnWarning() {
        when(diagnosticsService.getSlowConsumers()).thenReturn(List.of());
        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of(backlog(Severity.WARNING)));

        Map<String, Object> summary = endpoint.health();

        assertThat(summary.get("status"))
                .isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_DEGRADED);
    }

    @Test
    @DisplayName("health：存在 CRITICAL 积压 → DOWN")
    void health_downOnCritical() {
        when(diagnosticsService.getSlowConsumers()).thenReturn(List.of());
        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of(backlog(Severity.CRITICAL)));

        Map<String, Object> summary = endpoint.health();

        assertThat(summary.get("status"))
                .isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_DOWN);
    }

    @Test
    @DisplayName("health：存在慢消费者但无积压 → DEGRADED")
    void health_degradedOnSlowConsumer() {
        when(diagnosticsService.getSlowConsumers()).thenReturn(List.of(TOPIC + ":" + GROUP));
        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of());

        Map<String, Object> summary = endpoint.health();

        assertThat(summary.get("status"))
                .isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_DEGRADED);
    }

    @Test
    @DisplayName("health：TTL 窗口内命中缓存（下游只采集一次），但 timestamp 每次实时")
    void health_withinTtl_servesCachedSnapshotWithFreshTimestamp() {
        when(diagnosticsService.getSlowConsumers()).thenReturn(List.of());
        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of());

        Map<String, Object> first = endpoint.health();
        Map<String, Object> second = endpoint.health();

        verify(diagnosticsService, times(1)).getSlowConsumers();
        verify(diagnosticsService, times(1)).getAllBacklogs();
        assertThat(second.get("status")).isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_UP);
        // 快照可以陈旧，但 timestamp 必须表达"本次返回"的时间——否则运维会把数据年龄误读为刚采集
        assertThat((Long) second.get(StreamMQDiagnosticsEndpointConstants.KEY_TIMESTAMP))
                .isGreaterThanOrEqualTo(
                        (Long) first.get(StreamMQDiagnosticsEndpointConstants.KEY_TIMESTAMP));
    }

    @Test
    @DisplayName("health：TTL 置 0（测试钩子）后每次调用都重新采集")
    void health_ttlZero_recollectsEveryCall() {
        endpoint.setHealthCacheTtlMillis(0L);
        when(diagnosticsService.getSlowConsumers()).thenReturn(List.of());
        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of());
        assertThat(endpoint.health().get("status"))
                .isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_UP);

        when(diagnosticsService.getAllBacklogs()).thenReturn(List.of(backlog(Severity.CRITICAL)));
        assertThat(endpoint.health().get("status"))
                .isEqualTo(StreamMQDiagnosticsEndpointConstants.STATUS_DOWN);

        verify(diagnosticsService, times(2)).getAllBacklogs();
    }

    private static BacklogReport backlog(Severity severity) {
        return new BacklogReport(
                TOPIC, GROUP, 100_000L, 10.0, -1L, 100.0, 1.0, "scale out", severity, "backlog");
    }
}
