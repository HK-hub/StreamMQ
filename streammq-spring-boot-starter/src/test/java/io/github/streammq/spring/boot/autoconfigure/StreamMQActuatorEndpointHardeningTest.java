/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.streammq.core.policy.ManagementAuthenticator;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

/**
 * {@link StreamMQActuatorEndpoint} 管理端点加固回归测试。
 *
 * <p>覆盖：命名参数校验（group / topic 与核心规则一致）、messageId 格式校验、未知子路径 404、 topics 创建的精确路径匹配、组配置更新上限（数量 / 键名 /
 * 值长度）。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("管理端点加固测试")
class StreamMQActuatorEndpointHardeningTest {

    @Mock private StreamMQAdminEndpoint adminEndpoint;

    private StreamMQActuatorEndpoint endpoint;

    @BeforeEach
    void setUp() {
        ManagementAuthenticator allowAll = (user, password, resource) -> true;
        endpoint = new StreamMQActuatorEndpoint(adminEndpoint, null, allowAll);
    }

    // ===================== 命名参数校验 =====================

    @Test
    @DisplayName("requeueDlq - 非法 group 字符返回 400 且不下探后端")
    void requeue_invalidGroup_returns400() {
        Object result =
                endpoint.writeDispatch(
                        new String[] {"dlq", "bad:group"}, "1-2", "order-topic", null, null);

        assertThat(result).isInstanceOf(WebEndpointResponse.class);
        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
        verify(adminEndpoint, never()).requeueDlq(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("requeueDlq - 非法 targetTopic 返回 400")
    void requeue_invalidTargetTopic_returns400() {
        Object result =
                endpoint.writeDispatch(
                        new String[] {"dlq", "order-group"}, "1-2", "bad{topic}", null, null);

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
        verify(adminEndpoint, never()).requeueDlq(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("DELETE dlq - messageId 非 ts-seq 格式返回 400")
    void deleteDlq_invalidMessageId_returns400() {
        Object result = endpoint.deleteDispatch(new String[] {"dlq", "order-group", "abc"}, null);

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);

        Object ok =
                endpoint.deleteDispatch(new String[] {"dlq", "order-group", "123-456"}, "123-456");
        verify(adminEndpoint).deleteDlq("order-group", "123-456", "123-456");
        assertThat(ok).isNotInstanceOf(WebEndpointResponse.class);
    }

    @Test
    @DisplayName("DELETE topics - confirm 缺失/不匹配返回 400 且不下探后端")
    void deleteTopic_missingOrWrongConfirm_returns400() {
        // confirm 缺失（null）
        Object nullConfirm = endpoint.deleteDispatch(new String[] {"topics", "order-topic"}, null);
        assertThat(((WebEndpointResponse<?>) nullConfirm).getStatus()).isEqualTo(400);

        // confirm 与 topic 不匹配
        Object wrongConfirm =
                endpoint.deleteDispatch(new String[] {"topics", "order-topic"}, "other-topic");
        assertThat(((WebEndpointResponse<?>) wrongConfirm).getStatus()).isEqualTo(400);

        verify(adminEndpoint, never()).deleteTopic(anyString(), anyString());
    }

    @Test
    @DisplayName("DELETE topics - confirm 与 topic 一致时才执行删除")
    void deleteTopic_matchingConfirm_delegates() {
        org.mockito.Mockito.when(adminEndpoint.deleteTopic("order-topic", "order-topic"))
                .thenReturn(java.util.Map.of("success", true));
        Object result =
                endpoint.deleteDispatch(new String[] {"topics", "order-topic"}, "order-topic");

        verify(adminEndpoint).deleteTopic("order-topic", "order-topic");
        assertThat(result).isNotInstanceOf(WebEndpointResponse.class);
    }

    @Test
    @DisplayName("pending - 非法 topic 返回 400")
    void pending_invalidTopic_returns400() {
        Object result = endpoint.readDispatch(new String[] {"pending", "order-group", "bad topic"});

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
    }

    @Test
    @DisplayName("rebalance - 合法 group 正常下发")
    void rebalance_validGroup_delegates() {
        org.mockito.Mockito.when(adminEndpoint.triggerRebalance("order-group"))
                .thenReturn(java.util.Map.of("success", true));
        Object result =
                endpoint.writeDispatch(
                        new String[] {"rebalance", "order-group"}, null, null, null, null);
        verify(adminEndpoint).triggerRebalance("order-group");
        assertThat(result).isNotNull();
    }

    // ===================== 路径分发 =====================

    @Test
    @DisplayName("POST topics 携带额外段时返回 404 而非按创建处理")
    void postTopics_withExtraSegment_returns404() {
        Object result =
                endpoint.writeDispatch(new String[] {"topics", "foo"}, null, null, "t", null);

        assertThat(result).isInstanceOf(WebEndpointResponse.class);
        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(404);
        verify(adminEndpoint, never()).createTopic(anyString());
    }

    @Test
    @DisplayName("POST 精确 topics 单段路径时正常创建")
    void postTopics_exactPath_delegates() {
        org.mockito.Mockito.when(adminEndpoint.createTopic("order-topic"))
                .thenReturn(java.util.Map.of("success", true));
        Object result =
                endpoint.writeDispatch(new String[] {"topics"}, null, null, "order-topic", null);

        verify(adminEndpoint).createTopic("order-topic");
        assertThat(result).isNotNull();
    }

    @Test
    @DisplayName("GET 未知子路径返回 404")
    void get_unknownPath_returns404() {
        Object result = endpoint.readDispatch(new String[] {"no-such-route"});

        assertThat(result).isInstanceOf(WebEndpointResponse.class);
        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(404);
    }

    @Test
    @DisplayName("POST 缺少路径段返回 400")
    void post_missingPath_returns400() {
        Object result = endpoint.writeDispatch(new String[0], null, null, null, null);

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
    }

    // ===================== 组配置上限 =====================

    @Test
    @DisplayName("updateGroupConfig - 超过 32 个键值对返回 400")
    void updateConfig_tooManyEntries_returns400() {
        Map<String, String> config = new HashMap<>();
        for (int i = 0; i < 33; i++) {
            config.put("key-" + i, "v");
        }
        Object result =
                endpoint.writeDispatch(
                        new String[] {"config", "order-group"}, null, null, null, config);

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
        verify(adminEndpoint, never()).updateGroupConfig(anyString(), eq(config));
    }

    @Test
    @DisplayName("updateGroupConfig - key 含非法字符返回 400")
    void updateConfig_invalidKey_returns400() {
        Map<String, String> config = Map.of("bad:key!", "v");
        Object result =
                endpoint.writeDispatch(
                        new String[] {"config", "order-group"}, null, null, null, config);

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
    }

    @Test
    @DisplayName("updateGroupConfig - value 超长返回 400")
    void updateConfig_valueTooLong_returns400() {
        Map<String, String> config = Map.of("maxRetries", "x".repeat(1025));
        Object result =
                endpoint.writeDispatch(
                        new String[] {"config", "order-group"}, null, null, null, config);

        assertThat(((WebEndpointResponse<?>) result).getStatus()).isEqualTo(400);
    }

    @Test
    @DisplayName("updateGroupConfig - 合法配置正常下发")
    void updateConfig_valid_delegates() {
        Map<String, String> config = Map.of("maxRetries", "16");
        org.mockito.Mockito.when(adminEndpoint.updateGroupConfig("order-group", config))
                .thenReturn(java.util.Map.of("success", true));
        Object result =
                endpoint.writeDispatch(
                        new String[] {"config", "order-group"}, null, null, null, config);

        verify(adminEndpoint).updateGroupConfig("order-group", config);
        assertThat(result).isNotNull();
    }
}
