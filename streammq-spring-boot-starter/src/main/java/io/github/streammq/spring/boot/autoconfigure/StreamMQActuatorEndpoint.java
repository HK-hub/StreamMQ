/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.core.util.WebRequestAuthSupport;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.endpoint.annotation.DeleteOperation;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Spring Boot Actuator 端点，暴露 StreamMQ 运维管理能力。 端点路径：{@code /actuator/streammq}
 *
 * <p><b>路由模型：</b>Actuator 端点只能通过位置型 {@code @Selector} 生成路径变量， 无法注册字面量路径段； 且多个无参
 * {@code @ReadOperation} 会产生完全相同的请求谓词（歧义注册）。 因此本端点采用<b>单操作 + 路径分发</b>模型：
 *
 * <ul>
 *   <li>{@code GET /actuator/streammq} — 总览
 *   <li>{@code GET /actuator/streammq/groups} — 消费组列表
 *   <li>{@code GET /actuator/streammq/topics} — Topic 列表
 *   <li>{@code GET /actuator/streammq/pending/{group}/{topic}} — Pending 消息
 *   <li>{@code GET /actuator/streammq/dlq/{group}} — DLQ 消息
 *   <li>{@code POST /actuator/streammq/dlq/{group}?messageId&targetTopic} — DLQ 重新入队
 *   <li>{@code DELETE /actuator/streammq/dlq/{group}/{messageId}} — 删除 DLQ 消息
 *   <li>{@code GET /actuator/streammq/stats/{group}/{topic}} — 运行时统计
 *   <li>{@code POST /actuator/streammq/ack/{group}/{topic}?messageId} — 手动 ACK
 *   <li>{@code POST /actuator/streammq/rebalance/{group}} — 触发重平衡
 *   <li>{@code POST /actuator/streammq/topics?topic=} — 创建 Topic
 *   <li>{@code DELETE /actuator/streammq/topics/{topic}} — 删除 Topic
 *   <li>{@code POST /actuator/streammq/config/{group}} — 更新消费组配置（JSON body）
 * </ul>
 *
 * <p><b>安全：</b>所有操作（含只读）均通过 {@link ManagementAuthenticator} 鉴权，鉴权失败返回 HTTP 401。 默认实现 {@code
 * DenyAllAuthenticator} 拒绝一切访问，业务方需注册 {@code AllowAllAuthenticator} / {@code
 * BasicAuthAuthenticator} / {@code TokenAuthenticator}（或自定义实现）Bean 以开放访问。Basic 凭据取自请求的 {@code
 * Authorization: Basic} 头，Token 凭据取自同一头的密码字段。
 *
 * <p>为避免对 Servlet API 的编译期依赖，请求头通过 {@code RequestContextHolder} 反射读取； 非 Web 环境下凭据视为空（默认拒绝）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@WebEndpoint(id = StreamMQSpringConstants.ENDPOINT_ID_STREAMMQ)
public class StreamMQActuatorEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQActuatorEndpoint.class);

    private final StreamMQAdminEndpoint adminEndpoint;
    private final HealthIndicator healthIndicator;
    private final ManagementAuthenticator authenticator;

    /** 管理端点列表默认页大小，可通过 {@link #setListPageSize(int)} 覆盖 */
    private volatile int listPageSize = StreamMQSpringConstants.DEFAULT_LIST_PAGE_SIZE;

    public StreamMQActuatorEndpoint(
            StreamMQAdminEndpoint adminEndpoint,
            HealthIndicator healthIndicator,
            ManagementAuthenticator authenticator) {
        this.adminEndpoint = Objects.requireNonNull(adminEndpoint, "adminEndpoint");
        this.healthIndicator = healthIndicator;
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    }

    /**
     * 设置列表默认页大小。
     *
     * @param pageSize 页大小，必须 &gt; 0
     */
    public void setListPageSize(int pageSize) {
        if (pageSize > 0) {
            this.listPageSize = pageSize;
        }
    }

    /** 校验当前请求是否具有管理权限；无权时返回 HTTP 401 响应，有权限返回 null。 */
    private WebEndpointResponse<?> checkPermission(String resource) {
        String[] credentials = WebRequestAuthSupport.parseBasicCredentialsFromRequest();
        boolean allowed =
                Objects.nonNull(credentials)
                        ? authenticator.authenticate(credentials[0], credentials[1], resource)
                        : authenticator.authenticate(null, null, resource);
        if (!allowed) {
            LOG.warn("StreamMQ admin access denied for resource: {}", resource);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("status", 401);
            body.put("error", "Access denied for resource " + resource);
            return new WebEndpointResponse<>(body, 401);
        }
        return null;
    }

    // ===================== GET 分发 =====================

    /**
     * GET 请求统一分发：按首段路由到 overview / groups / topics / pending / dlq / stats。
     *
     * @param path 端点基础路径之后的剩余段（可为 null 表示根路径）
     */
    @ReadOperation
    public Object readDispatch(@Selector(match = Selector.Match.ALL_REMAINING) String[] path) {
        if (Objects.isNull(path) || path.length == 0 || StringUtils.isEmpty(path[0])) {
            return overview();
        }
        switch (path[0]) {
            case "groups" -> {
                return groups();
            }
            case "topics" -> {
                return topics();
            }
            case "pending" -> {
                requireSegments(path, 3, "/streammq/pending/{group}/{topic}");
                return pending(path[1], path[2]);
            }
            case "dlq" -> {
                requireSegments(path, 2, "/streammq/dlq/{group}");
                return dlq(path[1]);
            }
            case "stats" -> {
                requireSegments(path, 3, "/streammq/stats/{group}/{topic}");
                return stats(path[1], path[2]);
            }
            default -> throw unknownPath("GET", path);
        }
    }

    // ===================== POST 分发 =====================

    /**
     * POST 请求统一分发：按首段路由到 requeueDlq / ackPending / triggerRebalance / createTopic /
     * updateGroupConfig。
     *
     * @param path 端点基础路径之后的剩余段
     * @param messageId 消息 ID（requeueDlq / ackPending 必填）
     * @param targetTopic 目标 Topic（requeueDlq 必填）
     * @param topic Topic 名（createTopic / ackPending 使用）
     * @param config 消费组配置（updateGroupConfig 使用）
     */
    @SuppressWarnings("unchecked")
    @WriteOperation
    public Object writeDispatch(
            @Selector(match = Selector.Match.ALL_REMAINING) String[] path,
            String messageId,
            String targetTopic,
            String topic,
            Map<String, String> config) {
        requireNonNullPath(path);
        switch (path[0]) {
            case "dlq" -> {
                requireSegments(path, 2, "/streammq/dlq/{group}?messageId&targetTopic (POST)");
                return requeueDlq(path[1], messageId, targetTopic);
            }
            case "ack" -> {
                requireSegments(path, 3, "/streammq/ack/{group}/{topic}?messageId (POST)");
                return ackPending(path[1], path[2], messageId);
            }
            case "rebalance" -> {
                requireSegments(path, 2, "/streammq/rebalance/{group} (POST)");
                return triggerRebalance(path[1]);
            }
            case "topics" -> {
                return createTopic(topic);
            }
            case "config" -> {
                requireSegments(path, 2, "/streammq/config/{group} (POST)");
                return updateGroupConfig(path[1], config);
            }
            default -> throw unknownPath("POST", path);
        }
    }

    // ===================== DELETE 分发 =====================

    /**
     * DELETE 请求统一分发：按首段路由到 deleteDlq / deleteTopic。
     *
     * @param path 端点基础路径之后的剩余段
     */
    @DeleteOperation
    public Object deleteDispatch(@Selector(match = Selector.Match.ALL_REMAINING) String[] path) {
        requireNonNullPath(path);
        switch (path[0]) {
            case "dlq" -> {
                requireSegments(path, 3, "/streammq/dlq/{group}/{messageId} (DELETE)");
                return deleteDlq(path[1], path[2]);
            }
            case "topics" -> {
                requireSegments(path, 2, "/streammq/topics/{topic} (DELETE)");
                return deleteTopic(path[1]);
            }
            default -> throw unknownPath("DELETE", path);
        }
    }

    // ===================== 操作实现 =====================

    private Object overview() {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_OVERVIEW);
        if (denied != null) {
            return denied;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(
                "status",
                healthIndicator != null
                        ? healthIndicator.health().getStatus().getCode()
                        : "UNKNOWN");
        result.put("groups", adminEndpoint.listGroups());
        result.put("topics", adminEndpoint.listTopics());
        return result;
    }

    private Object groups() {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_GROUPS);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listGroups();
    }

    private Object topics() {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listTopics();
    }

    private Object pending(String group, String topic) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_PENDING_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        if (StringUtils.isEmpty(topic)) {
            // 空 topic 会下探到 Redis 产生难以理解的报错，这里直接给出明确 4xx 语义
            throw new IllegalArgumentException(
                    "topic is required: /actuator/streammq/pending/{group}/{topic}");
        }
        return adminEndpoint.listPending(group, topic, listPageSize);
    }

    private Object dlq(String group) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listDlq(group, listPageSize);
    }

    private Object requeueDlq(String group, String messageId, String targetTopic) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        if (StringUtils.isEmpty(targetTopic)) {
            throw new IllegalArgumentException("targetTopic is required");
        }
        return adminEndpoint.requeueDlq(group, messageId, targetTopic);
    }

    private Object deleteDlq(String group, String messageId) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.deleteDlq(group, messageId);
    }

    private Object stats(String group, String topic) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_STATS_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.getStats(group, topic);
    }

    private Object ackPending(String group, String topic, String messageId) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_ACK_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.ackPending(group, topic, messageId);
    }

    private Object triggerRebalance(String group) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_REBALANCE_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.triggerRebalance(group);
    }

    private Object createTopic(String topic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        if (StringUtils.isEmpty(topic)) {
            throw new IllegalArgumentException(
                    "topic is required: POST /actuator/streammq/topics?topic=");
        }
        return adminEndpoint.createTopic(topic);
    }

    private Object deleteTopic(String topic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.deleteTopic(topic);
    }

    private Object updateGroupConfig(String group, Map<String, String> config) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_CONFIG_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.updateGroupConfig(group, config);
    }

    // ===================== 参数校验辅助 =====================

    private void requireNonNullPath(String[] path) {
        if (Objects.isNull(path)
                || path.length == 0
                || Objects.isNull(path[0])
                || path[0].isEmpty()) {
            throw new IllegalArgumentException(
                    "Path segment is required. See README management API table for routes.");
        }
    }

    private void requireSegments(String[] path, int minLength, String usage) {
        if (path.length < minLength || StringUtils.isEmpty(path[minLength - 1])) {
            throw new IllegalArgumentException("Missing path segments, expected: " + usage);
        }
    }

    private IllegalArgumentException unknownPath(String method, String[] path) {
        return new IllegalArgumentException(
                "Unknown "
                        + method
                        + " path: /actuator/streammq/"
                        + String.join("/", Arrays.asList(path)));
    }
}
