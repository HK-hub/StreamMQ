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
import java.util.regex.Pattern;
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
 * <p><b>参数校验：</b>路径变量中的 group / topic 按核心命名规则校验（{@link StringUtils#requireValidName}）， messageId
 * 必须为 {@code ts-seq} 纯数字格式，组配置更新受数量 / 键名 / 值长度上限约束； 校验失败返回 HTTP 400， 未知子路径返回 HTTP 404（而非异常导致的 500）。
 *
 * <p><b>安全：</b>所有操作（含只读）均通过 {@link ManagementAuthenticator} 鉴权，鉴权失败返回 HTTP 401。 默认实现 {@code
 * DenyAllAuthenticator} 拒绝一切访问，业务方需注册 {@code AllowAllAuthenticator} / {@code
 * BasicAuthAuthenticator} / {@code TokenAuthenticator}（或自定义实现）Bean 以开放访问。Basic 凭据取自请求的 {@code
 * Authorization: Basic} 头，Token 凭据取自同一头的密码字段。
 *
 * <p><b>CSRF 防护：</b>所有写/删操作（POST/DELETE）额外执行同源校验——跨站请求（携带与外域 {@code Host} 不一致的 {@code Origin}）直接返回
 * HTTP 403。 该检查只拦截浏览器发起的跨站简单请求，不影响 curl / SDK 等合法非浏览器调用。 若通过反向代理暴露端点， 仍建议在代理层强制同源并启用 Spring
 * Security。
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

    /**
     * 是否使用了 {@code AllowAllAuthenticator}（开放全部管理操作）。仅用于启动告警提示，不影响运行期行为。
     *
     * <p>发布前修复 P2-5：此前仅 {@code DenyAll} 场景会触发告警，而 {@code AllowAll}（最危险、零鉴权） 反而没有任何启动提示。
     */
    private volatile boolean allowAllAuthenticator = false;

    public void setAllowAll(boolean allowAllAuthenticator) {
        this.allowAllAuthenticator = allowAllAuthenticator;
    }

    public boolean isAllowAll() {
        return allowAllAuthenticator;
    }

    /** 管理端点列表默认页大小，可通过 {@link #setListPageSize(int)} 覆盖 */
    private volatile int listPageSize = StreamMQSpringConstants.DEFAULT_LIST_PAGE_SIZE;

    /** 消息 ID 合法格式：{@code ts-seq}（纯数字） */
    private static final Pattern MESSAGE_ID_PATTERN = Pattern.compile("^\\d+-\\d+$");

    /** 组配置更新：单次最大键值对数量 */
    private static final int MAX_GROUP_CONFIG_ENTRIES = 32;

    /** 组配置更新：单个 key 最大长度与合法字符集 */
    private static final Pattern GROUP_CONFIG_KEY_PATTERN =
            Pattern.compile("^[A-Za-z0-9._-]{1,64}$");

    /** 组配置更新：单个 value 最大长度（字符数） */
    private static final int MAX_GROUP_CONFIG_VALUE_LENGTH = 1024;

    /** 健康状态缓存 TTL（毫秒）：overview 高频轮询时不至于每次都触发 Redis PING */
    private static final long HEALTH_CACHE_TTL_MILLIS = 5_000L;

    /** 最近一次健康状态（volatile：缓存读写无需强一致） */
    private volatile String cachedHealthStatus;

    /** 最近一次健康状态刷新时间戳（毫秒） */
    private volatile long cachedHealthStatusAt;

    /**
     * 返回健康状态，带短 TTL 缓存。
     *
     * <p>{@code overview} 是监控高频轮询入口，而 {@code healthIndicator.health()} 每次都会发起 Redis PING； 5
     * 秒级缓存可显著降低 Redis 往返压力，且健康检查本身的时延容忍度远高于此。并发下可能重复计算一次， 幂等无害。
     *
     * @return 健康状态码（UP/DOWN/UNKNOWN），健康指示器缺失时为 UNKNOWN
     */
    private String cachedHealthStatus() {
        if (healthIndicator == null) {
            return "UNKNOWN";
        }
        long now = System.currentTimeMillis();
        String cached = cachedHealthStatus;
        if (cached != null && now - cachedHealthStatusAt < HEALTH_CACHE_TTL_MILLIS) {
            return cached;
        }
        String status = healthIndicator.health().getStatus().getCode();
        cachedHealthStatus = status;
        cachedHealthStatusAt = now;
        return status;
    }

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

    /**
     * 同源校验（CSRF 防护）：跨站请求直接拒绝（HTTP 403）。
     *
     * <p>仅影响浏览器发起的跨站写/删简单请求（POST/DELETE 不附带自定义头、但会携带外域 {@code Origin}）， 不破坏 curl / SDK 等不带 {@code
     * Origin} 或携带与 {@code Host} 一致 {@code Origin} 的合法调用。 详见 {@link
     * WebRequestAuthSupport#isSameOriginRequest()}。
     */
    private WebEndpointResponse<?> checkCsrf() {
        if (!WebRequestAuthSupport.isSameOriginRequest()) {
            LOG.warn("StreamMQ admin mutating request rejected: cross-origin (possible CSRF)");
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("status", 403);
            body.put("error", "Cross-origin request rejected (CSRF protection)");
            return new WebEndpointResponse<>(body, 403);
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
                WebEndpointResponse<?> missing =
                        requireSegments(path, 3, "/streammq/pending/{group}/{topic}");
                if (missing != null) {
                    return missing;
                }
                return pending(path[1], path[2]);
            }
            case "dlq" -> {
                WebEndpointResponse<?> missing = requireSegments(path, 2, "/streammq/dlq/{group}");
                if (missing != null) {
                    return missing;
                }
                return dlq(path[1]);
            }
            case "stats" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(path, 3, "/streammq/stats/{group}/{topic}");
                if (missing != null) {
                    return missing;
                }
                return stats(path[1], path[2]);
            }
            default -> {
                return unknownPath("GET", path);
            }
        }
    }

    // ===================== POST 分发 =====================

    /**
     * POST 请求统一分发：按首段路由到 requeueDlq / ackPending / triggerRebalance / createTopic /
     * updateGroupConfig。
     *
     * <p>Topic 创建仅接受精确的 {@code ["topics"]} 单段路径；携带未知子路径时返回 HTTP 404 而非静默按创建处理。
     *
     * @param path 端点基础路径之后的剩余段
     * @param messageId 消息 ID（requeueDlq / ackPending 必填，格式 {@code ts-seq}）
     * @param targetTopic 目标 Topic（requeueDlq 必填）
     * @param topic Topic 名（createTopic 使用）
     * @param config 消费组配置（updateGroupConfig 使用，键值对数量与长度受限）
     */
    @WriteOperation
    public Object writeDispatch(
            @Selector(match = Selector.Match.ALL_REMAINING) String[] path,
            String messageId,
            String targetTopic,
            String topic,
            Map<String, String> config) {
        WebEndpointResponse<?> missingPath = requireNonNullPath(path);
        if (missingPath != null) {
            return missingPath;
        }
        WebEndpointResponse<?> csrf = checkCsrf();
        if (csrf != null) {
            return csrf;
        }
        switch (path[0]) {
            case "dlq" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(
                                path, 2, "/streammq/dlq/{group}?messageId&targetTopic" + " (POST)");
                if (missing != null) {
                    return missing;
                }
                return requeueDlq(path[1], messageId, targetTopic);
            }
            case "ack" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(path, 3, "/streammq/ack/{group}/{topic}?messageId (POST)");
                if (missing != null) {
                    return missing;
                }
                return ackPending(path[1], path[2], messageId);
            }
            case "rebalance" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(path, 2, "/streammq/rebalance/{group} (POST)");
                if (missing != null) {
                    return missing;
                }
                return triggerRebalance(path[1]);
            }
            case "topics" -> {
                if (path.length != 1) {
                    return unknownPath("POST", path);
                }
                return createTopic(topic);
            }
            case "config" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(path, 2, "/streammq/config/{group} (POST)");
                if (missing != null) {
                    return missing;
                }
                return updateGroupConfig(path[1], config);
            }
            default -> {
                return unknownPath("POST", path);
            }
        }
    }

    // ===================== DELETE 分发 =====================

    /**
     * DELETE 请求统一分发：按首段路由到 deleteDlq / deleteTopic。
     *
     * @param path 端点基础路径之后的剩余段
     */
    @DeleteOperation
    public Object deleteDispatch(
            @Selector(match = Selector.Match.ALL_REMAINING) String[] path, String confirm) {
        WebEndpointResponse<?> missingPath = requireNonNullPath(path);
        if (missingPath != null) {
            return missingPath;
        }
        WebEndpointResponse<?> csrf = checkCsrf();
        if (csrf != null) {
            return csrf;
        }
        switch (path[0]) {
            case "dlq" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(path, 3, "/streammq/dlq/{group}/{messageId} (DELETE)");
                if (missing != null) {
                    return missing;
                }
                return deleteDlq(path[1], path[2], confirm);
            }
            case "topics" -> {
                WebEndpointResponse<?> missing =
                        requireSegments(
                                path, 2, "/streammq/topics/{topic}?confirm={topic} (DELETE)");
                if (missing != null) {
                    return missing;
                }
                return deleteTopic(path[1], confirm);
            }
            default -> {
                return unknownPath("DELETE", path);
            }
        }
    }

    // ===================== 操作实现 =====================

    private Object overview() {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_OVERVIEW);
        if (denied != null) {
            return denied;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", cachedHealthStatus());
        result.put("groups", adminEndpoint.listGroups());
        result.put("topics", adminEndpoint.listTopics());
        // 广播消费组数量是容量规划级指标：它随实例重启累积，持续增长说明实例崩溃循环
        // 或心跳超时配置过长，最终表现为 Redis 内存无声上涨。
        result.put("broadcastGroups", adminEndpoint.countBroadcastGroups());
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
        WebEndpointResponse<?> invalid = checkGroupAndTopic(group, topic);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.listPending(group, topic, listPageSize);
    }

    private Object dlq(String group) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroup(group);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.listDlq(group, listPageSize);
    }

    private Object requeueDlq(String group, String messageId, String targetTopic) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroup(group);
        if (invalid != null) {
            return invalid;
        }
        invalid = checkName(targetTopic, "targetTopic");
        if (invalid != null) {
            return invalid;
        }
        invalid = checkMessageId(messageId);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.requeueDlq(group, messageId, targetTopic);
    }

    private Object deleteDlq(String group, String messageId, String confirm) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroup(group);
        if (invalid != null) {
            return invalid;
        }
        invalid = checkMessageId(messageId);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.deleteDlq(group, messageId, confirm);
    }

    private Object stats(String group, String topic) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_STATS_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroupAndTopic(group, topic);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.getStats(group, topic);
    }

    private Object ackPending(String group, String topic, String messageId) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_ACK_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroupAndTopic(group, topic);
        if (invalid != null) {
            return invalid;
        }
        invalid = checkMessageId(messageId);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.ackPending(group, topic, messageId);
    }

    private Object triggerRebalance(String group) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_REBALANCE_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroup(group);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.triggerRebalance(group);
    }

    private Object createTopic(String topic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkName(topic, "topic");
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.createTopic(topic);
    }

    /**
     * 删除 Topic（不可逆，需 {@code confirm} 确认）。
     *
     * <p>发布前修复 P2：删除 Topic 会销毁整条 Stream 及其全部数据。要求调用方回传 {@code confirm=<topic>} 以确认意图，避免路径参数被误构造 /
     * 误触发时直接造成不可逆损失。
     */
    private Object deleteTopic(String topic, String confirm) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkName(topic, "topic");
        if (invalid != null) {
            return invalid;
        }
        if (!Objects.equals(topic, confirm)) {
            return badRequest(
                    "Refusing to delete topic '"
                            + topic
                            + "': irreversible operation requires confirm="
                            + topic);
        }
        return adminEndpoint.deleteTopic(topic, confirm);
    }

    private Object updateGroupConfig(String group, Map<String, String> config) {
        WebEndpointResponse<?> denied =
                checkPermission(StreamMQSpringConstants.RES_CONFIG_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        WebEndpointResponse<?> invalid = checkGroup(group);
        if (invalid != null) {
            return invalid;
        }
        invalid = checkGroupConfigCaps(config);
        if (invalid != null) {
            return invalid;
        }
        return adminEndpoint.updateGroupConfig(group, config);
    }

    // ===================== 参数校验辅助 =====================

    /** 名称类参数缺失时返回 HTTP 400 响应，否则返回 null。 */
    private WebEndpointResponse<?> requireNonNullPath(String[] path) {
        if (Objects.isNull(path)
                || path.length == 0
                || Objects.isNull(path[0])
                || path[0].isEmpty()) {
            return badRequest(
                    "Path segment is required. See README management API table for routes.");
        }
        return null;
    }

    /** 路径段数量不足时返回 HTTP 400 响应，否则返回 null。 */
    private WebEndpointResponse<?> requireSegments(String[] path, int minLength, String usage) {
        if (path.length < minLength || StringUtils.isEmpty(path[minLength - 1])) {
            return badRequest("Missing path segments, expected: " + usage);
        }
        return null;
    }

    /** 未知子路径统一返回 HTTP 404 响应（此前为异常 → 500）。 */
    private WebEndpointResponse<Map<String, Object>> unknownPath(String method, String[] path) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", 404);
        body.put(
                "error",
                "Unknown "
                        + method
                        + " path: /actuator/streammq/"
                        + String.join("/", Arrays.asList(path)));
        return new WebEndpointResponse<>(body, 404);
    }

    /** 构造 HTTP 400 错误响应。 */
    private WebEndpointResponse<Map<String, Object>> badRequest(String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", 400);
        body.put("error", message);
        return new WebEndpointResponse<>(body, 400);
    }

    /** 校验 group 与 topic 命名合法性（与核心层命名规则一致），非法时返回 400。 */
    private WebEndpointResponse<?> checkGroupAndTopic(String group, String topic) {
        WebEndpointResponse<?> invalid = checkGroup(group);
        if (invalid != null) {
            return invalid;
        }
        return checkName(topic, "topic");
    }

    /** 校验消费者组命名合法性，非法时返回 400。 */
    private WebEndpointResponse<?> checkGroup(String group) {
        return checkName(group, "consumerGroup");
    }

    /** 按核心命名规则（{@link StringUtils#requireValidName}）校验名称类参数； 非法时返回携带原因的 HTTP 400 响应。 */
    private WebEndpointResponse<?> checkName(String value, String kind) {
        try {
            StringUtils.requireValidName(value, kind);
            return null;
        } catch (RuntimeException ex) {
            LOG.warn("StreamMQ admin request rejected, invalid {}: {}", kind, ex.getMessage());
            return badRequest(ex.getMessage());
        }
    }

    /** 校验消息 ID 格式（{@code ts-seq} 纯数字），非法时返回 400。 */
    private WebEndpointResponse<?> checkMessageId(String messageId) {
        if (StringUtils.isEmpty(messageId)
                || !MESSAGE_ID_PATTERN.matcher(messageId.trim()).matches()) {
            return badRequest("Invalid messageId '" + messageId + "', expected format: ts-seq");
        }
        return null;
    }

    /**
     * 校验消费组配置上限：键值对数量 ≤ {@value #MAX_GROUP_CONFIG_ENTRIES}、 key 匹配 {@code [A-Za-z0-9._-]{1,64}}、
     * value 长度 ≤ {@value #MAX_GROUP_CONFIG_VALUE_LENGTH} 字符； 违规时返回 400。
     */
    private WebEndpointResponse<?> checkGroupConfigCaps(Map<String, String> config) {
        if (config == null || config.isEmpty()) {
            return badRequest("config must not be null or empty");
        }
        if (config.size() > MAX_GROUP_CONFIG_ENTRIES) {
            return badRequest(
                    "config size "
                            + config.size()
                            + " exceeds cap of "
                            + MAX_GROUP_CONFIG_ENTRIES
                            + " entries");
        }
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (key == null || !GROUP_CONFIG_KEY_PATTERN.matcher(key).matches()) {
                return badRequest(
                        "Invalid config key '" + key + "', must match ^[A-Za-z0-9._-]{1,64}$");
            }
            String value = entry.getValue();
            if (value != null && value.length() > MAX_GROUP_CONFIG_VALUE_LENGTH) {
                return badRequest(
                        "Config value for key '"
                                + key
                                + "' exceeds max length of "
                                + MAX_GROUP_CONFIG_VALUE_LENGTH
                                + " chars");
            }
        }
        return null;
    }
}
