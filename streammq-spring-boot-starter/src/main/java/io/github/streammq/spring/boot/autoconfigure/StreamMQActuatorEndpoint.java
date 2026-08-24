package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.core.util.WebRequestAuthSupport;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.endpoint.annotation.*;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Spring Boot Actuator 端点，暴露 StreamMQ 运维管理能力。 端点路径：{@code /actuator/streammq}
 *
 * <p><b>安全：</b>所有操作（含只读）均通过 {@link ManagementAuthenticator} 鉴权，鉴权失败返回 HTTP 401。 默认实现 {@code
 * DenyAllAuthenticator} 拒绝一切访问，业务方需注册 {@code AllowAllAuthenticator} / {@code
 * BasicAuthAuthenticator} / {@code TokenAuthenticator}（或自定义实现）Bean 以开放访问。Basic 凭据取自请求的 {@code
 * Authorization: Basic} 头，Token 凭据取自同一头的密码字段。
 *
 * <p>为避免对 Servlet API 的编译期依赖，请求头通过 {@link RequestContextHolder} 反射读取； 非 Web 环境下凭据视为空（默认拒绝）。
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

    @ReadOperation
    public Object overview() {
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

    @ReadOperation
    public Object groups() {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_GROUPS);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listGroups();
    }

    @ReadOperation
    public Object pending(
            @Selector String group, @Selector(match = Selector.Match.ALL_REMAINING) String[] path) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_PENDING_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        String topic = path.length > 0 ? path[0] : "";
        return adminEndpoint.listPending(group, topic, listPageSize);
    }

    @ReadOperation
    public Object dlq(@Selector String group) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listDlq(group, listPageSize);
    }

    @WriteOperation
    public Object requeueDlq(@Selector String group, String messageId, String targetTopic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        if (StringUtils.isEmpty(targetTopic)) {
            throw new IllegalArgumentException("targetTopic is required");
        }
        return adminEndpoint.requeueDlq(group, messageId, targetTopic);
    }

    @DeleteOperation
    public Object deleteDlq(@Selector String group, @Selector String messageId) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_DLQ_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.deleteDlq(group, messageId);
    }

    @ReadOperation
    public Object topics() {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listTopics();
    }

    @ReadOperation
    public Object stats(@Selector String group, @Selector String topic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_STATS_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.getStats(group, topic);
    }

    @WriteOperation
    public Object ackPending(@Selector String group, @Selector String topic, String messageId) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_ACK_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.ackPending(group, topic, messageId);
    }

    @WriteOperation
    public Object triggerRebalance(@Selector String group) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_REBALANCE_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.triggerRebalance(group);
    }

    @WriteOperation
    public Object createTopic(String topic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        if (StringUtils.isEmpty(topic)) {
            throw new IllegalArgumentException("topic is required");
        }
        return adminEndpoint.createTopic(topic);
    }

    @DeleteOperation
    public Object deleteTopic(@Selector String topic) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_TOPICS);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.deleteTopic(topic);
    }

    @WriteOperation
    public Object updateGroupConfig(@Selector String group, Map<String, String> config) {
        WebEndpointResponse<?> denied = checkPermission(StreamMQSpringConstants.RES_CONFIG_PREFIX + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.updateGroupConfig(group, config);
    }
}
