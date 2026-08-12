package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.util.StringUtils;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
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
@WebEndpoint(id = "streammq")
public class StreamMQActuatorEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQActuatorEndpoint.class);

    private final StreamMQAdminEndpoint adminEndpoint;
    private final HealthIndicator healthIndicator;
    private final ManagementAuthenticator authenticator;

    public StreamMQActuatorEndpoint(
            StreamMQAdminEndpoint adminEndpoint,
            HealthIndicator healthIndicator,
            ManagementAuthenticator authenticator) {
        this.adminEndpoint = Objects.requireNonNull(adminEndpoint, "adminEndpoint");
        this.healthIndicator = healthIndicator;
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    }

    /** 校验当前请求是否具有管理权限；无权时返回 HTTP 401 响应，有权限返回 null。 */
    private WebEndpointResponse<?> checkPermission(String resource) {
        String[] credentials = parseBasicCredentialsFromRequest();
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

    /** 从当前请求上下文中反射读取 {@code Authorization: Basic} 头，返回 [user, pass] 或 null。 */
    private static String[] parseBasicCredentialsFromRequest() {
        // 纯反射访问 Spring 的 RequestContextHolder，避免对 spring-web / servlet-api 的编译期依赖：
        // 非 Web 环境或缺少 spring-web 时返回 null，凭据为空 → 默认拒绝。
        Object attrs = null;
        try {
            Class<?> holder =
                    Class.forName("org.springframework.web.context.request.RequestContextHolder");
            attrs = holder.getMethod("getRequestAttributes").invoke(null);
        } catch (ReflectiveOperationException | LinkageError ex) {
            LOG.debug("RequestContextHolder unavailable: {}", ex.getMessage());
        }
        if (attrs == null) {
            return null;
        }
        Object request = null;
        try {
            request = attrs.getClass().getMethod("getRequest").invoke(attrs);
        } catch (ReflectiveOperationException ex) {
            LOG.debug("No servlet request in context: {}", ex.getMessage());
        }
        if (request == null) {
            return null;
        }
        String authorizationHeader = null;
        try {
            Method getHeader = request.getClass().getMethod("getHeader", String.class);
            authorizationHeader = (String) getHeader.invoke(request, "Authorization");
        } catch (ReflectiveOperationException ex) {
            LOG.debug("Failed to read Authorization header: {}", ex.getMessage());
        }
        return parseBasicCredentials(authorizationHeader);
    }

    /** 解析 {@code Authorization: Basic base64(user:pass)} 头，返回 [user, pass] 或 null。 */
    private static String[] parseBasicCredentials(String authorizationHeader) {
        if (StringUtils.isEmpty(authorizationHeader)
                || !authorizationHeader.regionMatches(true, 0, "Basic ", 0, 6)) {
            return null;
        }
        try {
            String encoded = authorizationHeader.substring(6).trim();
            byte[] decoded = Base64.getDecoder().decode(encoded);
            String decodedStr = new String(decoded, StandardCharsets.UTF_8);
            int idx = decodedStr.indexOf(':');
            if (idx < 0) {
                return null;
            }
            return new String[] {decodedStr.substring(0, idx), decodedStr.substring(idx + 1)};
        } catch (IllegalArgumentException ex) {
            LOG.debug("Invalid Basic Authorization header: {}", ex.getMessage());
            return null;
        }
    }

    @ReadOperation
    public Object overview() {
        WebEndpointResponse<?> denied = checkPermission("overview");
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
        WebEndpointResponse<?> denied = checkPermission("groups");
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listGroups();
    }

    @ReadOperation
    public Object pending(
            @Selector String group, @Selector(match = Selector.Match.ALL_REMAINING) String[] path) {
        WebEndpointResponse<?> denied = checkPermission("pending:" + group);
        if (denied != null) {
            return denied;
        }
        String topic = path.length > 0 ? path[0] : "";
        return adminEndpoint.listPending(group, topic, 100);
    }

    @ReadOperation
    public Object dlq(@Selector String group) {
        WebEndpointResponse<?> denied = checkPermission("dlq:" + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listDlq(group, 100);
    }

    @WriteOperation
    public Object requeueDlq(@Selector String group, String messageId, String targetTopic) {
        WebEndpointResponse<?> denied = checkPermission("dlq:" + group);
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
        WebEndpointResponse<?> denied = checkPermission("dlq:" + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.deleteDlq(group, messageId);
    }

    @ReadOperation
    public Object topics() {
        WebEndpointResponse<?> denied = checkPermission("topics");
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.listTopics();
    }

    @ReadOperation
    public Object stats(@Selector String group, @Selector String topic) {
        WebEndpointResponse<?> denied = checkPermission("stats:" + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.getStats(group, topic);
    }

    @WriteOperation
    public Object ackPending(@Selector String group, @Selector String topic, String messageId) {
        WebEndpointResponse<?> denied = checkPermission("ack:" + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.ackPending(group, topic, messageId);
    }

    @WriteOperation
    public Object triggerRebalance(@Selector String group) {
        WebEndpointResponse<?> denied = checkPermission("rebalance:" + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.triggerRebalance(group);
    }

    @WriteOperation
    public Object createTopic(String topic) {
        WebEndpointResponse<?> denied = checkPermission("topics");
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
        WebEndpointResponse<?> denied = checkPermission("topics");
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.deleteTopic(topic);
    }

    @WriteOperation
    public Object updateGroupConfig(@Selector String group, Map<String, String> config) {
        WebEndpointResponse<?> denied = checkPermission("config:" + group);
        if (denied != null) {
            return denied;
        }
        return adminEndpoint.updateGroupConfig(group, config);
    }
}
