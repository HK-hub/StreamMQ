package io.github.streammq.spring.boot.autoconfigure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.endpoint.annotation.*;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.*;

/**
 * Spring Boot Actuator 端点，暴露 StreamMQ 运维管理能力。
 * 端点路径：{@code /actuator/streammq}
 *
 * @author StreamMQ Contributors
 * @since 0.2.0
 */
@WebEndpoint(id = "streammq")
public class StreamMQActuatorEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQActuatorEndpoint.class);

    private final StreamMQAdminEndpoint adminEndpoint;
    private final HealthIndicator healthIndicator;

    public StreamMQActuatorEndpoint(StreamMQAdminEndpoint adminEndpoint,
                                     HealthIndicator healthIndicator) {
        this.adminEndpoint = Objects.requireNonNull(adminEndpoint, "adminEndpoint");
        this.healthIndicator = healthIndicator;
    }

    @ReadOperation
    public Map<String, Object> overview() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", healthIndicator != null ? healthIndicator.health().getStatus().getCode() : "UNKNOWN");
        result.put("groups", adminEndpoint.listGroups());
        result.put("topics", adminEndpoint.listTopics());
        return result;
    }

    @ReadOperation
    public List<Map<String, Object>> groups() {
        return adminEndpoint.listGroups();
    }

    @ReadOperation
    public List<Map<String, Object>> pending(@Selector String group,
                                              @Selector(match = Selector.Match.ALL_REMAINING) String[] path) {
        String topic = path.length > 0 ? path[0] : "";
        return adminEndpoint.listPending(group, topic, 100);
    }

    @ReadOperation
    public List<Map<String, Object>> dlq(@Selector String group) {
        return adminEndpoint.listDlq(group, 100);
    }

    @WriteOperation
    public Map<String, Object> requeueDlq(@Selector String group, String messageId, String targetTopic) {
        if (targetTopic == null || targetTopic.isEmpty()) {
            throw new IllegalArgumentException("targetTopic is required");
        }
        return adminEndpoint.requeueDlq(group, messageId, targetTopic);
    }

    @DeleteOperation
    public Map<String, Object> deleteDlq(@Selector String group, @Selector String messageId) {
        return adminEndpoint.deleteDlq(group, messageId);
    }

    @ReadOperation
    public List<String> topics() {
        return adminEndpoint.listTopics();
    }

    @ReadOperation
    public Map<String, Object> stats(@Selector String group, @Selector String topic) {
        return adminEndpoint.getStats(group, topic);
    }
}
