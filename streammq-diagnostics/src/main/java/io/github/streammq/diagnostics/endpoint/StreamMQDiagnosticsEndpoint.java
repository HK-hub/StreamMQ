/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.endpoint;

import io.github.streammq.core.policy.ManagementAuthenticator;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.core.util.WebRequestAuthSupport;
import io.github.streammq.diagnostics.MessageProfileService;
import io.github.streammq.diagnostics.StreamMQDiagnosticsService;
import io.github.streammq.diagnostics.model.BacklogReport;
import io.github.streammq.diagnostics.model.DlqReport;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.Severity;
import io.github.streammq.diagnostics.model.SlowConsumeReport;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;

/**
 * StreamMQ 诊断 REST 端点，暴露 JSON 接口供仪表盘集成。
 *
 * <p>基础路径：{@code /streammq/diagnostics}
 *
 * <p><b>@apiNote 信任模型（务必知悉）：</b>这些端点挂在<b>应用主端口</b>上、以普通 Spring MVC {@code @Controller} 形式注册，<b>不受
 * {@code management.endpoints.web.exposure} 控制</b>—— 引入诊断模块并启用装配即注册生效。鉴权仍然强制执行（默认 {@code
 * DenyAllAuthenticator} 拒绝一切访问）， 但路径本身不经过 Actuator 的管理端口隔离。生产环境建议：
 *
 * <ul>
 *   <li>搭配独立的 {@code management.server.port} 将主业务流量与管理/诊断流量隔离，或
 *   <li>通过防火墙 / Ingress 规则限制 {@code /streammq/diagnostics/**} 的可达来源
 * </ul>
 *
 * <p>本类不使用 {@code @RestController}，而是通过 {@link
 * StreamMQDiagnosticsAutoConfiguration#streamMQDiagnosticsEndpoint} 注册为 Bean，
 * 确保仅在诊断服务与画像服务均就绪时才启用端点，避免组件扫描导致的依赖缺失问题。
 *
 * <p>提供以下端点：
 *
 * <ul>
 *   <li>{@code GET /streammq/diagnostics/profile/{messageId}} - 获取消息画像
 *   <li>{@code GET /streammq/diagnostics/slow-consume?topic=&group=} - 诊断慢消费
 *   <li>{@code GET /streammq/diagnostics/backlog?topic=&group=} - 诊断积压
 *   <li>{@code GET /streammq/diagnostics/dlq?group=} - 诊断死信队列
 *   <li>{@code GET /streammq/diagnostics/slow-consumers} - 列出所有慢消费者
 *   <li>{@code GET /streammq/diagnostics/all-backlogs} - 列出所有积压报告
 *   <li>{@code GET /streammq/diagnostics/health} - 诊断健康概览
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@ResponseBody
@RequestMapping(StreamMQDiagnosticsEndpointConstants.BASE_PATH)
public class StreamMQDiagnosticsEndpoint {

    private static final Logger log = LoggerFactory.getLogger(StreamMQDiagnosticsEndpoint.class);

    private final StreamMQDiagnosticsService diagnosticsService;
    private final MessageProfileService profileService;
    private final ManagementAuthenticator authenticator;

    /**
     * 构造诊断端点。
     *
     * @param diagnosticsService 诊断服务
     * @param profileService 消息画像服务
     * @param authenticator 管理鉴权器（默认拒绝所有访问，返回 401）
     */
    public StreamMQDiagnosticsEndpoint(
            StreamMQDiagnosticsService diagnosticsService,
            MessageProfileService profileService,
            ManagementAuthenticator authenticator) {
        this.diagnosticsService = Objects.requireNonNull(diagnosticsService, "diagnosticsService");
        this.profileService = Objects.requireNonNull(profileService, "profileService");
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator");
    }

    /**
     * 校验当前请求是否具有诊断访问权限；无权限时抛出 401。
     *
     * @param resource 被访问的资源标识
     */
    private void checkPermission(String resource) {
        String[] credentials = WebRequestAuthSupport.parseBasicCredentialsFromRequest();
        boolean allowed =
                Objects.nonNull(credentials)
                        ? authenticator.authenticate(credentials[0], credentials[1], resource)
                        : authenticator.authenticate(null, null, resource);
        if (!allowed) {
            log.warn("StreamMQ diagnostics access denied for resource: {}", resource);
            throw new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "Access denied for resource " + resource);
        }
    }

    /**
     * 获取消息完整生命周期画像。
     *
     * @param messageId 消息 ID
     * @return 消息画像，若不存在则返回 404
     */
    @GetMapping("/profile/{messageId}")
    public ResponseEntity<MessageProfile> getProfile(@PathVariable String messageId) {
        String validMessageId = requireValid(messageId, "messageId");
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_PROFILE_PREFIX + validMessageId);
        MessageProfile profile = profileService.getProfile(validMessageId);
        if (Objects.isNull(profile)) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(profile);
    }

    /**
     * 诊断慢消费。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 慢消费诊断报告
     */
    @GetMapping("/slow-consume")
    public SlowConsumeReport diagnoseSlowConsume(
            @RequestParam String topic, @RequestParam String group) {
        String validTopic = requireValidTopic(topic);
        String validGroup = requireValidGroup(group);
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_SLOW_CONSUME_PREFIX + validTopic);
        return diagnosticsService.diagnoseSlowConsume(validTopic, validGroup);
    }

    /**
     * 诊断消息积压。
     *
     * @param topic 主题
     * @param group 消费者组
     * @return 积压诊断报告
     */
    @GetMapping("/backlog")
    public BacklogReport diagnoseBacklog(@RequestParam String topic, @RequestParam String group) {
        String validTopic = requireValidTopic(topic);
        String validGroup = requireValidGroup(group);
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_BACKLOG_PREFIX + validTopic);
        return diagnosticsService.diagnoseBacklog(validTopic, validGroup);
    }

    /**
     * 诊断死信队列。
     *
     * @param group 消费者组
     * @return 死信队列诊断报告
     */
    @GetMapping("/dlq")
    public DlqReport diagnoseDlq(@RequestParam String group) {
        String validGroup = requireValidGroup(group);
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_DLQ_PREFIX + validGroup);
        return diagnosticsService.diagnoseDlq(validGroup);
    }

    /**
     * 列出所有慢消费者。
     *
     * @return 慢消费者标识列表（格式：topic:group）
     */
    @GetMapping("/slow-consumers")
    public List<String> getSlowConsumers() {
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_SLOW_CONSUMERS);
        return diagnosticsService.getSlowConsumers();
    }

    /**
     * 列出所有消费者组的积压报告。
     *
     * @return 积压报告列表
     */
    @GetMapping("/all-backlogs")
    public List<BacklogReport> getAllBacklogs() {
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_ALL_BACKLOGS);
        return diagnosticsService.getAllBacklogs();
    }

    /**
     * 诊断健康概览，聚合所有诊断数据为单一视图。
     *
     * <p><b>状态不再是常量（发布前红队审查 R5）：</b>旧实现无论积压多严重、慢消费者多少，{@code status} 恒为 {@code
     * UP}。运维若以该字段做告警/看板判据，事故期间会持续显示健康——典型的"静默故障"。 现按积压严重度与慢消费者聚合推导：
     *
     * <ul>
     *   <li>任一积压报告为 {@link Severity#CRITICAL} → {@code DOWN}
     *   <li>否则存在 {@link Severity#WARNING} 积压、或存在慢消费者 → {@code DEGRADED}
     *   <li>否则 {@code UP}
     * </ul>
     *
     * @return 健康概览 Map
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        checkPermission(StreamMQDiagnosticsEndpointConstants.RES_HEALTH);
        List<String> slowConsumers = diagnosticsService.getSlowConsumers();
        List<BacklogReport> backlogs = diagnosticsService.getAllBacklogs();
        long totalBacklog = backlogs.stream().mapToLong(BacklogReport::currentBacklog).sum();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put(
                StreamMQDiagnosticsEndpointConstants.KEY_STATUS,
                deriveStatus(backlogs, slowConsumers));
        summary.put(
                StreamMQDiagnosticsEndpointConstants.KEY_SLOW_CONSUMER_COUNT, slowConsumers.size());
        summary.put(StreamMQDiagnosticsEndpointConstants.KEY_SLOW_CONSUMERS, slowConsumers);
        summary.put(StreamMQDiagnosticsEndpointConstants.KEY_TOTAL_BACKLOG, totalBacklog);
        summary.put(StreamMQDiagnosticsEndpointConstants.KEY_BACKLOG_REPORTS, backlogs);
        summary.put(StreamMQDiagnosticsEndpointConstants.KEY_TIMESTAMP, System.currentTimeMillis());
        return summary;
    }

    /** 由积压严重度与慢消费者数量推导整体状态（CRITICAL > WARNING/慢消费者 > UP）。 */
    private static String deriveStatus(List<BacklogReport> backlogs, List<String> slowConsumers) {
        boolean degraded = !slowConsumers.isEmpty();
        for (BacklogReport report : backlogs) {
            Severity severity = report.severity();
            if (severity == Severity.CRITICAL) {
                return StreamMQDiagnosticsEndpointConstants.STATUS_DOWN;
            }
            if (severity == Severity.WARNING) {
                degraded = true;
            }
        }
        return degraded
                ? StreamMQDiagnosticsEndpointConstants.STATUS_DEGRADED
                : StreamMQDiagnosticsEndpointConstants.STATUS_UP;
    }

    /**
     * 校验并规范化 topic：非法输入返回 400 而不是把原始字符串透传给下游拼 Redis Key。
     *
     * <p>与 Actuator 端点（{@code StringUtils.requireValidName}）保持同一口径：禁止
     * {@code :}、{@code *}、{@code {}、空白等字符，避免 key 结构被越权构造/混淆。
     */
    private static String requireValidTopic(String topic) {
        return requireValid(topic, "topic");
    }

    /** 校验并规范化消费者组名（口径同 {@link #requireValidTopic(String)}）。 */
    private static String requireValidGroup(String group) {
        return requireValid(group, "group");
    }

    private static String requireValid(String value, String field) {
        try {
            return StringUtils.requireValidName(value, field);
        } catch (IllegalArgumentException | NullPointerException ex) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Invalid " + field + ": " + ex.getMessage(), ex);
        }
    }
}
