/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Startup 安全提醒：管理端点暴露面检查。
 *
 * <p>当 {@code streammq.admin.enabled=true}（默认）且 Actuator 端点被注册到主应用上下文时， 在 {@link
 * ApplicationReadyEvent} 阶段发出 WARN 级别的安全提醒，提示运维方：
 *
 * <ul>
 *   <li>管理端点是标准 Actuator Web 端点（{@code @WebEndpoint(id="streammq")}），路径 {@code
 *       /actuator/streammq/**}，<b>受 {@code management.endpoints.web.exposure.*} 治理</b>——必须在 {@code
 *       exposure.include} 中加入 {@code streammq} 才能访问（Spring Boot 默认仅暴露 {@code health} / {@code
 *       info}）。
 *   <li>默认鉴权器 {@code DenyAllAuthenticator} 拒绝一切访问； 若显式注册了 {@code AllowAllAuthenticator} 或 弱 Token
 *       策略， 该端点会暴露在公网/内网。
 *   <li>关闭方式：{@code streammq.admin.enabled=false}；或将其路由到独立 management 端口（{@code
 *       management.server.port}）。
 * </ul>
 *
 * <p>另请注意：{@code streammq-diagnostics} 的 {@code /streammq/diagnostics/**} 是普通 MVC Controller（挂主端口、
 * <b>不</b>受 Actuator 治理），若引入该模块，请通过网络层（安全组 / Ingress）单独限制其访问。
 *
 * <p>此组件零行为影响——仅日志输出。可在测试环境通过 {@code -Dstreammq.admin.startup-warn=false} 关闭提醒。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class AdminEndpointExposureStartupWarner {

    private static final Logger LOG =
            LoggerFactory.getLogger(AdminEndpointExposureStartupWarner.class);

    private final ObjectProvider<StreamMQActuatorEndpoint> endpointProvider;
    private final Environment environment;

    public AdminEndpointExposureStartupWarner(
            ObjectProvider<StreamMQActuatorEndpoint> endpointProvider, Environment environment) {
        this.endpointProvider = endpointProvider;
        this.environment = environment;
    }

    /**
     * 应用启动完成后输出管理端点暴露面提醒。
     *
     * <p>仅在 {@code streammq.admin.enabled=true}（默认）且 Actuator 端点实际被装配时输出； 关闭提醒可通过 {@code
     * -Dstreammq.admin.startup-warn=false} 关闭（用于本地/CI 环境抑制噪音）。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warnOnStartup() {
        if (Boolean.FALSE.equals(
                environment.getProperty("streammq.admin.startup-warn", Boolean.class))) {
            return;
        }
        StreamMQActuatorEndpoint endpoint = endpointProvider.getIfAvailable();
        if (endpoint == null) {
            return;
        }
        String managementPort = environment.getProperty("management.server.port");
        String mainPort = environment.getProperty("server.port", "8080");
        boolean onManagementPort = managementPort != null && !managementPort.isBlank();
        if (onManagementPort) {
            LOG.info(
                    "StreamMQ admin endpoint registered on dedicated management port: {}"
                            + " (streammq.admin.enabled=true).",
                    managementPort);
            return;
        }
        // 发布前修复 P2-5：AllowAllAuthenticator 是最危险场景（零鉴权、可被任意调用方删除 Topic/DLQ/重投），
        // 必须发出比端口暴露更强的告警，避免误以为「默认安全」而对外开放。
        if (endpoint.isAllowAll()) {
            LOG.error(
                    "SECURITY ALERT: StreamMQ admin endpoint is using AllowAllAuthenticator — ALL"
                        + " management operations (create/delete Topic, delete DLQ messages,"
                        + " requeue, rebalance, manual ACK) are OPEN TO ANYONE with network access,"
                        + " including destructive ones. This is only acceptable for local dev /"
                        + " isolated networks. For any shared or production environment, register a"
                        + " BasicAuthAuthenticator / TokenAuthenticator (or custom"
                        + " ManagementAuthenticator) and restrict access at the network layer. To"
                        + " suppress this warning, set -Dstreammq.admin.startup-warn=false.");
        }
        LOG.warn(
                "StreamMQ admin endpoint (WebEndpoint id=streammq) registered on the MAIN"
                    + " application port ({}). Path: /actuator/streammq/** — governed by"
                    + " management.endpoints.web.exposure.*: make sure \"streammq\" is included in"
                    + " management.endpoints.web.exposure.include (Spring Boot default exposes only"
                    + " health/info, so /actuator/streammq/** is NOT reachable until added)."
                    + " Recommend either: (a) set management.server.port to isolate the endpoint,"
                    + " (b) restrict access at network level (firewall / Ingress), (c) keep"
                    + " DenyAllAuthenticator and only open via custom SPI registration. To suppress"
                    + " this warning, set -Dstreammq.admin.startup-warn=false.",
                mainPort);
    }
}
