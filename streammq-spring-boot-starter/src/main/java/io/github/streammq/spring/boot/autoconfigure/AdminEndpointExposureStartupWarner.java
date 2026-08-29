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
 *   <li>管理端点通过 MVC 暴露，路径 {@code /actuator/streammq/**}，与主业务接口同端口； 不会被 {@code
 *       management.endpoints.web.exposure.*} 控制。
 *   <li>默认鉴权器 {@code DenyAllAuthenticator} 拒绝一切访问； 若显式注册了 {@code AllowAllAuthenticator} 或 弱 Token
 *       策略， 该端点会暴露在公网/内网。
 *   <li>关闭方式：{@code streammq.admin.enabled=false}；或将其路由到独立 management 端口（{@code
 *       management.server.port}）。
 * </ul>
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
        LOG.warn(
                "StreamMQ admin endpoint registered on the MAIN application port ({}). Path:"
                    + " /actuator/streammq/** — NOT governed by management.endpoints.web.exposure.*"
                    + " — recommend either: (a) set management.server.port to isolate the endpoint,"
                    + " (b) restrict access at network level (firewall / Ingress), (c) keep"
                    + " DenyAllAuthenticator and only open via custom SPI registration. To suppress"
                    + " this warning, set -Dstreammq.admin.startup-warn=false.",
                mainPort);
    }
}
