/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * K8s 健康组件注册集成测试。
 *
 * <p>回归背景：{@code CloudK8sAutoConfiguration} 此前仅注册 Operator 相关 3 个 Bean， {@link
 * StreamMQHealthController} / {@link StreamMQHealthIndicator} / {@link GracefulShutdownHandler}
 * 从未被实例化，导致 {@code /streammq/health/*} 探针端点 404。
 *
 * <p>验证：启用模块后三个 Bean 均存在，且 Servlet Web 环境下 readiness 路径能被 {@link RequestMappingHandlerMapping} 解析到
 * {@code StreamMQHealthController.readiness} 方法。
 */
@DisplayName("K8s 健康组件注册集成测试")
class KubernetesHealthRegistrationIT {

    private final WebApplicationContextRunner runner =
            new WebApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    WebMvcAutoConfiguration.class, CloudK8sAutoConfiguration.class))
                    .withPropertyValues("streammq.cloud.k8s.enabled=true");

    @Test
    @DisplayName("启用模块后健康组件 Bean 存在且探针路径可被解析")
    void beansExistAndRequestMappingResolves() {
        runner.run(
                context -> {
                    assertThat(context).hasBean("streamMQK8sHealthIndicator");
                    assertThat(context).hasBean("streamMQHealthController");
                    assertThat(context).hasBean("gracefulShutdownHandler");

                    // 探针控制器为 @RestController：验证就绪探针路径已注册映射（无需真实端口）
                    RequestMappingHandlerMapping mapping =
                            context.getBean(RequestMappingHandlerMapping.class);
                    HttpServletRequest request =
                            new MockHttpServletRequest("GET", "/streammq/health/readiness");
                    HandlerExecutionChain chain = mapping.getHandler(request);
                    assertThat(chain).isNotNull();
                    assertThat(chain.getHandler().toString()).contains("readiness");
                });
    }

    @Test
    @DisplayName("未显式启用时（默认 false）不注册任何健康组件")
    void disabledByDefault_noBeansRegistered() {
        new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CloudK8sAutoConfiguration.class))
                .run(
                        context -> {
                            assertThat(context).doesNotHaveBean("streamMQK8sHealthIndicator");
                            assertThat(context).doesNotHaveBean("streamMQHealthController");
                            assertThat(context).doesNotHaveBean("gracefulShutdownHandler");
                        });
    }

    @Test
    @DisplayName("health-endpoint-enabled=false 时跳过探针控制器但保留其余组件")
    void healthEndpointDisabled_controllerSkipped() {
        runner.withPropertyValues("streammq.cloud.k8s.health-endpoint-enabled=false")
                .run(
                        context -> {
                            assertThat(context).hasBean("streamMQK8sHealthIndicator");
                            assertThat(context).doesNotHaveBean("streamMQHealthController");
                            assertThat(context).hasBean("gracefulShutdownHandler");
                        });
    }

    @Test
    @DisplayName("优雅关闭处理器注册为容器 Bean 且初始未处于关闭状态")
    void gracefulShutdown_registeredAsBean() {
        runner.run(
                context -> {
                    GracefulShutdownHandler handler =
                            context.getBean(GracefulShutdownHandler.class);
                    assertThat(handler.isShuttingDown()).isFalse();
                });
    }
}
