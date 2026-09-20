/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * 管理面 / 健康面开关门控回归测试（R6-S1、R6-S2，失败即红）。
 *
 * <p>回归背景：
 *
 * <ul>
 *   <li><b>S1（死代码）</b>：{@link AdminEndpointExposureStartupWarner} 与 {@link
 *       AuthenticatorStartupLogger} 标了 {@code @Component} 却不在用户扫描范围、未进 {@code .imports} /
 *       {@code @Import}，因此 {@code AllowAllAuthenticator} 的 SECURITY ALERT 与文档承诺的 {@code
 *       streammq.admin.startup-warn} 开关全部不生效；
 *   <li><b>S2（开关串扰）</b>：管理端点与 Actuator 端点此前被类级 {@code streammq.health.enabled} 条件包住， 用户按文档关健康组件后
 *       {@code /actuator/streammq/**} 连带 404。
 * </ul>
 */
@DisplayName("管理面开关门控测试（S1/S2）")
class StreamMQAdminAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(StreamMQAutoConfiguration.class))
                    .withBean(RedissonClient.class, () -> mock(RedissonClient.class))
                    // 屏蔽真实容器（其 SmartLifecycle.start() 会触碰 Redis）：本用例只关心 Bean 门控
                    .withBean(
                            DefaultStreamMQListenerContainer.class,
                            () -> mock(DefaultStreamMQListenerContainer.class));

    @Test
    @DisplayName("S1：默认配置下两个启动提醒组件是真实 Bean（不再是扫描不到的死代码）")
    void startupWarnComponentsAreRegisteredBeans() {
        runner.run(
                context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context)
                            .hasBean("streamMQAdminEndpointExposureStartupWarner")
                            .hasBean("streamMQAuthenticatorStartupLogger");
                    assertThat(context.getBean(AdminEndpointExposureStartupWarner.class))
                            .isNotNull();
                    assertThat(context.getBean(AuthenticatorStartupLogger.class)).isNotNull();
                    // 与 Actuator 端点同源：端点存在时提醒才有意义
                    assertThat(context).hasBean("streamMQActuatorEndpoint");
                });
    }

    @Test
    @DisplayName("S1：startup-warn=false 只关提醒组件，管理端点不受影响")
    void startupWarnDisabled_onlyRemovesWarners() {
        runner.withPropertyValues("streammq.admin.startup-warn=false")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(context)
                                    .doesNotHaveBean("streamMQAdminEndpointExposureStartupWarner")
                                    .doesNotHaveBean("streamMQAuthenticatorStartupLogger");
                            assertThat(context)
                                    .hasBean("streamMQAdminEndpoint")
                                    .hasBean("streamMQActuatorEndpoint");
                        });
    }

    @Test
    @DisplayName("S2：health.enabled=false 只关健康指示器，管理/Actuator 端点必须仍在")
    void healthDisabled_keepsAdminEndpoints() {
        runner.withPropertyValues("streammq.health.enabled=false")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(context).doesNotHaveBean("streamMQHealthIndicator");
                            assertThat(context)
                                    .hasBean("streamMQAdminEndpoint")
                                    .hasBean("streamMQActuatorEndpoint");
                            // 提醒组件属于管理面，不受 health 开关影响
                            assertThat(context)
                                    .hasBean("streamMQAdminEndpointExposureStartupWarner")
                                    .hasBean("streamMQAuthenticatorStartupLogger");
                        });
    }

    @Test
    @DisplayName("S2：admin.enabled=false 只关管理端点（含提醒），健康指示器必须仍在")
    void adminDisabled_keepsHealthIndicator() {
        runner.withPropertyValues("streammq.admin.enabled=false")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            assertThat(context)
                                    .doesNotHaveBean("streamMQAdminEndpoint")
                                    .doesNotHaveBean("streamMQActuatorEndpoint")
                                    .doesNotHaveBean("streamMQAdminEndpointExposureStartupWarner")
                                    .doesNotHaveBean("streamMQAuthenticatorStartupLogger");
                            assertThat(context).hasBean("streamMQHealthIndicator");
                        });
    }
}
