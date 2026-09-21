/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.core.policy.ManagementAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;

/**
 * Startup warner: when the default {@link DenyAllAuthenticator} is in use and admin is enabled,
 * emit a one-line INFO message explaining how to open access.
 *
 * <p>This is a sibling of {@link AdminEndpointExposureStartupWarner}; together they cover the two
 * common "I started the app, the admin endpoint returned 401, what now?" confusion points.
 *
 * <p>Suppress with {@code -Dstreammq.admin.startup-warn=false}.
 *
 * <p><b>注意（R6-S1）：</b>本类<b>不能</b>再标注 {@code @Component}——它位于自动装配包，不在用户组件扫描范围， 此前正是因此成为死代码。注册点见
 * {@link StreamMQAdminAutoConfiguration}（{@code @Bean} + 条件，{@code @EventListener} 对 {@code @Bean}
 * 实例同样生效）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AuthenticatorStartupLogger {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticatorStartupLogger.class);

    private final ObjectProvider<ManagementAuthenticator> authenticatorProvider;
    private final Environment environment;

    public AuthenticatorStartupLogger(
            ObjectProvider<ManagementAuthenticator> authenticatorProvider,
            Environment environment) {
        this.authenticatorProvider = authenticatorProvider;
        this.environment = environment;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void logAuthenticator() {
        // 容错解析：非法取值不得让 ApplicationReadyEvent 抛异常阻断应用启动（见 StartupWarnToggle）
        if (StartupWarnToggle.isDisabled(environment)) {
            return;
        }
        ManagementAuthenticator authenticator =
                StreamMQBeanResolution.uniqueOrNull(
                        authenticatorProvider, "ManagementAuthenticator");
        if (authenticator == null) {
            return;
        }
        if (authenticator instanceof DenyAllAuthenticator) {
            boolean adminEnabled =
                    Boolean.parseBoolean(environment.getProperty("streammq.admin.enabled", "true"));
            if (adminEnabled) {
                LOG.info(
                        "StreamMQ admin endpoint is gated by DenyAllAuthenticator. To open access,"
                                + " register one of: AllowAllAuthenticator,"
                                + " BasicAuthAuthenticator, TokenAuthenticator, or your own"
                                + " ManagementAuthenticator implementation. See"
                                + " docs/archived-historical/02-architecture.md or README for"
                                + " examples.");
            }
        }
    }
}
