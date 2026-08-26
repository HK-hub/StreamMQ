/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.security.DenyAllAuthenticator;
import io.github.streammq.core.policy.ManagementAuthenticator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 管理端点默认鉴权装配回归测试。
 *
 * <p>安全基线：未注册任何 {@link ManagementAuthenticator} Bean 时，自动装配必须落到 {@link
 * DenyAllAuthenticator}（fail-closed）。此前缺少对该装配行为的测试断言。
 */
@DisplayName("管理端点默认鉴权装配测试")
class ManagementAuthenticatorDefaultTest {

    @Test
    @DisplayName("无用户 Bean 时默认装配 DenyAllAuthenticator 且拒绝一切访问")
    void defaultAuthenticatorIsDenyAllAndDenies() {
        StreamMQCoreAutoConfiguration configuration =
                new StreamMQCoreAutoConfiguration(
                        new io.github.streammq.spring.boot.properties.StreamMQProperties());
        ManagementAuthenticator authenticator = configuration.streamMQManagementAuthenticator();

        assertThat(authenticator).isInstanceOf(DenyAllAuthenticator.class);
        assertThat(authenticator.authenticate("admin", "admin", "/overview")).isFalse();
        assertThat(authenticator.authenticate(null, null, null)).isFalse();
    }
}
