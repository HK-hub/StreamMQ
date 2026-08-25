/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link DenyAllAuthenticator} 单元测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("DenyAllAuthenticator 拒绝所有鉴权器测试")
class DenyAllAuthenticatorTest {

    private final DenyAllAuthenticator authenticator = new DenyAllAuthenticator();

    @Test
    @DisplayName("authenticate 始终返回 false（任意入参）")
    void authenticateAlwaysFalse() {
        assertThat(authenticator.authenticate("admin", "pwd", "topic:t1")).isFalse();
        assertThat(authenticator.authenticate(null, null, null)).isFalse();
        assertThat(authenticator.authenticate("user", "secret", "dlq:g1")).isFalse();
    }

    @Test
    @DisplayName("name 返回 deny-all")
    void name() {
        assertThat(authenticator.name()).isEqualTo("deny-all");
    }
}
