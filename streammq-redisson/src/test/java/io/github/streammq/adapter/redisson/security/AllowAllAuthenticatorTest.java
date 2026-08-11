package io.github.streammq.adapter.redisson.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link AllowAllAuthenticator} 单元测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("AllowAllAuthenticator 放行所有鉴权器测试")
class AllowAllAuthenticatorTest {

  private final AllowAllAuthenticator authenticator = new AllowAllAuthenticator();

  @Test
  @DisplayName("authenticate 始终返回 true（任意入参）")
  void authenticateAlwaysTrue() {
    assertThat(authenticator.authenticate("admin", "pwd", "topic:t1")).isTrue();
    assertThat(authenticator.authenticate(null, null, null)).isTrue();
    assertThat(authenticator.authenticate("user", "secret", "dlq:g1")).isTrue();
  }

  @Test
  @DisplayName("name 返回 allow-all")
  void name() {
    assertThat(authenticator.name()).isEqualTo("allow-all");
  }
}
