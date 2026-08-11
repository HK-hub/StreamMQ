package io.github.streammq.adapter.redisson.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link BasicAuthAuthenticator} 单元测试，覆盖匹配/不匹配、null 入参与构造校验。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("BasicAuthAuthenticator Basic Auth 鉴权器测试")
class BasicAuthAuthenticatorTest {

  private final BasicAuthAuthenticator authenticator =
      new BasicAuthAuthenticator("admin", "s3cret");

  @Test
  @DisplayName("用户名密码匹配返回 true")
  void authenticateMatched() {
    assertThat(authenticator.authenticate("admin", "s3cret", "topic:t1")).isTrue();
  }

  @Test
  @DisplayName("用户名密码匹配时 resource 任意值均通过")
  void authenticateMatchedAnyResource() {
    assertThat(authenticator.authenticate("admin", "s3cret", "dlq:g1")).isTrue();
    assertThat(authenticator.authenticate("admin", "s3cret", null)).isTrue();
  }

  @Test
  @DisplayName("密码不匹配返回 false")
  void authenticateWrongPassword() {
    assertThat(authenticator.authenticate("admin", "wrong", "topic:t1")).isFalse();
  }

  @Test
  @DisplayName("用户名不匹配返回 false")
  void authenticateWrongUsername() {
    assertThat(authenticator.authenticate("root", "s3cret", "topic:t1")).isFalse();
  }

  @Test
  @DisplayName("用户名密码均不匹配返回 false")
  void authenticateBothWrong() {
    assertThat(authenticator.authenticate("root", "wrong", "topic:t1")).isFalse();
  }

  @Test
  @DisplayName("username 为 null 返回 false")
  void authenticateNullUsername() {
    assertThat(authenticator.authenticate(null, "s3cret", "topic:t1")).isFalse();
  }

  @Test
  @DisplayName("password 为 null 返回 false")
  void authenticateNullPassword() {
    assertThat(authenticator.authenticate("admin", null, "topic:t1")).isFalse();
  }

  @Test
  @DisplayName("构造时 username 为 null 抛出 NullPointerException")
  void constructNullUsername() {
    assertThatThrownBy(() -> new BasicAuthAuthenticator(null, "pwd"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("username");
  }

  @Test
  @DisplayName("构造时 password 为 null 抛出 NullPointerException")
  void constructNullPassword() {
    assertThatThrownBy(() -> new BasicAuthAuthenticator("admin", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("password");
  }

  @Test
  @DisplayName("name 返回 basic-auth")
  void name() {
    assertThat(authenticator.name()).isEqualTo("basic-auth");
  }
}
