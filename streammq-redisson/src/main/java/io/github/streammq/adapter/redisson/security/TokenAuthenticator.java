package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.policy.ManagementAuthenticator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * 基于 Token 的鉴权实现。
 *
 * <p>构造时注入期望的 token，{@link #authenticate(String, String, String)} 将 {@code password} 参数作为 token
 * 凭证，验证其是否与期望 token 匹配。
 *
 * <p>使用 {@link MessageDigest#isEqual(byte[], byte[])} 进行时间安全比较， 防止通过耗时差异推断 token 内容的时序攻击。
 *
 * <p>当前实现不区分 resource，仅校验 token 是否匹配。 适用于使用单一共享 Token 的简单鉴权场景；如需细粒度权限控制，请自行扩展。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TokenAuthenticator implements ManagementAuthenticator {

  private final byte[] expectedToken;

  /**
   * 构造 Token 鉴权器。
   *
   * @param expectedToken 期望的 token 字符串，不能为 null
   */
  public TokenAuthenticator(String expectedToken) {
    Objects.requireNonNull(expectedToken, "expectedToken");
    this.expectedToken = expectedToken.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public boolean authenticate(String username, String password, String resource) {
    if (Objects.isNull(password)) {
      return false;
    }
    byte[] credential = password.getBytes(StandardCharsets.UTF_8);
    // MessageDigest.isEqual 进行时间安全比较，防止时序攻击
    return MessageDigest.isEqual(credential, expectedToken);
  }

  @Override
  public String name() {
    return "token";
  }
}
