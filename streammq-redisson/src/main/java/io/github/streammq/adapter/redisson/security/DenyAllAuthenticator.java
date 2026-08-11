package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.policy.ManagementAuthenticator;

/**
 * 拒绝所有请求的鉴权器，安全兜底默认实现。
 *
 * <p>{@link #authenticate(String, String, String)} 始终返回 false， 适用于未配置鉴权时的安全兜底，避免误开放运维端点。
 *
 * <p>生产环境应替换为具体鉴权实现（如 {@link BasicAuthAuthenticator}）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DenyAllAuthenticator implements ManagementAuthenticator {

  @Override
  public boolean authenticate(String username, String password, String resource) {
    return false;
  }

  @Override
  public String name() {
    return "deny-all";
  }
}
