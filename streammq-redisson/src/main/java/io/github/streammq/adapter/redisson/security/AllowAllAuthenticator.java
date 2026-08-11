package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.policy.ManagementAuthenticator;

/**
 * 放行所有请求的鉴权器。
 *
 * <p>{@link #authenticate(String, String, String)} 始终返回 true，
 * 仅用于开发/测试环境，<strong>严禁</strong>在生产环境使用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AllowAllAuthenticator implements ManagementAuthenticator {

    @Override
    public boolean authenticate(String username, String password, String resource) {
        return true;
    }

    @Override
    public String name() {
        return "allow-all";
    }
}
