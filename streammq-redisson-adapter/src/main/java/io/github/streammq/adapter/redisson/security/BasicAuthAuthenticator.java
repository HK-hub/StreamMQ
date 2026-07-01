package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.spi.ManagementAuthenticator;

import java.util.Objects;

/**
 * 基于 Basic Auth 的鉴权实现。
 *
 * <p>构造时注入用户名与密码，{@link #authenticate(String, String, String)} 验证
 * 传入的用户名密码是否与配置匹配。
 *
 * <p>当前实现简化为：只要用户名密码匹配即通过，不区分 resource。
 * 后续可扩展为基于 resource 的细粒度权限控制。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class BasicAuthAuthenticator implements ManagementAuthenticator {

    private final String username;
    private final char[] password;

    /**
     * 构造 Basic Auth 鉴权器。
     *
     * @param username 用户名
     * @param password 密码
     */
    public BasicAuthAuthenticator(String username, String password) {
        this.username = Objects.requireNonNull(username, "username");
        this.password = Objects.requireNonNull(password, "password").toCharArray();
    }

    @Override
    public boolean authenticate(String username, String password, String resource) {
        if (username == null || password == null) {
            return false;
        }
        // 使用恒定时间比较以缓解时序攻击
        return constantTimeEquals(this.username, username)
                && constantTimeEquals(new String(this.password), password);
    }

    @Override
    public String name() {
        return "basic-auth";
    }

    /**
     * 恒定时间字符串比较，缓解时序攻击。
     *
     * @param a 字符串 a
     * @param b 字符串 b
     * @return true 如果相等
     */
    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        if (a.length() != b.length()) {
            return false;
        }
        int result = 0;
        for (int i = 0; i < a.length(); i++) {
            result |= a.charAt(i) ^ b.charAt(i);
        }
        return result == 0;
    }
}
