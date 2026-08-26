/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.policy.ManagementAuthenticator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * 基于 Basic Auth 的鉴权实现。
 *
 * <p>构造时注入用户名与密码，{@link #authenticate(String, String, String)} 验证 传入的用户名密码是否与配置匹配。
 *
 * <p>当前实现简化为：只要用户名密码匹配即通过，不区分 resource。 后续可扩展为基于 resource 的细粒度权限控制。
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
        if (Objects.isNull(username) || Objects.isNull(password)) {
            return false;
        }
        // 对 SHA-256 摘要做常量时间比较：即使长度不同，比较的输入也是等长摘要，
        // 不泄露用户名/密码的真实长度
        return MessageDigest.isEqual(sha256(this.username), sha256(username))
                && MessageDigest.isEqual(sha256(new String(this.password)), sha256(password));
    }

    @Override
    public String name() {
        return "basic-auth";
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException ex) {
            // JVM 必须支持 SHA-256（JCA 标准算法），此处仅为受检异常兜底
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }
}
