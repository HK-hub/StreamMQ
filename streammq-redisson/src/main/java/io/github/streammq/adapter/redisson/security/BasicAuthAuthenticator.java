/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.policy.ManagementAuthenticator;
import java.util.Objects;

/**
 * 基于 Basic Auth 的鉴权实现。
 *
 * <p>构造时注入用户名与密码，{@link #authenticate(String, String, String)} 验证 传入的用户名密码是否与配置匹配。
 *
 * <p>比较经由 {@link SecureCredentialMatcher}：两侧先做 SHA-256 再常量时间比较，使比较输入恒为等长 摘要，不泄露用户名/密码的真实长度。
 *
 * <p><b>安全边界（请勿误解）：</b>这里的摘要<b>不是</b>口令散列加固——两端都是配置中的明文凭据，无盐 SHA-256 无法抵御预计算攻击。真正的防护来自配置值本身的保密（环境变量
 * / 密钥管理服务）。 若需要真正的口令存储加固，请自行接入 bcrypt/Argon2。
 *
 * <p>当前实现简化为：只要用户名密码匹配即通过，不区分 resource。 后续可扩展为基于 resource 的细粒度权限控制。
 *
 * <p><b>缺少的能力：</b>本实现不含失败重试限流。管理端点默认挂在主应用端口且不受 {@code management.endpoints.web.exposure.*}
 * 治理，请在网络层（安全组 / Ingress）限制访问来源。
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
        return SecureCredentialMatcher.matches(this.username, username)
                && SecureCredentialMatcher.matches(new String(this.password), password);
    }

    @Override
    public String name() {
        return "basic-auth";
    }
}
