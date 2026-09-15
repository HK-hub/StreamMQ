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
    private final byte[] passwordDigest;

    /**
     * 构造 Basic Auth 鉴权器。
     *
     * @param username 用户名
     * @param password 密码
     */
    public BasicAuthAuthenticator(String username, String password) {
        this.username = Objects.requireNonNull(username, "username");
        // 只保留密码摘要，不保留明文/字符数组，避免每请求把 char[] 复制为不可变 String
        this.passwordDigest =
                SecureCredentialMatcher.digest(Objects.requireNonNull(password, "password"));
    }

    @Override
    public boolean authenticate(String username, String password, String resource) {
        if (Objects.isNull(username) || Objects.isNull(password)) {
            return false;
        }
        // 刻意使用非短路 & ：先完成两侧比较再合并结果，避免“用户名错误时跳过口令比较”
        // 形成的用户名有效性时序预言
        boolean usernameMatches = SecureCredentialMatcher.matches(this.username, username);
        boolean passwordMatches =
                SecureCredentialMatcher.matchesDigest(this.passwordDigest, password);
        return usernameMatches & passwordMatches;
    }

    @Override
    public String name() {
        return "basic-auth";
    }
}
