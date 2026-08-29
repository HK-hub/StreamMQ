/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.security;

import io.github.streammq.core.policy.ManagementAuthenticator;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * 基于 Token 的鉴权实现。
 *
 * <p>构造时注入期望的 token，{@link #authenticate(String, String, String)} 将 {@code password} 参数作为 token
 * 凭证，验证其是否与期望 token 匹配。
 *
 * <p>比较经由 {@link SecureCredentialMatcher}：两侧先做 SHA-256 再常量时间比较。
 *
 * <p><b>为什么必须先摘要：</b>{@link MessageDigest#isEqual} 在长度不等时立即返回 false。若直接比较 token 原始字节，响应耗时会泄露 token
 * 的真实长度，形成长度预言机。摘要后比较输入恒为 32 字节， 该信息不再泄露，且等长输入下 isEqual 是常量时间的。
 *
 * <p><b>安全边界：</b>摘要不是口令加固，无盐 SHA-256 无法抵御预计算；真正的安全来自 token 本身 的熵值与配置保密。请使用足够长的高熵随机 token（建议 ≥ 32
 * 字节随机）。
 *
 * <p>当前实现不区分 resource，仅校验 token 是否匹配。 适用于使用单一共享 Token 的简单鉴权场景；如需细粒度权限控制，请自行扩展。
 *
 * <p><b>缺少的能力：</b>本实现不含失败重试限流。管理端点默认挂在主应用端口且不受 {@code management.endpoints.web.exposure.*}
 * 治理，请在网络层（安全组 / Ingress）限制访问来源。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TokenAuthenticator implements ManagementAuthenticator {

    private final String expectedToken;

    /**
     * 构造 Token 鉴权器。
     *
     * @param expectedToken 期望的 token 字符串，不能为 null
     */
    public TokenAuthenticator(String expectedToken) {
        this.expectedToken = Objects.requireNonNull(expectedToken, "expectedToken");
    }

    @Override
    public boolean authenticate(String username, String password, String resource) {
        if (Objects.isNull(password)) {
            return false;
        }
        return SecureCredentialMatcher.matches(expectedToken, password);
    }

    @Override
    public String name() {
        return "token";
    }
}
