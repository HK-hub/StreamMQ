/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 凭据比较工具（包内私有，不对外暴露 API）。
 *
 * <p><b>为什么必须先做摘要再比较：</b>{@link MessageDigest#isEqual(byte[], byte[])} 在长度不等时 会<b>立即返回
 * false</b>，因此直接比较原始字节会泄露真实长度——攻击者可据此逐字节猜测 token 长度， 缩小爆破空间。对两侧都做 SHA-256 之后，比较的输入恒为 32
 * 字节，长度维度不再泄露信息， 且 isEqual 对等长输入是常量时间的。
 *
 * <p><b>它不做什么：</b>摘要在这里<b>不是</b>密码加固。两端都是配置里的明文凭据，无盐 SHA-256
 * 无法抵御预计算/彩虹表。真正的防护来自"配置值本身的保密"（环境变量、密钥管理服务）。 摘要的唯一目的是长度归一化，不要把它误当作口令散列方案。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
final class SecureCredentialMatcher {

    private static final String DIGEST_ALGORITHM = "SHA-256";

    private SecureCredentialMatcher() {}

    /**
     * 常量时间比较两个字符串（先摘要归一化长度）。
     *
     * @param expected 期望值
     * @param actual 实际值
     * @return true 表示相等；任一侧为 null 时返回 false
     */
    static boolean matches(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(sha256(expected), sha256(actual));
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM)
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException ex) {
            // SHA-256 是 JCA 强制实现的标准算法，此处仅为受检异常兜底
            throw new IllegalStateException(DIGEST_ALGORITHM + " unavailable", ex);
        }
    }
}
