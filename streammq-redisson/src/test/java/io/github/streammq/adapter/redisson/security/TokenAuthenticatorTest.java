/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * {@link TokenAuthenticator} 单元测试。
 *
 * <p>除常规匹配语义外，重点覆盖长度相关的回归：实现曾直接对原始字节调用 {@code MessageDigest.isEqual}，
 * 而该方法在长度不等时<b>立即返回</b>，会形成长度预言机。现已改为先摘要归一化长度再比较。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
@DisplayName("TokenAuthenticator Token 鉴权器测试")
class TokenAuthenticatorTest {

    private final TokenAuthenticator authenticator = new TokenAuthenticator("streammq-demo-token");

    @Nested
    @DisplayName("匹配语义")
    class Matching {

        @Test
        @DisplayName("token 匹配返回 true")
        void matched() {
            assertThat(authenticator.authenticate(null, "streammq-demo-token", "topic:t1"))
                    .isTrue();
        }

        @Test
        @DisplayName("token 匹配时 username 与 resource 不参与校验")
        void matchedIgnoresUsernameAndResource() {
            assertThat(authenticator.authenticate("anyone", "streammq-demo-token", null)).isTrue();
            assertThat(authenticator.authenticate(null, "streammq-demo-token", "dlq:g1")).isTrue();
        }

        @Test
        @DisplayName("token 完全不匹配返回 false")
        void mismatched() {
            assertThat(authenticator.authenticate(null, "wrong-token", "topic:t1")).isFalse();
        }

        @Test
        @DisplayName("password 为 null 返回 false")
        void nullPassword() {
            assertThat(authenticator.authenticate(null, null, "topic:t1")).isFalse();
        }
    }

    @Nested
    @DisplayName("长度预言机防护（回归）")
    class LengthOracleRegression {

        @Test
        @DisplayName("长度不同但前缀相同返回 false")
        void differentLengthSamePrefix() {
            // 期望 token 的前缀，长度更短
            assertThat(authenticator.authenticate(null, "streammq", "topic:t1")).isFalse();
            // 期望 token + 追加字符，长度更长
            assertThat(authenticator.authenticate(null, "streammq-demo-token-x", "topic:t1"))
                    .isFalse();
        }

        @Test
        @DisplayName("长度相同的错误 token 返回 false（不因摘要归一化而误判为相等）")
        void sameLengthWrongToken() {
            String sameLengthWrong = "streammq-demo-tokeX";
            assertThat(sameLengthWrong).hasSameSizeAs("streammq-demo-token");
            assertThat(authenticator.authenticate(null, sameLengthWrong, "topic:t1")).isFalse();
        }

        @Test
        @DisplayName("空 token 与超长 token 均安全返回 false")
        void emptyAndOversized() {
            assertThat(authenticator.authenticate(null, "", "topic:t1")).isFalse();
            assertThat(authenticator.authenticate(null, "x".repeat(10_000), "topic:t1")).isFalse();
        }
    }

    @Nested
    @DisplayName("构造校验")
    class Construction {

        @Test
        @DisplayName("expectedToken 为 null 抛出 NullPointerException")
        void nullToken() {
            assertThatThrownBy(() -> new TokenAuthenticator(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("expectedToken");
        }

        @Test
        @DisplayName("name 返回 token")
        void name() {
            assertThat(authenticator.name()).isEqualTo("token");
        }
    }
}
