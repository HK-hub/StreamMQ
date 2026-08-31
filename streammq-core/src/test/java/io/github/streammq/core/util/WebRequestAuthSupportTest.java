/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link WebRequestAuthSupport} 单元测试。
 *
 * <p>覆盖发布前安全修复的可测纯函数：Basic 凭据解析、CIDR 合法性校验与匹配（IPv4/IPv6、前缀边界、 地址族隔离、fail-closed 语义），以及 {@code
 * X-Forwarded-For} 可信策略的默认 fail-closed 行为。
 */
@DisplayName("Web 请求鉴权辅助工具测试")
class WebRequestAuthSupportTest {

    // ===================== Basic 凭据解析 =====================

    @Test
    @DisplayName("解析合法 Basic 头返回 [user, pass]")
    void parseBasicCredentials_validHeader() {
        String header =
                "Basic " + java.util.Base64.getEncoder().encodeToString("admin:s3cret".getBytes());
        assertThat(WebRequestAuthSupport.parseBasicCredentials(header))
                .containsExactly("admin", "s3cret");
    }

    @Test
    @DisplayName("非 Basic scheme、null、空串一律返回 null")
    void parseBasicCredentials_invalidHeader_returnsNull() {
        assertThat(WebRequestAuthSupport.parseBasicCredentials(null)).isNull();
        assertThat(WebRequestAuthSupport.parseBasicCredentials("")).isNull();
        assertThat(WebRequestAuthSupport.parseBasicCredentials("Bearer abc")).isNull();
        // scheme 大小写不敏感但必须有 Basic 前缀
        assertThat(WebRequestAuthSupport.parseBasicCredentials("basic x")).isNull();
    }

    @Test
    @DisplayName("非法 base64 与缺冒号返回 null")
    void parseBasicCredentials_garbage_returnsNull() {
        assertThat(WebRequestAuthSupport.parseBasicCredentials("Basic !!!not-base64!!!")).isNull();
        assertThat(
                        WebRequestAuthSupport.parseBasicCredentials(
                                "Basic "
                                        + java.util.Base64.getEncoder()
                                                .encodeToString("no-colon".getBytes())))
                .isNull();
    }

    // ===================== CIDR 合法性校验 =====================

    @Test
    @DisplayName("合法 IPv4/IPv6 CIDR 通过校验")
    void isValidCidr_valid() {
        assertThat(WebRequestAuthSupport.isValidCidr("10.0.0.0/8")).isTrue();
        assertThat(WebRequestAuthSupport.isValidCidr("192.168.1.0/24")).isTrue();
        assertThat(WebRequestAuthSupport.isValidCidr("0.0.0.0/0")).isTrue();
        assertThat(WebRequestAuthSupport.isValidCidr("255.255.255.255/32")).isTrue();
        assertThat(WebRequestAuthSupport.isValidCidr("2001:db8::/32")).isTrue();
        assertThat(WebRequestAuthSupport.isValidCidr("::1/128")).isTrue();
        assertThat(WebRequestAuthSupport.isValidCidr("::/0")).isTrue();
    }

    @Test
    @DisplayName("非法 CIDR 一律返回 false（含 null/空/缺前缀/前缀越界/非法地址）")
    void isValidCidr_invalid_returnsFalse() {
        assertThat(WebRequestAuthSupport.isValidCidr(null)).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("  ")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("10.0.0.0")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("10.0.0.0/")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("/8")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("10.0.0.0/33")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("10.0.0.0/-1")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("10.0.0.0/abc")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("999.1.1.1/8")).isFalse();
        assertThat(WebRequestAuthSupport.isValidCidr("2001:db8::/129")).isFalse();
    }

    // ===================== CIDR 匹配 =====================

    @Test
    @DisplayName("IPv4 CIDR 命中/未命中")
    void matchesCidr_ipv4() {
        assertThat(WebRequestAuthSupport.matchesCidr("10.1.2.3", "10.0.0.0/8")).isTrue();
        assertThat(WebRequestAuthSupport.matchesCidr("192.168.1.99", "192.168.1.0/24")).isTrue();
        assertThat(WebRequestAuthSupport.matchesCidr("192.168.2.1", "192.168.1.0/24")).isFalse();
        // 前缀边界：/32 精确匹配
        assertThat(WebRequestAuthSupport.matchesCidr("10.0.0.1", "10.0.0.1/32")).isTrue();
        assertThat(WebRequestAuthSupport.matchesCidr("10.0.0.2", "10.0.0.1/32")).isFalse();
        // /0 全命中
        assertThat(WebRequestAuthSupport.matchesCidr("1.2.3.4", "0.0.0.0/0")).isTrue();
    }

    @Test
    @DisplayName("IPv6 CIDR 命中/未命中")
    void matchesCidr_ipv6() {
        assertThat(WebRequestAuthSupport.matchesCidr("2001:db8::1", "2001:db8::/32")).isTrue();
        assertThat(WebRequestAuthSupport.matchesCidr("2001:db9::1", "2001:db8::/32")).isFalse();
        assertThat(WebRequestAuthSupport.matchesCidr("::1", "::1/128")).isTrue();
        assertThat(WebRequestAuthSupport.matchesCidr("::2", "::1/128")).isFalse();
    }

    @Test
    @DisplayName("地址族不一致、非法输入 fail-closed 返回 false")
    void matchesCidr_failClosed() {
        assertThat(WebRequestAuthSupport.matchesCidr("10.0.0.1", "2001:db8::/32")).isFalse();
        assertThat(WebRequestAuthSupport.matchesCidr("2001:db8::1", "10.0.0.0/8")).isFalse();
        assertThat(WebRequestAuthSupport.matchesCidr(null, "10.0.0.0/8")).isFalse();
        assertThat(WebRequestAuthSupport.matchesCidr("not-an-ip", "10.0.0.0/8")).isFalse();
        assertThat(WebRequestAuthSupport.matchesCidr("10.0.0.1", "10.0.0.0")).isFalse();
        assertThat(WebRequestAuthSupport.matchesCidr("10.0.0.1", "10.0.0.0/33")).isFalse();
    }

    // ===================== XFF 可信策略 =====================

    @Test
    @DisplayName("默认不信任 X-Forwarded-For（安全默认值）")
    void xff_trustOffByDefault() {
        // 每次测试前复位为默认值，避免类级静态状态串扰
        WebRequestAuthSupport.configure(false, List.of());
        assertThat(WebRequestAuthSupport.isTrustForwardedHeaders()).isFalse();
        assertThat(WebRequestAuthSupport.getTrustedProxyCidrs()).isEmpty();
    }

    @Test
    @DisplayName("configure 显式开启后状态可读且 CIDR 集合不可变")
    void xff_configureReflectsState() {
        WebRequestAuthSupport.configure(true, List.of("10.0.0.0/8", "2001:db8::/32"));
        assertThat(WebRequestAuthSupport.isTrustForwardedHeaders()).isTrue();
        assertThat(WebRequestAuthSupport.getTrustedProxyCidrs())
                .containsExactlyInAnyOrder("10.0.0.0/8", "2001:db8::/32");

        // null 入参归一为空集合
        WebRequestAuthSupport.configure(true, null);
        assertThat(WebRequestAuthSupport.getTrustedProxyCidrs()).isEmpty();

        // 复位为默认，避免影响其他测试
        WebRequestAuthSupport.configure(false, List.of());
    }

    @Test
    @DisplayName("非 Web 环境读取请求上下文返回 null（fail-closed 拒绝）")
    void requestScoped_readersFailClosedWithoutWebContext() {
        // core 模块无 spring-web 依赖：currentRequest() 反射探测失败返回 null
        assertThat(WebRequestAuthSupport.parseBasicCredentialsFromRequest()).isNull();
        assertThat(WebRequestAuthSupport.getClientAddressFromRequest()).isNull();
    }
}
