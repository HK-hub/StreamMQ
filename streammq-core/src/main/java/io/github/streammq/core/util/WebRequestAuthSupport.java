/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;
import lombok.experimental.UtilityClass;

/**
 * Web 请求鉴权辅助工具，从当前请求上下文反射读取 {@code Authorization: Basic} 头与客户端地址。
 *
 * <p>为避免对 Servlet API / Spring Web 的编译期依赖，通过反射访问 {@code RequestContextHolder}； 非 Web 环境或缺少
 * spring-web 时返回 null（凭据为空，默认拒绝）。
 *
 * <p>同时被 {@code streammq-spring-boot-starter} 的 Actuator 管理端点与 {@code streammq-diagnostics}
 * 诊断端点复用，保证管理/诊断接口的鉴权语义一致。
 *
 * <h2>客户端地址可信模型（安全关键）</h2>
 *
 * <p>鉴权失败限流（{@code RateLimitedAuthenticator}）按来源地址聚合。客户端地址默认<b>仅取不可伪造的 {@code
 * remoteAddr}</b>——{@code X-Forwarded-For} 完全由客户端可控，未经可信代理校验就采用它， 攻击者每次换一个 XFF
 * 值即可让限流失去意义（且每造一个值就新增一个限流条目）。
 *
 * <p>仅当满足以下全部条件时才会采用 {@code X-Forwarded-For} 首值：
 *
 * <ol>
 *   <li>{@link #configure} 显式开启 {@code trustForwardedHeaders}（对应配置 {@code
 *       streammq.admin.trust-forwarded-headers=true}）
 *   <li>直连对端（{@code remoteAddr}）是回环地址，或命中 {@code streammq.admin.trusted-proxies} 配置的可信代理 CIDR 列表
 * </ol>
 *
 * <p>注意：启用 XFF 后，限流按"可信代理报告的原始客户端"聚合；若代理链不可信，仍可被伪造， 请仅在代理网络受控时开启。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@UtilityClass
public class WebRequestAuthSupport {

    /** spring-web RequestContextHolder 全限定名（编译期解耦的反射探测目标） */
    private static final String REQUEST_CONTEXT_HOLDER_CLASS_NAME =
            "org.springframework.web.context.request.RequestContextHolder";

    /** HTTP Authorization 请求头名称 */
    private static final String HEADER_AUTHORIZATION = "Authorization";

    /** Basic 鉴权 scheme 前缀 */
    private static final String BASIC_AUTH_PREFIX = "Basic ";

    /** X-Forwarded-For 请求头名称（反向代理场景下识别真实客户端地址） */
    private static final String HEADER_X_FORWARDED_FOR = "X-Forwarded-For";

    /**
     * 是否信任 {@code X-Forwarded-For} 请求头。默认 {@code false}：仅使用不可伪造的 {@code remoteAddr}。
     *
     * <p>安全默认值——伪造 XFF 让限流失效的历史问题由此修复（见类 Javadoc）。
     */
    private static volatile boolean trustForwardedHeaders = false;

    /**
     * 可信代理 CIDR 列表（IPv4/IPv6，如 {@code 10.0.0.0/8}、{@code 192.168.1.0/24}）。
     *
     * <p>仅当 {@link #trustForwardedHeaders} 为 true 时生效；空集表示只信任回环地址（{@code 127.0.0.1} / {@code ::1}）。
     */
    private static volatile Set<String> trustedProxyCidrs = Set.of();

    /**
     * 配置客户端地址解析策略（由 Spring Boot 装配在上下文刷新时调用）。
     *
     * @param trustForwardedHeaders 是否信任 {@code X-Forwarded-For}（对应 {@code
     *     streammq.admin.trust-forwarded-headers}，默认 false）
     * @param trustedProxyCidrs 可信代理 CIDR 列表（对应 {@code streammq.admin.trusted-proxies}， 可为 null/空）
     */
    public static synchronized void configure(
            boolean trustForwardedHeaders, java.util.Collection<String> trustedProxyCidrs) {
        WebRequestAuthSupport.trustForwardedHeaders = trustForwardedHeaders;
        WebRequestAuthSupport.trustedProxyCidrs =
                trustedProxyCidrs == null ? Set.of() : Set.copyOf(trustedProxyCidrs);
    }

    /** 当前是否信任 {@code X-Forwarded-For}（供诊断端点/启动日志展示）。 */
    public static boolean isTrustForwardedHeaders() {
        return trustForwardedHeaders;
    }

    /** 当前可信代理 CIDR 集合（不可修改）。 */
    public static Set<String> getTrustedProxyCidrs() {
        return trustedProxyCidrs;
    }

    /**
     * 从当前请求上下文中反射读取 {@code Authorization: Basic} 头，返回 {@code [user, pass]} 或 null。
     *
     * <p>非 Web 环境、缺少 spring-web、或请求头非 Basic 格式时返回 null。
     *
     * @return 凭据数组 {@code [user, pass]}，无法获取时为 null
     */
    public static String[] parseBasicCredentialsFromRequest() {
        Object request = currentRequest();
        if (request == null) {
            return null;
        }
        String authorizationHeader = getHeader(request, HEADER_AUTHORIZATION);
        return parseBasicCredentials(authorizationHeader);
    }

    /**
     * 从当前请求上下文反射读取客户端地址，用于鉴权失败限流的来源聚合。
     *
     * <p><b>可信规则：</b>
     *
     * <ul>
     *   <li>{@code trustForwardedHeaders=false}（默认）：忽略 {@code X-Forwarded-For}，直接返回不可伪造的 {@code
     *       remoteAddr}——反代场景下所有客户端共享代理 IP，限流按代理 IP 聚合（安全优先，宁可误伤不可绕过）
     *   <li>{@code trustForwardedHeaders=true} 且直连对端命中可信代理（回环或 {@code trusted-proxies} CIDR）： 返回
     *       {@code X-Forwarded-For} 首值（真实客户端）
     *   <li>非 Web 环境或读取失败：返回 null（由调用方退化为全局计数）
     * </ul>
     *
     * @return 客户端地址，无法获取时为 null
     */
    public static String getClientAddressFromRequest() {
        Object request = currentRequest();
        if (request == null) {
            return null;
        }
        String remoteAddr = getRemoteAddr(request);
        if (trustForwardedHeaders && isTrustedPeer(remoteAddr)) {
            String forwarded = getHeader(request, HEADER_X_FORWARDED_FOR);
            if (StringUtils.isNotEmpty(forwarded)) {
                int comma = forwarded.indexOf(',');
                String candidate =
                        comma < 0 ? forwarded.trim() : forwarded.substring(0, comma).trim();
                if (StringUtils.isNotEmpty(candidate)) {
                    return candidate;
                }
            }
        }
        return remoteAddr;
    }

    /**
     * 解析 {@code Authorization: Basic base64(user:pass)} 头，返回 {@code [user, pass]} 或 null。
     *
     * @param authorizationHeader Authorization 请求头，可为 null
     * @return 凭据数组 {@code [user, pass]}，无法解析时为 null
     */
    public static String[] parseBasicCredentials(String authorizationHeader) {
        if (StringUtils.isEmpty(authorizationHeader)
                || !authorizationHeader.regionMatches(
                        true, 0, BASIC_AUTH_PREFIX, 0, BASIC_AUTH_PREFIX.length())) {
            return null;
        }
        try {
            String encoded = authorizationHeader.substring(BASIC_AUTH_PREFIX.length()).trim();
            byte[] decoded = Base64.getDecoder().decode(encoded);
            String decodedStr = new String(decoded, StandardCharsets.UTF_8);
            int idx = decodedStr.indexOf(':');
            if (idx < 0) {
                return null;
            }
            return new String[] {decodedStr.substring(0, idx), decodedStr.substring(idx + 1)};
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    /** 判断直连对端是否为可信代理（回环地址或命中可信 CIDR）。 */
    private static boolean isTrustedPeer(String remoteAddr) {
        if (StringUtils.isEmpty(remoteAddr)) {
            return false;
        }
        if (isLoopback(remoteAddr)) {
            return true;
        }
        for (String cidr : trustedProxyCidrs) {
            if (matchesCidr(remoteAddr, cidr)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLoopback(String addr) {
        try {
            return InetAddress.getByName(addr).isLoopbackAddress();
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * 校验 CIDR 表达式的合法性（如 {@code 10.0.0.0/8}、{@code 2001:db8::/32}）。
     *
     * <p>供配置校验使用：地址可解析、前缀在合法范围内即视为合法。非法时返回 false。
     *
     * @param cidr 待校验的 CIDR 表达式
     * @return true 表示合法
     */
    public static boolean isValidCidr(String cidr) {
        if (cidr == null || cidr.isBlank()) {
            return false;
        }
        try {
            String[] parts = cidr.trim().split("/", -1);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                return false;
            }
            byte[] network = InetAddress.getByName(parts[0]).getAddress();
            int prefix = Integer.parseInt(parts[1]);
            return prefix >= 0 && prefix <= network.length * 8;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * IPv4/IPv6 CIDR 匹配（如 {@code 10.0.0.0/8}、{@code 2001:db8::/32}）。
     *
     * <p>不匹配（非法 CIDR、地址族不一致、解析失败）一律返回 false——安全方向 fail-closed。
     */
    static boolean matchesCidr(String ip, String cidr) {
        try {
            InetAddress addr = InetAddress.getByName(ip);
            String[] parts = cidr.trim().split("/", -1);
            if (parts.length != 2 || parts[0].isBlank()) {
                return false;
            }
            InetAddress network = InetAddress.getByName(parts[0]);
            byte[] a = addr.getAddress();
            byte[] n = network.getAddress();
            if (a.length != n.length) {
                return false; // IPv4 与 IPv6 不可混配
            }
            int prefix = Integer.parseInt(parts[1]);
            if (prefix < 0 || prefix > a.length * 8) {
                return false;
            }
            int fullBytes = prefix / 8;
            int remainderBits = prefix % 8;
            for (int i = 0; i < fullBytes; i++) {
                if (a[i] != n[i]) {
                    return false;
                }
            }
            if (remainderBits > 0) {
                int mask = (0xFF << (8 - remainderBits)) & 0xFF;
                if ((a[fullBytes] & mask) != (n[fullBytes] & mask)) {
                    return false;
                }
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * 反射获取当前请求对象。
     *
     * <p><b>线程上下文限制：</b>该实现依赖 {@code RequestContextHolder} 的 ThreadLocal——异步请求处理 （{@code
     * spring.mvc.async}、返回 {@code CompletableFuture} 等）切换线程后取不到当前请求，凭据为 null（fail-closed
     * 拒绝）。如需在异步场景使用，请实现自定义 {@code ManagementAuthenticator} 从安全上下文（如 Spring Security {@code
     * SecurityContextHolder}）取凭据。
     *
     * @return 当前请求对象，非 Web 环境或缺少 spring-web 时为 null
     */
    private static Object currentRequest() {
        Object attrs;
        try {
            Class<?> holder = Class.forName(REQUEST_CONTEXT_HOLDER_CLASS_NAME);
            attrs = holder.getMethod("getRequestAttributes").invoke(null);
        } catch (ReflectiveOperationException | LinkageError ex) {
            // 非 Web 环境：无可用的请求上下文
            return null;
        }
        if (attrs == null) {
            return null;
        }
        try {
            return attrs.getClass().getMethod("getRequest").invoke(attrs);
        } catch (ReflectiveOperationException ex) {
            return null;
        }
    }

    /** 反射读取直连对端地址，失败返回 null。 */
    private static String getRemoteAddr(Object request) {
        try {
            Method getRemoteAddr = request.getClass().getMethod("getRemoteAddr");
            Object addr = getRemoteAddr.invoke(request);
            return addr instanceof String s && StringUtils.isNotEmpty(s) ? s : null;
        } catch (ReflectiveOperationException ex) {
            return null;
        }
    }

    /**
     * 反射读取请求头。
     *
     * @param request 请求对象
     * @param name 请求头名称
     * @return 请求头值，读取失败时为 null
     */
    private static String getHeader(Object request, String name) {
        try {
            Method getHeader = request.getClass().getMethod("getHeader", String.class);
            Object value = getHeader.invoke(request, name);
            return value instanceof String s ? s : null;
        } catch (ReflectiveOperationException ex) {
            return null;
        }
    }
}
