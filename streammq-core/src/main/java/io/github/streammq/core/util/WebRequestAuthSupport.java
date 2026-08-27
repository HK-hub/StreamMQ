/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.experimental.UtilityClass;

/**
 * Web 请求鉴权辅助工具，从当前请求上下文反射读取 {@code Authorization: Basic} 头。
 *
 * <p>为避免对 Servlet API / Spring Web 的编译期依赖，通过反射访问 {@code RequestContextHolder}； 非 Web 环境或缺少
 * spring-web 时返回 null（凭据为空，默认拒绝）。
 *
 * <p>同时被 {@code streammq-spring-boot-starter} 的 Actuator 管理端点与 {@code streammq-diagnostics}
 * 诊断端点复用，保证管理/诊断接口的鉴权语义一致。
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

    /**
     * 从当前请求上下文中反射读取 {@code Authorization: Basic} 头，返回 {@code [user, pass]} 或 null。
     *
     * <p>非 Web 环境、缺少 spring-web、或请求头非 Basic 格式时返回 null。
     *
     * @return 凭据数组 {@code [user, pass]}，无法获取时为 null
     */
    public static String[] parseBasicCredentialsFromRequest() {
        Object attrs = null;
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
        Object request = null;
        try {
            request = attrs.getClass().getMethod("getRequest").invoke(attrs);
        } catch (ReflectiveOperationException ex) {
            return null;
        }
        if (request == null) {
            return null;
        }
        String authorizationHeader = null;
        try {
            Method getHeader = request.getClass().getMethod("getHeader", String.class);
            authorizationHeader = (String) getHeader.invoke(request, HEADER_AUTHORIZATION);
        } catch (ReflectiveOperationException ex) {
            return null;
        }
        return parseBasicCredentials(authorizationHeader);
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
}
