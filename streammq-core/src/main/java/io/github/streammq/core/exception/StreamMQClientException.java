/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.exception;

/**
 * 客户端配置异常：外部配置项非法（{@code streammq.*} 属性越界、互斥配置冲突、必填配置缺失）等。
 *
 * <p>此类异常通常无法通过重试解决，需修复业务代码或配置。
 *
 * <p><b>与 JDK 异常的分工（0.1.2 起明确）：</b>方法参数 / 契约违反（Builder 入参、值对象构造、集合元素为 null 等）仍使用 {@link
 * IllegalArgumentException} / {@link NullPointerException}；本异常只用于"外部配置错误"这类 框架运行时错误，便于业务层统一 {@code
 * catch (StreamMQException)} 收敛处理。 首次真实使用点为 Spring Boot Starter 的 {@code
 * StreamMQProperties#validate()}（0.1.2）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQClientException extends StreamMQException {

    private static final long serialVersionUID = 1L;

    public StreamMQClientException(String message) {
        super(message);
    }

    public StreamMQClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
