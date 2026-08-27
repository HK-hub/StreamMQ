/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.annotation;

import java.lang.annotation.*;

/**
 * StreamMQ 启用注解，标注在 Spring Boot 启动类上显式声明使用 StreamMQ。
 *
 * <p>对齐 RocketMQ Spring Starter 的 {@code @EnableRocketMQ} 体验。
 *
 * <p><b>自动装配触发方式</b>：当 {@code streammq-spring-boot-starter} 在 classpath 时， Spring Boot 通过 {@code
 * META-INF/spring/AutoConfiguration.imports} 自动装配 {@code StreamMQAutoConfiguration}，无需手动
 * {@code @Import}。本注解作为显式标记，便于声明式表达「使用 StreamMQ」。 全局行为（追踪、扫描等）一律通过配置文件 {@code streammq.*}
 * 属性表达，本注解不携带属性。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * @SpringBootApplication
 * @EnableStreamMQ
 * public class OrderApplication {
 *     public static void main(String[] args) {
 *         SpringApplication.run(OrderApplication.class, args);
 *     }
 * }
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableStreamMQ {}
