/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMQConstants;
import java.lang.annotation.*;

/**
 * StreamMQ 启用注解，标注在 Spring Boot 启动类上显式声明使用 StreamMQ。
 *
 * <p>对齐 RocketMQ Spring Starter 的 {@code @EnableRocketMQ} 体验。
 *
 * <p><b>自动装配触发方式</b>：当 {@code streammq-spring-boot-starter} 在 classpath 时， Spring Boot 通过 {@code
 * META-INF/spring/AutoConfiguration.imports} 自动装配 {@code StreamMQAutoConfiguration}，无需手动
 * {@code @Import}。本注解作为显式标记，便于声明式表达「使用 StreamMQ」；其属性当前为预留项， 未参与装配行为（0.1.0 阶段）。
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
public @interface EnableStreamMQ {

    /**
     * 启用模式，默认 {@link StreamMQConstants#MODE_STANDARD}。
     *
     * <p><b>预留属性（0.1.0 未生效）</b>：{@code MODE_LITE} 轻量模式规划中，当前任何取值均不改变装配行为。
     *
     * @return 模式字符串
     */
    String mode() default StreamMQConstants.MODE_STANDARD;

    /**
     * 全局追踪开关，默认 false。
     *
     * <p><b>预留属性（0.1.0 未生效）</b>：追踪请通过配置文件 {@code streammq.tracing.enabled=true} 或 引入 {@code
     * streammq-tracing-opentelemetry} 模块启用，本属性当前不注册任何追踪组件。
     *
     * @return true 启用追踪
     */
    boolean tracingEnabled() default false;

    /**
     * 自定义扫描包路径（默认使用 Spring Boot 启动类所在包）。
     *
     * <p><b>预留属性（0.1.0 未生效）</b>：消费者扫描基于 Spring Bean 发现机制，无需指定包路径； 本属性当前不参与扫描行为。
     *
     * @return 扫描包路径数组
     */
    String[] scanBasePackages() default {};
}
