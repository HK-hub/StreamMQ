package io.github.streammq.core.annotation;

import io.github.streammq.core.StreamMqConstants;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * StreamMQ 启用注解，标注在 Spring Boot 启动类上触发自动装配。
 *
 * <p>对齐 RocketMQ Spring Starter 的 {@code @EnableRocketMQ} 体验。
 * 通过 {@code @Import(StreamMqAutoConfiguration.class)} 触发装配。
 *
 * <p>使用示例：
 * <pre>{@code
 * @SpringBootApplication
 * @EnableStreamMq
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
public @interface EnableStreamMq {

    /**
     * 启用模式，默认 {@link StreamMqConstants#MODE_STANDARD}。
     * v1.0+ 支持 {@link StreamMqConstants#MODE_LITE}（轻量模式，不启用 Actuator 指标）。
     *
     * @return 模式字符串
     */
    String mode() default StreamMqConstants.MODE_STANDARD;

    /**
     * 全局追踪开关，默认 false。
     * 设置为 true 时启用全局消息追踪（注册 Slf4jTraceCollector 与追踪拦截器）。
     *
     * @return true 启用追踪
     */
    boolean tracingEnabled() default false;

    /**
     * 自定义扫描包路径（默认使用 Spring Boot 启动类所在包）。
     *
     * @return 扫描包路径数组
     */
    String[] scanBasePackages() default {};
}
