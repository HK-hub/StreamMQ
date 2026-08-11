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
 * {@code @Import}。 本注解作为显式标记与配置属性载体（{@link #mode()} / {@link #tracingEnabled()} / {@link
 * #scanBasePackages()}），供 starter 中的 {@code StreamMQListenerRegistrar} 等组件读取以调整行为。
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
   * 启用模式，默认 {@link StreamMQConstants#MODE_STANDARD}。 v1.0+ 支持 {@link
   * StreamMQConstants#MODE_LITE}（轻量模式，不启用 Actuator 指标）。
   *
   * @return 模式字符串
   */
  String mode() default StreamMQConstants.MODE_STANDARD;

  /**
   * 全局追踪开关，默认 false。 设置为 true 时启用全局消息追踪（注册 Slf4jTraceCollector 与追踪拦截器）。
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
