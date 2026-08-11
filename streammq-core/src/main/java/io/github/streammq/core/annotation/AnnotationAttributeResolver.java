package io.github.streammq.core.annotation;

/**
 * 注解属性解析器接口，支持 ${...} 属性占位符和 #{...} SpEL 表达式。
 *
 * <p>用于解析 {@code @StreamMQConsumer}、{@code @StreamMQTransactionConsumer} 等注解中的
 * topic、consumerGroup、namespace、selectorExpression 等字符串属性， 使其支持通过配置文件动态指定。
 *
 * <p>接口位于 core 模块，便于 starter 与适配器层提供不同实现 （如基于 Spring {@code ConfigurableApplicationContext} 的实现）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface AnnotationAttributeResolver {

    /**
     * 解析属性值，支持 ${...} 占位符和 #{...} SpEL 表达式。
     *
     * @param value 原始值（可能包含 {@code ${}} 或 {@code #{}} 表达式）
     * @return 解析后的值；若入参为 {@code null} 或空串则原样返回
     */
    String resolve(String value);
}
