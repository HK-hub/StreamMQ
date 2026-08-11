package io.github.streammq.spring.boot.support;

import io.github.streammq.core.annotation.AnnotationAttributeResolver;
import io.github.streammq.core.util.StringUtils;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

/**
 * 基于 Spring {@link ConfigurableApplicationContext} 的注解属性解析器实现， 支持 ${...} 属性占位符和 #{...} SpEL 表达式。
 *
 * <p>用于解析 {@code @StreamMQConsumer}、{@code @StreamMQTransactionConsumer} 等注解中的
 * topic、consumerGroup、namespace、selectorExpression 等字符串属性， 使其支持通过配置文件动态指定。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class SpringAnnotationAttributeResolver implements AnnotationAttributeResolver {
    private final ConfigurableBeanFactory beanFactory;
    private final Environment environment;

    public SpringAnnotationAttributeResolver(ConfigurableApplicationContext applicationContext) {
        this.beanFactory = applicationContext.getBeanFactory();
        this.environment = applicationContext.getEnvironment();
    }

    /**
     * 解析属性值，支持 ${...} 占位符和 #{...} SpEL 表达式。
     *
     * @param value 原始值（可能包含 {@code ${}} 或 {@code #{}} 表达式）
     * @return 解析后的值；若入参为 {@code null} 或空串则原样返回
     */
    @Override
    public String resolve(String value) {
        if (StringUtils.isEmpty(value)) {
            return value;
        }
        // 先解析 ${} 占位符
        String resolved = environment.resolvePlaceholders(value);
        // 再解析 #{} SpEL 表达式
        if (resolved.contains("#{")) {
            BeanExpressionResolver resolver = beanFactory.getBeanExpressionResolver();
            if (resolver != null) {
                BeanExpressionContext bec = new BeanExpressionContext(beanFactory, null);
                Object result = resolver.evaluate(resolved, bec);
                if (result != null) {
                    resolved = result.toString();
                }
            }
        }
        return resolved;
    }
}
