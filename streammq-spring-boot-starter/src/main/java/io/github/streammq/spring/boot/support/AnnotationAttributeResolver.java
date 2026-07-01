package io.github.streammq.spring.boot.support;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

/**
 * 注解属性解析器，支持 ${...} 属性占位符和 #{...} SpEL 表达式。
 *
 * <p>用于解析 @StreamMqListener、@StreamMqProducer 等注解中的
 * topic、consumerGroup、group、namespace、selectorExpression 等字符串属性，
 * 使其支持通过配置文件动态指定。
 */
public class AnnotationAttributeResolver {
    private final ConfigurableBeanFactory beanFactory;
    private final Environment environment;

    public AnnotationAttributeResolver(ConfigurableApplicationContext applicationContext) {
        this.beanFactory = applicationContext.getBeanFactory();
        this.environment = applicationContext.getEnvironment();
    }

    /**
     * 解析属性值，支持 ${...} 占位符和 #{...} SpEL 表达式。
     *
     * @param value 原始值（可能包含 ${} 或 #{} 表达式）
     * @return 解析后的值
     */
    public String resolve(String value) {
        if (value == null || value.isEmpty()) {
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
