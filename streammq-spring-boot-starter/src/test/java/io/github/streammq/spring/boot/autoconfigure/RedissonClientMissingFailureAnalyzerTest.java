/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * {@link RedissonClientMissingFailureAnalyzer} 回归守卫。
 *
 * <p>锁定历史缺陷：实现此前写成 {@code REDISSON_CLIENT_CLASS.equals(cause.getBeanType())}，而 {@code
 * getBeanType()} 返回 {@code Class<?>} —— {@code String.equals(Class)} <b>恒为 false</b>， 于是 {@code
 * analyze()} 永远返回 {@code null}，这个"把裸 NoSuchBeanDefinitionException 换成含依赖声明与 配置示例的可操作报告"的 DX
 * 特性<b>从未生效</b>。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("RedissonClientMissingFailureAnalyzer")
class RedissonClientMissingFailureAnalyzerTest {

    private final RedissonClientMissingFailureAnalyzer analyzer =
            new RedissonClientMissingFailureAnalyzer();

    @Test
    @DisplayName("缺少 RedissonClient Bean 时给出可操作的失败报告")
    void analyzesMissingRedissonClient() {
        NoSuchBeanDefinitionException cause =
                new NoSuchBeanDefinitionException(RedissonClient.class);

        FailureAnalysis analysis = analyzer.analyze(cause, cause);

        assertThat(analysis).isNotNull();
        assertThat(analysis.getDescription()).contains("RedissonClient");
        assertThat(analysis.getAction())
                .contains("redisson-spring-boot-starter")
                .contains("spring");
        assertThat(analysis.getCause()).isSameAs(cause);
    }

    @Test
    @DisplayName("缺少其它类型 Bean 时保持沉默，不劫持无关的启动失败")
    void ignoresUnrelatedBeanType() {
        NoSuchBeanDefinitionException other = new NoSuchBeanDefinitionException(String.class);
        assertThat(analyzer.analyze(other, other)).isNull();
    }

    @Test
    @DisplayName("beanType 为 null 时不抛异常也不误报")
    void handlesNullBeanType() {
        NoSuchBeanDefinitionException cause = new NoSuchBeanDefinitionException("someBeanName");
        assertThat(analyzer.analyze(cause, cause)).isNull();
    }
}
