/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * 将 {@code NoSuchBeanDefinitionException: RedissonClient} 转换为可操作的启动失败描述。
 *
 * <p><b>为什么需要它：</b>{@code streammq-redisson} 把 {@code redisson} 声明为 {@code provided} scope
 * ——这是有意的（避免把 Redis 客户端版本强加给使用方），但副作用是：只引入 {@code streammq-spring-boot-starter}
 * 的用户会在启动时得到一条不含任何修复线索的 {@code NoSuchBeanDefinitionException}，而真正的原因只是"少加了一个依赖"。这是新用户最容易踩的坑，
 * 且错误信息本身完全无法自解释。
 *
 * <p>本分析器把它替换为明确的依赖声明 + 配置示例，把"第一次上手的 5 分钟"还给使用者。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RedissonClientMissingFailureAnalyzer
        extends AbstractFailureAnalyzer<NoSuchBeanDefinitionException> {

    private static final String REDISSON_CLIENT_CLASS = "org.redisson.api.RedissonClient";

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, NoSuchBeanDefinitionException cause) {
        if (!REDISSON_CLIENT_CLASS.equals(cause.getBeanType())) {
            return null;
        }
        String description =
                "StreamMQ requires a RedissonClient bean, but none was found in the application"
                        + " context.";
        String action =
                "Add the Redisson Spring Boot starter to your dependencies. StreamMQ declares\n"
                        + "redisson as a 'provided' dependency on purpose, so that you stay in"
                        + " control of the\n"
                        + "Redis client version — this means you must add it yourself:\n\n"
                        + "    <dependency>\n"
                        + "        <groupId>org.redisson</groupId>\n"
                        + "        <artifactId>redisson-spring-boot-starter</artifactId>\n"
                        + "    </dependency>\n\n"
                        + "(the version is managed by io.github.streammq:streammq-bom)\n\n"
                        + "Then configure the Redis address, for example:\n\n"
                        + "    spring:\n"
                        + "      data:\n"
                        + "        redis:\n"
                        + "          host: localhost\n"
                        + "          port: 6379\n\n"
                        + "Alternatively register your own RedissonClient @Bean, or set"
                        + " streammq.enabled=false to\ndisable StreamMQ auto-configuration"
                        + " entirely.";
        return new FailureAnalysis(description, action, cause);
    }
}
