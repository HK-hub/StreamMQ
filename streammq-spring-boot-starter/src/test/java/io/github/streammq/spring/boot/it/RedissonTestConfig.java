package io.github.streammq.spring.boot.it;

import io.github.streammq.spring.boot.autoconfigure.StreamMqAutoConfiguration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * 集成测试共享配置：作为 {@code @SpringBootConfiguration} 启动 Spring 上下文,
 * 通过 {@link Import} 显式导入 {@link StreamMqAutoConfiguration} 触发完整装配链,
 * 并手动注册 {@link RedissonClient} Bean 连接本地 Redis。
 *
 * <p>由于 {@code redisson-spring-boot-starter} 在 starter 模块中为 {@code provided} 作用域,
 * 测试环境需手动注册 RedissonClient Bean。
 *
 * <p>不使用 {@code @EnableAutoConfiguration} 以避免与 {@code redisson-spring-boot-starter}
 * 的自动装配产生 Bean 冲突。
 */
@SpringBootConfiguration
@Import(StreamMqAutoConfiguration.class)
public class RedissonTestConfig {

    /**
     * 创建连接本地 Redis 的 RedissonClient。
     *
     * @return Redisson 客户端
     */
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        return Redisson.create(config);
    }
}
