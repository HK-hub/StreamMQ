package io.github.streammq.spring.boot.it;

import io.github.streammq.spring.boot.autoconfigure.StreamMQAutoConfiguration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * 集成测试共享配置：作为 {@code @SpringBootConfiguration} 启动 Spring 上下文, 通过 {@link Import} 显式导入 {@link
 * StreamMQAutoConfiguration} 触发完整装配链, 并手动注册 {@link RedissonClient} Bean 连接本地 Redis。
 *
 * <p>由于 {@code redisson-spring-boot-starter} 在 starter 模块中为 {@code provided} 作用域, 测试环境需手动注册
 * RedissonClient Bean。
 *
 * <p>不使用 {@code @EnableAutoConfiguration} 以避免与 {@code redisson-spring-boot-starter} 的自动装配产生 Bean
 * 冲突。
 *
 * <p>使用 {@link StringCodec} 作为默认编解码器，避免与 Lua 脚本交互时的 Kryo 反序列化问题。
 */
@SpringBootConfiguration
@Import(StreamMQAutoConfiguration.class)
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
    // 使用 StringCodec 避免 Kryo 反序列化问题（与 Lua 脚本交互时）
    config.setCodec(StringCodec.INSTANCE);
    return Redisson.create(config);
  }
}
