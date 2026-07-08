package io.github.streammq.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * StreamMQ 测试基类，提供嵌入式 Redis 和 Redisson 客户端。
 *
 * <p>所有集成测试应继承此类，获得预配置的测试环境。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class StreamMQTestBase {

    protected static EmbeddedRedisServer redisServer;
    protected static RedissonClient redissonClient;

    @BeforeAll
    static void setupAll() {
        redisServer = new EmbeddedRedisServer();
        redisServer.start();

        Config config = new Config();
        config.useSingleServer()
                .setAddress(redisServer.getConnectionString())
                .setDatabase(0)
                .setConnectionPoolSize(8)
                .setConnectionMinimumIdleSize(2);

        redissonClient = Redisson.create(config);
    }

    @AfterAll
    static void teardownAll() {
        if (redissonClient != null) {
            redissonClient.shutdown();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    protected RedissonClient getRedissonClient() {
        return redissonClient;
    }

    protected String getRedisHost() {
        return redisServer.getHost();
    }

    protected int getRedisPort() {
        return redisServer.getPort();
    }

    protected String getRedisConnectionString() {
        return redisServer.getConnectionString();
    }

    protected void clearRedisData() {
        if (redissonClient != null) {
            redissonClient.getKeys().flushdb();
        }
    }
}