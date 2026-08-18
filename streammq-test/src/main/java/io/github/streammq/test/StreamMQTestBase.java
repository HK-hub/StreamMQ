package io.github.streammq.test;

import io.github.streammq.core.util.RedisAvailability;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamMQ 测试基类，提供 Redis 连接支持。
 *
 * <p>支持两种模式：
 *
 * <ul>
 *   <li><b>本地 Redis（默认）</b>：通过系统属性 {@code streammq.test.redis.host} / {@code
 *       streammq.test.redis.port} 指定本地 Redis 地址（默认 localhost:6379）。
 *   <li><b>Docker Testcontainers</b>：设置 {@code streammq.test.redis.mode=docker} 切换到 Testcontainers
 *       模式，自动拉起 Redis 容器。
 * </ul>
 *
 * <p>所有集成测试应继承此类，获得预配置的 RedissonClient。
 *
 * <p>使用 {@link StringCodec} 作为默认编解码器，避免与 Lua 脚本交互时的 Kryo 反序列化问题。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class StreamMQTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQTestBase.class);

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6379;
    private static final String MODE_DOCKER = "docker";

    protected static EmbeddedRedisServer redisServer;
    protected static RedissonClient redissonClient;
    protected static boolean useDocker;

    @BeforeAll
    static void setupAll() {
        String mode = System.getProperty("streammq.test.redis.mode", "local");
        useDocker = MODE_DOCKER.equalsIgnoreCase(mode);

        if (useDocker) {
            LOG.info("Starting Redis via Testcontainers (Docker mode)...");
            redisServer = new EmbeddedRedisServer();
            redisServer.start();
        } else {
            LOG.info("Connecting to local Redis (local mode)...");
        }

        String host = getRedisHost();
        int port = getRedisPort();

        // 无 Redis 可用时优雅跳过集成测试，避免 Connection refused 直接失败
        Assumptions.assumeTrue(
                RedisAvailability.isAvailable(host, port),
                "Redis not available at " + host + ":" + port + ", skipping integration test");

        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + host + ":" + port)
                .setDatabase(0)
                .setConnectionPoolSize(8)
                .setConnectionMinimumIdleSize(2)
                .setConnectTimeout(3000)
                .setTimeout(3000);
        // 使用 StringCodec 避免 Kryo 反序列化问题（与 Lua 脚本交互时）
        config.setCodec(StringCodec.INSTANCE);

        redissonClient = Redisson.create(config);
        LOG.info("RedissonClient connected to {}:{}", host, port);
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

    protected static String getRedisHost() {
        if (useDocker && redisServer != null) {
            return redisServer.getHost();
        }
        return System.getProperty("streammq.test.redis.host", DEFAULT_HOST);
    }

    protected static int getRedisPort() {
        if (useDocker && redisServer != null) {
            return redisServer.getPort();
        }
        return Integer.getInteger("streammq.test.redis.port", DEFAULT_PORT);
    }

    protected String getRedisConnectionString() {
        return "redis://" + getRedisHost() + ":" + getRedisPort();
    }

    protected void clearRedisData() {
        if (redissonClient != null) {
            redissonClient.getKeys().flushdb();
        }
    }
}
