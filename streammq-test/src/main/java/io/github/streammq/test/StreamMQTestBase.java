/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test;

import io.github.streammq.test.util.RedisAvailability;
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

    // ==================== 测试系统属性名 ====================
    /** 系统属性：Redis 连接模式（local / docker） */
    public static final String SYSPROP_TEST_REDIS_MODE = "streammq.test.redis.mode";

    /** 系统属性：Redis 主机 */
    public static final String SYSPROP_TEST_REDIS_HOST = "streammq.test.redis.host";

    /** 系统属性：Redis 端口 */
    public static final String SYSPROP_TEST_REDIS_PORT = "streammq.test.redis.port";

    /** 默认 Redis 模式 */
    public static final String MODE_LOCAL = "local";

    /** Docker 模式标识 */
    private static final String MODE_DOCKER = "docker";

    // ==================== 默认连接参数 ====================
    /** 默认 Redis 主机 */
    public static final String DEFAULT_HOST = "localhost";

    /** 默认 Redis 端口 */
    public static final int DEFAULT_PORT = 6379;

    // ==================== 客户端默认参数 ====================
    /** 测试客户端连接池大小 */
    public static final int TEST_POOL_SIZE = 8;

    /** 测试客户端最小空闲连接数 */
    public static final int TEST_POOL_MIN_IDLE = 2;

    /** 测试客户端连接超时（毫秒） */
    public static final int TEST_CONNECT_TIMEOUT_MS = 3_000;

    /** 测试客户端响应超时（毫秒） */
    public static final int TEST_RESPONSE_TIMEOUT_MS = 3_000;

    /** Redis URI scheme 前缀 */
    public static final String REDIS_URI_PREFIX = "redis://";

    protected static ContainerizedRedisServer redisServer;
    protected static RedissonClient redissonClient;
    protected static boolean useDocker;

    @BeforeAll
    static void setupAll() {
        String mode = System.getProperty(SYSPROP_TEST_REDIS_MODE, MODE_LOCAL);
        useDocker = MODE_DOCKER.equalsIgnoreCase(mode);

        if (useDocker) {
            LOG.info("Starting Redis via Testcontainers (Docker mode)...");
            redisServer = new ContainerizedRedisServer();
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
                .setAddress(REDIS_URI_PREFIX + host + ":" + port)
                .setDatabase(0)
                .setConnectionPoolSize(TEST_POOL_SIZE)
                .setConnectionMinimumIdleSize(TEST_POOL_MIN_IDLE)
                .setConnectTimeout(TEST_CONNECT_TIMEOUT_MS)
                .setTimeout(TEST_RESPONSE_TIMEOUT_MS);
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
        return System.getProperty(SYSPROP_TEST_REDIS_HOST, DEFAULT_HOST);
    }

    protected static int getRedisPort() {
        if (useDocker && redisServer != null) {
            return redisServer.getPort();
        }
        return Integer.getInteger(SYSPROP_TEST_REDIS_PORT, DEFAULT_PORT);
    }

    protected String getRedisConnectionString() {
        return REDIS_URI_PREFIX + getRedisHost() + ":" + getRedisPort();
    }

    /**
     * 清空当前连接数据库中的全部数据（flushdb）。
     *
     * <p><b>隔离机制说明：</b>测试间的数据隔离<b>主要依赖</b>每个测试使用独立的 namespace / topic （唯一 Redis Key），而非
     * flushdb——后者只是可选的辅助清理手段。
     *
     * <p><b>安全守卫：</b>本地模式下本方法默认静默跳过（no-op），必须显式设置系统属性 {@code
     * -Dstreammq.test.redis.flushAllowed=true} 才会真正执行 flush；docker/container 模式下实例是测试独占的，无需该开关。
     */
    protected void clearRedisData() {
        if (redissonClient == null) {
            return;
        }
        if (!useDocker && !Boolean.getBoolean("streammq.test.redis.flushAllowed")) {
            LOG.warn(
                    "Skipped flushdb against {}:{} — set -Dstreammq.test.redis.flushAllowed=true"
                            + " to allow flushing a LOCAL Redis (may contain non-StreamMQ data)",
                    getRedisHost(),
                    getRedisPort());
            return;
        }
        redissonClient.getKeys().flushdb();
    }
}
