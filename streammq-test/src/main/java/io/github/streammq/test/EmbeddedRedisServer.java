package io.github.streammq.test;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;

/**
 * 嵌入式 Redis 服务器，基于 Testcontainers。
 *
 * <p>提供测试环境下的 Redis 实例，支持启动、停止和获取连接信息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class EmbeddedRedisServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedRedisServer.class);

    private static final String REDIS_IMAGE = "redis:7.2-alpine";

    private GenericContainer<?> container;

    /**
     * 启动嵌入式 Redis 服务器。
     */
    public void start() {
        if (container != null && container.isRunning()) {
            LOG.warn("Redis container already running");
            return;
        }

        LOG.info("Starting embedded Redis server...");
        container = new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                .withExposedPorts(6379)
                .withEnv("REDIS_DISABLE_COMMANDS", "")
                .withCommand("--appendonly", "yes");

        container.start();
        LOG.info("Embedded Redis server started: {}:{}", getHost(), getPort());
    }

    /**
     * 停止嵌入式 Redis 服务器。
     */
    public void stop() {
        if (container != null && container.isRunning()) {
            LOG.info("Stopping embedded Redis server...");
            container.stop();
            LOG.info("Embedded Redis server stopped");
        }
    }

    /**
     * 获取 Redis 主机地址。
     *
     * @return 主机地址
     */
    public String getHost() {
        if (container == null) {
            throw new IllegalStateException("Redis container not started");
        }
        return container.getHost();
    }

    /**
     * 获取 Redis 端口。
     *
     * @return 端口号
     */
    public int getPort() {
        if (container == null) {
            throw new IllegalStateException("Redis container not started");
        }
        return container.getMappedPort(6379);
    }

    /**
     * 获取 Redis 连接字符串。
     *
     * @return 连接字符串，格式为 redis://host:port
     */
    public String getConnectionString() {
        return "redis://" + getHost() + ":" + getPort();
    }

    /**
     * 判断 Redis 是否正在运行。
     *
     * @return true 如果正在运行
     */
    public boolean isRunning() {
        return container != null && container.isRunning();
    }

    @Override
    public void close() throws IOException {
        stop();
    }
}