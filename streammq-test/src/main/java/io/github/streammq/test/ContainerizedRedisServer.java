/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test;

import java.io.Closeable;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * 嵌入式 Redis 服务器，基于 Testcontainers。
 *
 * <p>提供测试环境下的 Redis 实例，支持启动、停止和获取连接信息。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class ContainerizedRedisServer implements Closeable {
    /** Docker 缺失时的可操作提示（中英双语） */
    private static final String DOCKER_MISSING_MESSAGE =
            "ContainerizedRedisServer requires a running Docker daemon."
                    + " 本测试工具基于 Testcontainers 容器化 Redis（并非进程内嵌入实现），"
                    + "请启动 Docker Desktop / dockerd 后重试，"
                    + "或改用本地模式：streammq.test.redis.mode=local";

    private static final Logger LOG = LoggerFactory.getLogger(ContainerizedRedisServer.class);

    /** Redis 容器镜像 */
    private static final String REDIS_IMAGE = "redis:7.2-alpine";

    /** 容器暴露的 Redis 端口 */
    private static final int EXPOSED_REDIS_PORT = StreamMQTestBase.DEFAULT_PORT;

    /** Redis 服务器启动参数：开启 AOF 持久化 */
    private static final String ARG_APPENDONLY = "--appendonly";

    private GenericContainer<?> container;

    /** 启动嵌入式 Redis 服务器。 */
    public void start() {
        if (container != null && container.isRunning()) {
            LOG.warn("Redis container already running");
            return;
        }
        ensureDockerAvailable();

        LOG.info("Starting Redis test container...");
        // 注意：本类并非真正的"嵌入式"进程内 Redis，而是基于 Testcontainers 的容器化 Redis，
        // 需要可用的 Docker daemon（见类 javadoc 与 ensureDockerAvailable 的失败提示）
        container =
                new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                        .withExposedPorts(EXPOSED_REDIS_PORT);

        container.start();
        LOG.info("Redis test container started: {}:{}", getHost(), getPort());
    }

    /**
     * 启动前探测 Docker 可用性，把 Testcontainers 原生的晦涩异常转换为可操作的提示。
     *
     * @throws IllegalStateException 当没有可用 Docker 环境时
     */
    private static void ensureDockerAvailable() {
        try {
            boolean ok = org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
            if (!ok) {
                throw new IllegalStateException(DOCKER_MISSING_MESSAGE);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(DOCKER_MISSING_MESSAGE, e);
        }
    }

    /** 停止嵌入式 Redis 服务器。 */
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
        return container.getMappedPort(EXPOSED_REDIS_PORT);
    }

    /**
     * 获取 Redis 连接字符串。
     *
     * @return 连接字符串，格式为 redis://host:port
     */
    public String getConnectionString() {
        return StreamMQTestBase.REDIS_URI_PREFIX + getHost() + ":" + getPort();
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
