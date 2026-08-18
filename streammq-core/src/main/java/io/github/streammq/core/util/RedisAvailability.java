package io.github.streammq.core.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Redis 可用性探测工具，供测试基类在连接前判断本地 Redis 是否可用。
 *
 * <p>用于避免在无 Redis 环境中运行集成测试时直接连接失败（Connection refused）， 而是在 {@code @BeforeAll} /
 * {@code @BeforeEach} 阶段通过 JUnit {@code Assumptions} 优雅跳过依赖 Redis 的测试。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class RedisAvailability {

    private static final int CONNECT_TIMEOUT_MILLIS = 500;

    private RedisAvailability() {}

    /**
     * 探测指定地址的 TCP 端口是否可连接。
     *
     * @param host 主机地址
     * @param port 端口
     * @return true 表示可连接（Redis 大概率可用）
     */
    public static boolean isAvailable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            return true;
        } catch (IOException ex) {
            return false;
        }
    }
}
