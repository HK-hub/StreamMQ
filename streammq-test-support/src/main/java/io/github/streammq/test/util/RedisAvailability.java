/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Redis 可用性探测工具，供各模块测试基类在连接前判断本地 Redis 是否可用。
 *
 * <p>探测方式为真实协议握手：TCP 连接成功后发送 {@code PING} 命令， 并要求在超时窗口内收到以 {@code +PONG} 开头的 RESP 响应——避免把占用端口的非
 * Redis 服务误判为可用。任何 IOException / 超时 / 响应不匹配均判定为不可用。
 *
 * <p>用于避免在无 Redis 环境中运行集成测试时直接连接失败（Connection refused）， 而是在 {@code @BeforeAll} /
 * {@code @BeforeEach} 阶段通过 JUnit {@code Assumptions} 或 {@code @EnabledIf} 优雅跳过依赖 Redis 的测试。
 *
 * <p>本类位于零依赖叶子模块 {@code streammq-test-support}，请始终以 {@code test} scope 引用；不要在生产代码中使用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class RedisAvailability {

    private static final int CONNECT_TIMEOUT_MILLIS = 500;

    private static final int PROBE_SO_TIMEOUT_MILLIS = 300;

    private static final String PING_COMMAND = "PING\r\n";

    private static final byte[] PING_PAYLOAD = PING_COMMAND.getBytes(StandardCharsets.US_ASCII);

    private static final String EXPECTED_REPLY_PREFIX = "+PONG";

    private static final int PREFIX_LENGTH = EXPECTED_REPLY_PREFIX.length();

    private RedisAvailability() {}

    /**
     * 探测指定地址是否运行着可响应 PING 的 Redis。
     *
     * @param host 主机地址
     * @param port 端口
     * @return true 表示完成 PING/PONG 协议握手（Redis 可用）
     */
    public static boolean isAvailable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(PROBE_SO_TIMEOUT_MILLIS);
            OutputStream out = socket.getOutputStream();
            out.write(PING_PAYLOAD);
            out.flush();
            return readsPong(socket.getInputStream());
        } catch (IOException ex) {
            return false;
        }
    }

    /**
     * 读取响应并校验其以 {@code +PONG} 开头（容忍 TCP 分段）。
     *
     * <p>读到流结尾、超时或前缀不匹配均返回 false；方法内部保证只消费至多一个短缓冲区， 探测完成后由调用方关闭 Socket。
     */
    private static boolean readsPong(InputStream in) throws IOException {
        byte[] buffer = new byte[64];
        int offset = 0;
        while (offset < buffer.length) {
            int n = in.read(buffer, offset, buffer.length - offset);
            if (n < 0) {
                break;
            }
            offset += n;
            if (offset >= PREFIX_LENGTH) {
                break;
            }
        }
        return offset >= PREFIX_LENGTH
                && EXPECTED_REPLY_PREFIX.equals(
                        new String(buffer, 0, PREFIX_LENGTH, StandardCharsets.US_ASCII));
    }

    /**
     * 判断本地默认端口 (localhost:6379) 的 Redis 是否可用。
     *
     * <p>供测试类通过 {@code @EnabledIf} 引用（要求无参静态方法）， 在 Spring 上下文加载之前完成跳过判定。
     *
     * @return true 表示 Redis 可用
     */
    public static boolean localhostAvailable() {
        return isAvailable("localhost", 6379);
    }
}
