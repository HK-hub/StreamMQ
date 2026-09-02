/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

/**
 * Redis 集成测试基类,管理 Redisson 客户端和测试命名空间隔离。
 *
 * <p>每个测试类在 {@link #setUpRedis()} 中创建独立的 {@link RedissonClient} 与随机 namespace, 在 {@link
 * #tearDownRedis()} 中清理本命名空间所有 key 并关闭客户端,避免测试间相互干扰。
 *
 * <p>本地无 Redis 时（如开发环境未启动 Redis / 未配置 CI Redis service）， 通过 JUnit {@code Assumptions} 在 {@code
 * setUpRedis()} 阶段优雅跳过，避免集成测试因 Connection refused 直接失败。
 */
public abstract class AbstractRedisIT {

    protected RedissonClient redisson;
    protected String namespace;
    protected MessageSerializer<Object> serializer;
    protected MessageConverter converter;

    @BeforeEach
    void setUpRedis() {
        if (!isRedisAvailable("localhost", 6379)) {
            // 0.1.0 起：跳过时必须输出显眼警告，防止"全绿但其实没跑 IT"
            // 期望 CI 通过 Docker service 保证 Redis 可用；若在本地看到此警告，请启动 Redis 或使用 Testcontainers。
            System.err.println(
                    "[StreamMQ IT] SKIPPED — Redis not available at localhost:6379."
                            + " Start Redis (e.g. `docker run -d -p 6379:6379 redis:7.2`) to run"
                            + " integration tests.");
            Assumptions.assumeTrue(
                    false, "Redis not available at localhost:6379, skipping integration test");
        }
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        // 使用 StringCodec 避免 Kryo 反序列化问题（与 Lua 脚本交互时）
        config.setCodec(StringCodec.INSTANCE);
        redisson = Redisson.create(config);
        namespace = "it-" + UUID.randomUUID().toString().substring(0, 8);
        serializer = new JacksonJsonSerializer<>();
        converter = new DefaultMessageConverter(serializer);
    }

    @AfterEach
    void tearDownRedis() {
        if (redisson != null) {
            redisson.getKeys().deleteByPattern("streammq:" + namespace + ":*");
            redisson.shutdown();
        }
    }

    /**
     * 显式创建消费者组（{@code id(0-0)} 从头消费）。
     *
     * <p>主代码 {@code RedissonStreamListener.ensureGroup()} 在首条消息拉取时会自动创建消费者组， 本方法供需要
     * 提前建组（避免首条消息被跳过/竞态）的测试显式调用。 使用 {@code new StreamMessageId(0, 0)}（即 "0-0"）作为起始 ID。
     *
     * @param topic 主题
     * @param group 消费者组名
     */
    protected void createConsumerGroup(String topic, String group) {
        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        stream.createGroup(
                StreamCreateGroupArgs.name(group).makeStream().id(new StreamMessageId(0, 0)));
    }

    /** 本地 Redis 可用性探测：TCP 连接后发送 PING，要求收到 +PONG 响应。 */
    private static boolean isRedisAvailable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 500);
            socket.setSoTimeout(300);
            OutputStream out = socket.getOutputStream();
            out.write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return readsPong(socket.getInputStream());
        } catch (IOException ex) {
            return false;
        }
    }

    private static boolean readsPong(InputStream in) throws IOException {
        byte[] buffer = new byte[64];
        int offset = 0;
        String expected = "+PONG";
        while (offset < buffer.length) {
            int n = in.read(buffer, offset, buffer.length - offset);
            if (n < 0) break;
            offset += n;
            if (offset >= expected.length()) break;
        }
        return offset >= expected.length()
                && expected.equals(
                        new String(buffer, 0, expected.length(), StandardCharsets.US_ASCII));
    }
}
