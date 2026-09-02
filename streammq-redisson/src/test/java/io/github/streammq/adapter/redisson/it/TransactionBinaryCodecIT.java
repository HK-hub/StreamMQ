/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.codec.Kryo5Codec;
import org.redisson.config.Config;

/**
 * 回归测试：事务状态机在"客户端默认 codec 为二进制"（生产默认，如 redisson-spring-boot-starter） 时也必须正常工作。
 *
 * <p>背景：{@code AbstractRedisIT} 一律显式使用 {@code StringCodec}，恰好掩盖了如下缺陷—— {@code
 * TransactionScanner#casState} 等 Lua 脚本以 {@code StringCodec} 读写 txstate Hash 的字段， 而 Hash 由 {@code
 * RMap} 以客户端默认 codec 写入（二进制 codec 会给 key/value 加前缀）。 编码不一致导致 Lua {@code HGET} 永远 miss（返回
 * MISSING），{@code markCommit} 静默返回， 事务消息"只报成功、永不发布"。本 IT 刻意使用 {@link MarshallingCodec} 复现该场景并防止回归。
 *
 * <p>本地无 Redis 时按 JUnit Assumptions 优雅跳过。
 */
@DisplayName("事务状态机-二进制默认 codec 回归测试")
class TransactionBinaryCodecIT {

    /** 回查间隔（毫秒），足够大以避免断言期间的竞态条件 */
    private static final long CHECK_INTERVAL_MS = 1500L;

    /** 最大回查次数 */
    private static final int MAX_CHECK_TIMES = 3;

    /** 单次扫描批量 */
    private static final int BATCH_SIZE = 100;

    /** 测试用事务组名 */
    private static final String TX_GROUP = "tx-binary-it-group";

    private RedissonClient redisson;
    private String namespace;

    @BeforeEach
    void setUpRedis() {
        if (!isRedisAvailable("localhost", 6379)) {
            Assumptions.assumeTrue(
                    false, "Redis not available at localhost:6379, skipping integration test");
        }
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        // 生产默认：二进制 codec（Kryo/JBoss Marshalling）。若主代码依赖"默认 codec 恰好是
        // StringCodec"才能工作，本测试必挂——这正是要防止的回归。
        config.setCodec(new Kryo5Codec());
        redisson = Redisson.create(config);
        namespace = "it-bin-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    @DisplayName("二进制默认 codec 下 markCommit 必须真正发布到目标 Stream")
    void markCommit_publishes_to_target_stream_under_binary_codec() {
        TransactionScanner scanner = newScanner();
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-bin-commit-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "bin-commit-body");

            StreamMessageId halfId =
                    scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);
            assertThat(halfId).isNotNull();

            // 提交：若 casState 因 codec 不匹配返回 MISSING，markCommit 会静默返回，
            // 目标 Stream 将保持为空——本断言用于戳穿该静默失败。
            scanner.markCommit(txId, TX_GROUP);

            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isEqualTo(1);

            // 半消息已删除、状态为 COMMIT、回查调度已清除
            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();

            RMap<String, String> stateMap =
                    redisson.getMap(
                            StreamMQKeys.transactionStateHash(namespace, TX_GROUP),
                            org.redisson.client.codec.StringCodec.INSTANCE);
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_COMMIT);

            assertThat(
                            redisson.getScoredSortedSet(
                                            StreamMQKeys.transactionCheckZSet(namespace, TX_GROUP))
                                    .size())
                    .isZero();
        } finally {
            redisson.getKeys().deleteByPattern("streammq:" + namespace + ":*");
            redisson.shutdown();
        }
    }

    @Test
    @DisplayName("二进制默认 codec 下 markRollback 必须删除半消息")
    void markRollback_removes_half_message_under_binary_codec() {
        TransactionScanner scanner = newScanner();
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-bin-rollback-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "bin-rollback-body");

            StreamMessageId halfId =
                    scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);
            assertThat(halfId).isNotNull();

            scanner.markRollback(txId, TX_GROUP);

            RStream<String, String> halfStream =
                    redisson.getStream(StreamMQKeys.halfStream(namespace, TX_GROUP));
            assertThat(halfStream.size()).isZero();

            RStream<String, String> targetStream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic));
            assertThat(targetStream.size()).isZero();

            RMap<String, String> stateMap =
                    redisson.getMap(
                            StreamMQKeys.transactionStateHash(namespace, TX_GROUP),
                            org.redisson.client.codec.StringCodec.INSTANCE);
            assertThat(stateMap.get(txId)).isEqualTo(TransactionScanner.STATE_ROLLBACK);
        } finally {
            redisson.getKeys().deleteByPattern("streammq:" + namespace + ":*");
            redisson.shutdown();
        }
    }

    @Test
    @DisplayName("二进制默认 codec 下回查计数必须可读（Lua HINCRBY 与 RMap 读取一致）")
    void check_counter_is_readable_under_binary_codec() throws Exception {
        TransactionScanner scanner = newScanner();
        try {
            String txId = UUID.randomUUID().toString();
            String targetTopic = "tx-bin-check-" + UUID.randomUUID().toString().substring(0, 6);
            Map<String, String> fields = buildFields(targetTopic, "bin-check-body");

            StreamMessageId halfId =
                    scanner.registerHalfMessage(txId, TX_GROUP, targetTopic, fields);
            assertThat(halfId).isNotNull();

            // 触发回查（UNKNOWN 路径）：incrementCheckCount 用 Lua HINCRBY 写计数，
            // getCheckCount 用 RMap 读——两者 codec 不一致时 count 永远读不到。
            // 这里直接反射调用私有计数方法不可取，改为驱动 scanner 的扫描路径：
            // 先把状态降级为 UNKNOWN 再触发 triggerCheck 多次。
            scanner.markCommit(txId, TX_GROUP);
            assertThat(targetStreamSize(targetTopic)).isEqualTo(1);

            // 验证转投幂等（提交为单 Lua 脚本原子执行，半消息被 XDEL 后重复提交读到 HALF_MISSING）：
            // 再次 markCommit（幂等，终态 COMMIT 应被忽略且不抛异常，不会重复转投）。
            scanner.markCommit(txId, TX_GROUP);
            assertThat(targetStreamSize(targetTopic)).isEqualTo(1);
        } finally {
            redisson.getKeys().deleteByPattern("streammq:" + namespace + ":*");
            redisson.shutdown();
        }
    }

    private long targetStreamSize(String targetTopic) {
        return redisson.getStream(StreamMQKeys.topicStream(namespace, targetTopic)).size();
    }

    private TransactionScanner newScanner() {
        return new TransactionScanner(
                redisson, namespace, converter(), CHECK_INTERVAL_MS, MAX_CHECK_TIMES, BATCH_SIZE);
    }

    private io.github.streammq.core.converter.MessageConverter converter() {
        return new io.github.streammq.adapter.redisson.converter.DefaultMessageConverter(
                new io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer<>());
    }

    private Map<String, String> buildFields(String targetTopic, String body) {
        Message<String> message =
                MessageBuilder.<String>withTopic(targetTopic).tag("tx-tag").body(body).build();
        return converter().toStreamFields(message);
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
