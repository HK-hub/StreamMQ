/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.spring.boot.autoconfigure.StreamMQAdminEndpoint;
import io.github.streammq.test.util.RedisAvailability;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIf;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

/**
 * 管理端点 × <b>真实 Redis</b> 集成测试。
 *
 * <p>此前的管理端点测试全部基于 Mockito mock（{@code StreamMQAdminEndpointTest} / {@code
 * StreamMQActuatorEndpointHardeningTest}），无法发现"注册表 key 拼错 / codec 用错 / 真实数据结构与假设不符"这类缺陷——它们在 mock
 * 下永远看不出来。本类把端点方法跑在真实 Redis 上：
 *
 * <ul>
 *   <li>Topic 生命周期：{@code createTopic} 真实写入注册表 Set（且能被 {@code listTopics} 读到）、 {@code deleteTopic}
 *       必须 confirm 且真实删除 Stream 与注册表条目；
 *   <li>{@code getStats} 的 {@code pendingCount} 直接来自 Redis PEL（断言真实计数而非结构）；
 *   <li>{@code ackPending} 真实把消息移出 PEL（断言 pendingCount 归零）。
 * </ul>
 */
@SpringBootTest
@ActiveProfiles("it")
@ContextConfiguration(classes = {RedissonTestConfig.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("管理端点真实 Redis 集成测试")
@EnabledIf(
        value = "io.github.streammq.test.util.RedisAvailability#localhostAvailable",
        disabledReason = "Redis not available at localhost:6379")
class StreamMQAdminEndpointIT {

    /** 与 application-it.yml 中的 streammq.namespace 保持一致 */
    private static final String NS = "it-starter";

    private static final String GROUP = "admin-it-group";

    @Autowired private StreamMQAdminEndpoint adminEndpoint;

    @Autowired private RedissonClient redissonClient;

    private final String topic = "admin-it-topic-" + UUID.randomUUID().toString().substring(0, 8);

    @BeforeAll
    static void requireRedis() {
        // 无本地 Redis 时跳过，保证 mvn verify 在任意环境可复现
        Assumptions.assumeTrue(
                RedisAvailability.isAvailable("localhost", 6379),
                "Redis not available at localhost:6379, skipping IT");
    }

    @AfterAll
    void cleanUp() {
        redissonClient.getSet(StreamMQKeys.topicRegistry(NS), StringCodec.INSTANCE).remove(topic);
        redissonClient
                .getStream(StreamMQKeys.topicStream(NS, topic), StringCodec.INSTANCE)
                .delete();
    }

    @Test
    @DisplayName("Topic 生命周期：注册表真实写入 + confirm 保护的不可逆删除")
    void topicLifecycleAgainstRealRedis() {
        Map<String, Object> created = adminEndpoint.createTopic(topic);
        assertThat(created).containsEntry("success", true);
        assertThat(created).containsEntry("created", true);

        // 真实写入注册表 Set（StringCodec）——key 或 codec 写错时此处立即失败
        assertThat(
                        redissonClient
                                .<String>getSet(
                                        StreamMQKeys.topicRegistry(NS), StringCodec.INSTANCE)
                                .contains(topic))
                .isTrue();
        assertThat(adminEndpoint.listTopics()).contains(topic);

        // 不可逆删除必须显式 confirm，缺失/不匹配一律拒绝
        Map<String, Object> rejected = adminEndpoint.deleteTopic(topic, "wrong");
        assertThat(rejected).containsEntry("success", false);
        assertThat(String.valueOf(rejected.get("error"))).contains("confirm");
        assertThat(adminEndpoint.listTopics()).contains(topic);

        Map<String, Object> deleted = adminEndpoint.deleteTopic(topic, topic);
        assertThat(deleted).containsEntry("success", true);
        assertThat(adminEndpoint.listTopics()).doesNotContain(topic);
    }

    @Test
    @DisplayName("getStats.pendingCount 来自真实 PEL；ackPending 真实移出 PEL")
    void pendingCountAndAckAgainstRealRedis() {
        // 造一条真实 pending：先建组（新组位点 0-0）→ XADD → readGroup（不 ACK）
        RStream<String, String> stream =
                redissonClient.getStream(StreamMQKeys.topicStream(NS, topic), StringCodec.INSTANCE);
        stream.createGroup(org.redisson.api.stream.StreamCreateGroupArgs.name(GROUP).makeStream());
        stream.add(StreamAddArgs.entries(Map.of("body", "admin-it-payload")));
        Map<StreamMessageId, Map<String, String>> read =
                stream.readGroup(GROUP, "admin-it-consumer", StreamReadGroupArgs.neverDelivered());
        assertThat(read).isNotEmpty();
        StreamMessageId msgId = read.keySet().iterator().next();

        Object pendingCount = adminEndpoint.getStats(GROUP, topic).get("pendingCount");
        assertThat(pendingCount).isEqualTo(read.size());

        Map<String, Object> ack = adminEndpoint.ackPending(GROUP, topic, msgId.toString());
        assertThat(ack).containsEntry("success", true);

        assertThat(adminEndpoint.getStats(GROUP, topic).get("pendingCount")).isEqualTo(0);

        // listPending 与 listDlq 在真实 Redis 上也要可用（不抛异常、结构正确）
        List<Map<String, Object>> pending = adminEndpoint.listPending(GROUP, topic, 10);
        assertThat(pending).isEmpty();
        assertThat(adminEndpoint.listDlq(GROUP, 10)).isNotNull();
    }
}
