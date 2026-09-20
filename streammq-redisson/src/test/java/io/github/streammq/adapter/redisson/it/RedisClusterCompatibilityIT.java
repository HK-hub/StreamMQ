/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionCommitExecutor;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.support.RedisClusterCompatibility;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.StreamMQException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.options.KeysScanOptions;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamRangeArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

/**
 * Redis Cluster 兼容性实测（R6-CLUSTER 闭环）：把「0.1.x 不支持 Cluster」由文档推断升级为真实集群测量， 并锁定修复后的产品行为——跨 key 原子操作在
 * Cluster 配置下<b>写第一个 key 之前</b>即被拒绝。
 *
 * <p><b>实测结论矩阵（本类即证据，全部在真实 3 主集群上测得）：</b>
 *
 * <ul>
 *   <li><b>可用</b>：发送（单 key {@code XADD}）与基础消费/ACK（{@code XREADGROUP} / {@code XACK}）——这两条 路径不含
 *       Lua，单 key 路由，实测往返成功（{@link #singleKeyStreamOperations_workOnCluster}、 {@link
 *       #realProducerPath_sendsOnCluster}）；
 *   <li><b>多 key Lua 硬失败</b>：框架 key 家族（业务流、重试 ZSet/流、DLQ 流、延时 ZSet）不带 hash tag，实测落在不同 slot（{@link
 *       #frameworkKeyFamily_spansMultipleSlots}），形如"XACK + XADD/ZADD 跨 key"的脚本 被真实服务端 CROSSSLOT
 *       拒绝——PEL 认领补偿的跨流形态（{@link #multiKeyScript_failsWithCrossSlot_andLeavesMessagePending}、{@link
 *       #claimScriptShape_sameStreamVsCrossStream}）在 Cluster 上无法推进（除非引入 hash tag 强制同 slot，而全模块仅
 *       {@code PelClaimScheduler}、{@code TransactionCommitExecutor} 两处脚本声明多 key KEYS）；
 *   <li><b>多 key 原子批的失败形态取决于 key 落点</b>（绕过守卫直呼批原语的对照测量）：同 master 跨 slot → 编组为单个 MULTI/EXEC、在 EXEC
 *       时被 CROSSSLOT 拒绝（{@link #atomicBatchBypassingGuard_sameNodeIsRejectedWithCrossSlot}）；跨
 *       master → 按节点拆分、各自提交成功、整体 静默失去原子性（{@link
 *       #atomicBatchBypassingGuard_differentNodesIsSilentlySplit}）——"看运气的 key 落点"正是守卫必须前置拒绝的直接原因；
 *   <li><b>产品行为（本次审查的修复）</b>：Cluster 配置下，库自身全部跨 key 原子路径（延时登记/转投、重试与 DLQ 转投、事务登记/提交、跨流 PEL
 *       认领）在写入前抛出可操作的 {@link StreamMQException}（{@link
 *       RedisClusterCompatibility#requireCrossKeyAtomicity}）；{@link
 *       #delayEnqueueOnCluster_refusedBeforeAnyWrite}、{@link #transactionPaths_refusedOnCluster}
 *       逐条断言 "拒绝 = 零副作用"，{@link #transactionPaths_refusedOnCluster} 并保留三 key 原生脚本被 CROSSSLOT 拒绝的
 *       服务端证据；
 *   <li><b>拒绝是 fail-safe 的</b>：集群上已存在的调度条目不会被丢弃或半写——转投被拒后 payload 与调度条目原样保留， 修复部署形态后可继续投递（{@link
 *       #delayTransferOnCluster_refusedAndSchedulingStateIntact}）；
 *   <li><b>运行期守卫可命中</b>：{@link RedisClusterCompatibility#isClusterConfigured} 是硬拒绝的唯一依据
 *       （配置级），{@link RedisClusterCompatibility#isClusterMode} 额外覆盖"单机配置误指向 Cluster 节点"的
 *       探针级兜底并在启动期输出一次性 WARN（{@link #clusterTopologyDetectedByGuard}）。
 * </ul>
 *
 * <p>集群节点来自系统属性 {@code streammq.it.cluster.nodes}（缺省 {@code 127.0.0.1:7000-7002}），逐节点 TCP PING
 * 探测；任一节点不可达时整类 skip 而非 fail，并打印集群启动指引——没有集群的环境不会产生"全绿但没测"的假象。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("Redis Cluster 兼容性实测（真实 3 主集群）")
class RedisClusterCompatibilityIT {

    /** 集群节点列表系统属性：逗号分隔 host:port。 */
    private static final String CLUSTER_NODES_PROPERTY = "streammq.it.cluster.nodes";

    /** 缺省节点：本地 3 主集群（cluster_state:ok，16384 slots 全覆盖）。 */
    private static final String DEFAULT_CLUSTER_NODES =
            "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002";

    private static final String TOPIC = "cls-it-topic";
    private static final String GROUP = "cls-it-group";
    private static final String CONSUMER = "cls-it-consumer";

    /** 服务端权威 slot → 节点映射（{@code CLUSTER SLOTS}），用于确定性地构造两种放置场景。 */
    private static final String LUA_CLUSTER_SLOTS = "return redis.call('CLUSTER','SLOTS')";

    private RedissonClient redisson;
    private String namespace;
    private MessageConverter converter;

    /** slot → 负责节点（host:port）缓存：集群在测试期间不做 reshard，映射稳定。 */
    private Map<Integer, String> cachedSlotOwners;

    @BeforeEach
    void setUpClusterRedis() {
        List<String> nodes = configuredNodes();
        List<String> unreachable = nodes.stream().filter(node -> !isNodeAvailable(node)).toList();
        if (!unreachable.isEmpty()) {
            // 显眼警告 + skip：与 AbstractRedisIT 同一约定——"全绿但其实没跑 IT"必须能被一眼看穿。
            // 本类只在真实集群存在时才有意义：它测量的正是真实服务端对多 key 脚本的拒绝行为，
            // 任何 mock/单实例替身都无法产生这份证据。
            System.err.println(
                    "[StreamMQ IT] SKIPPED — Redis Cluster node(s) unreachable: "
                            + unreachable
                            + " (configured via -D"
                            + CLUSTER_NODES_PROPERTY
                            + "="
                            + String.join(",", nodes)
                            + "). Start a 3-master cluster on 127.0.0.1:7000-7002 to run this"
                            + " class, e.g. per node `redis-server --port 7000 --cluster-enabled"
                            + " yes --cluster-config-file nodes-7000.conf --appendonly yes` (repeat"
                            + " for 7001/7002), then `redis-cli --cluster create 127.0.0.1:7000"
                            + " 127.0.0.1:7001 127.0.0.1:7002 --cluster-replicas 0`. Until then the"
                            + " Cluster evidence is NOT measured in this environment.");
            Assumptions.assumeTrue(
                    false, "Redis Cluster unreachable: " + unreachable + ", skipping Cluster IT");
        }
        Config config = new Config();
        config.useClusterServers()
                .addNodeAddress(
                        nodes.stream().map(node -> "redis://" + node).toArray(String[]::new))
                // 与 AbstractRedisIT 相同的放宽理由：全量 verify 时同一台机器上的 Redis 在高压下偶发超过
                // Redisson 默认 3s 超时（RedisResponseTimeoutException），表现为与被测逻辑无关的 flaky。
                // 此处仅放宽测试基建客户端，不改变产品默认值。
                .setTimeout(10_000)
                .setConnectTimeout(10_000)
                .setRetryAttempts(5)
                // 3.52.0 起 setRetryInterval(int) 已废弃，等价的非废弃写法是 DelayStrategy（返回 Duration）
                .setRetryDelay(attempt -> Duration.ofMillis(1_000));
        // StringCodec：与 Lua 脚本（CLUSTER KEYSLOT 探针）交互，避免默认二进制 codec 的 key 前缀干扰测量
        config.setCodec(StringCodec.INSTANCE);
        redisson = Redisson.create(config);
        namespace = "it-cluster-" + UUID.randomUUID().toString().substring(0, 8);
        converter = new DefaultMessageConverter(new JacksonJsonSerializer<>());
        cachedSlotOwners = null;
    }

    @AfterEach
    void tearDownClusterRedis() {
        if (redisson != null) {
            // 与 AbstractRedisIT 相同的命名空间级清理：Cluster 下 Redisson 逐 master SCAN+DEL，
            // 残留 key 只可能属于本类随机命名空间，不会污染其他测试或下次运行。
            redisson.getKeys().deleteByPattern("streammq:" + namespace + ":*");
            redisson.shutdown();
        }
    }

    @Test
    @DisplayName("单 key 路径基线：XADD → XREADGROUP → XACK 在真实 Cluster 上完整往返")
    void singleKeyStreamOperations_workOnCluster() {
        // 先锁定"客户端确为 Cluster 配置"：否则本类可能在单实例上假绿，绿色基线不再构成 Cluster 证据
        assertThat(redisson.getConfig().isClusterConfig())
                .as("必须先确认客户端为 Cluster 配置，单 key 基线才有测量意义")
                .isTrue();

        RStream<String, String> stream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
        StreamMessageId id = stream.add(StreamAddArgs.entries(Map.of("body", "cluster-baseline")));
        assertThat(id).as("XADD 必须返回真实 Entry ID").isNotNull();

        stream.createGroup(
                StreamCreateGroupArgs.name(GROUP).makeStream().id(new StreamMessageId(0, 0)));
        Map<StreamMessageId, Map<String, String>> read =
                stream.readGroup(GROUP, CONSUMER, StreamReadGroupArgs.neverDelivered().count(10));
        assertThat(read).as("XREADGROUP 必须读回刚写入的条目（单 key 路由）").containsKey(id);
        assertThat(read.get(id)).containsEntry("body", "cluster-baseline");

        // 未 ACK 前必须在 PEL：这是下面用例"CROSSSLOT 不产生假 ACK"断言的对照基准
        assertThat(stream.pendingRange(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .as("读取后未 ACK 的条目必须进入 PEL")
                .hasSize(1);

        long acked = stream.ack(GROUP, id);
        assertThat(acked).as("XACK 必须真实生效（返回被确认的条目数 1）").isEqualTo(1L);
        assertThat(stream.pendingRange(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .as("ACK 后 PEL 必须清空——单 key ACK 在 Cluster 上可用")
                .isEmpty();
    }

    @Test
    @DisplayName("结构性证明：框架 key 家族实测跨多个 slot（多 key Lua 因此结构上不可能）")
    void frameworkKeyFamily_spansMultipleSlots() {
        // 逐个 key 由真实服务端计算 slot（CLUSTER KEYSLOT 脚本，而非客户端推断）。
        // 覆盖的正是多 key Lua 会同时触碰的 key 家族：业务流 / 重试 ZSet / 重试流 / 死信流 / 延时 ZSet。
        String topicStream = StreamMQKeys.topicStream(namespace, TOPIC);
        String retryZSet = StreamMQKeys.retryZSet(namespace, TOPIC, GROUP);
        String retryStream = StreamMQKeys.retryStream(namespace, TOPIC, GROUP);
        String dlqStream = StreamMQKeys.dlqStream(namespace, GROUP);
        String delayZSet = StreamMQKeys.delayCustomZSet(namespace);

        Map<String, Integer> slotMap = new LinkedHashMap<>();
        slotMap.put(topicStream, slotOf(topicStream));
        slotMap.put(retryZSet, slotOf(retryZSet));
        slotMap.put(retryStream, slotOf(retryStream));
        slotMap.put(dlqStream, slotOf(dlqStream));
        slotMap.put(delayZSet, slotOf(delayZSet));

        // 只要存在 ≥2 个不同 slot，任何"同时触碰两个 key 家族"的脚本就必然被服务端拒绝——
        // 这正是重试/DLQ 路由、PEL 认领、延时转投等路径的真实形态（除非引入 hash tag 强制同 slot）。
        long distinctSlots = slotMap.values().stream().distinct().count();
        assertThat(distinctSlots)
                .as("框架 key 家族必须跨多个 slot（完整 slot 分布：%s）", slotMap)
                .isGreaterThanOrEqualTo(2L);
    }

    @Test
    @DisplayName("多 key 脚本：服务端 CROSSSLOT 拒绝，且失败是 fail-safe 的（无假 ACK / 无丢失）")
    void multiKeyScript_failsWithCrossSlot_andLeavesMessagePending() {
        String topic = pickTopicSpanningSlots();
        String streamKey = StreamMQKeys.topicStream(namespace, topic);
        String retryKey = StreamMQKeys.retryZSet(namespace, topic, GROUP);
        assertThat(slotOf(streamKey))
                .as("前置条件：两 key 必须跨 slot，否则脚本会执行成功：%s vs %s", streamKey, retryKey)
                .isNotEqualTo(slotOf(retryKey));

        RStream<String, String> stream = redisson.getStream(streamKey);
        RScoredSortedSet<String> retryZset = redisson.getScoredSortedSet(retryKey);

        // 制造 PEL 滞留：XADD 1 条 + 建组 + 消费者读取后不 ACK（模拟"重试待转投"的真实形态）
        StreamMessageId id = stream.add(StreamAddArgs.entries(Map.of("body", "cross-slot-body")));
        stream.createGroup(
                StreamCreateGroupArgs.name(GROUP).makeStream().id(new StreamMessageId(0, 0)));
        stream.readGroup(GROUP, CONSUMER, StreamReadGroupArgs.neverDelivered().count(10));
        assertThat(stream.pendingRange(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .as("前置条件：消息必须已进入 PEL")
                .hasSize(1);

        // 复刻主代码形态的多 key 脚本（XACK 旧条目 + ZADD 重试调度）；keys 不带 hash tag，跨 slot。
        // 断言基于真实服务端回复：Redisson 只按 KEYS[1] 路由，拒绝发生在服务端而非客户端。
        String crossSlotScript =
                "redis.call('XACK', KEYS[1], ARGV[1]);"
                        + " redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3]);"
                        + " return 1;";
        assertThatThrownBy(
                        () ->
                                redisson.getScript(StringCodec.INSTANCE)
                                        .eval(
                                                RScript.Mode.READ_WRITE,
                                                crossSlotScript,
                                                RScript.ReturnType.INTEGER,
                                                List.of(streamKey, retryKey),
                                                id.toString(),
                                                "0",
                                                id.toString()))
                .as("跨 slot 的 KEYS 必须被真实服务端以 CROSSSLOT 拒绝（非客户端合成错误）")
                .hasMessageContaining("CROSSSLOT");

        // fail-safe 三连：无假 ACK（仍在 PEL）、消息未丢（仍在 Stream）、目的 ZSet 未被写入。
        // 这正是"功能不可用但数据不丢"的实测证据——任何让脚本先写后失败的实现都会在这里变红。
        assertThat(stream.pendingRange(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100))
                .as("脚本被拒时不得产生任何副作用：条目必须仍留在 PEL")
                .hasSize(1);
        Map<StreamMessageId, Map<String, String>> pendingEntries =
                stream.pendingRange(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100);
        assertThat(pendingEntries).containsKey(id);
        assertThat(pendingEntries.get(id)).containsEntry("body", "cross-slot-body");
        assertThat(
                        stream.range(
                                StreamRangeArgs.startId(StreamMessageId.MIN)
                                        .endId(StreamMessageId.MAX)))
                .as("消息必须仍在 Stream 中（XACK 未执行，未丢失）")
                .containsKey(id);
        assertThat(retryZset.size()).as("ZADD 未执行：重试 ZSet 必须为空").isZero();
    }

    @Test
    @DisplayName("真实生产路径：RedissonStreamProducer.syncSend 在 Cluster 上成功且条目可读回")
    void realProducerPath_sendsOnCluster() {
        RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, GROUP, converter, 3000L, 0, 0, 0);
        try {
            Message<String> message =
                    MessageBuilder.<String>withTopic(TOPIC)
                            .tag("cluster")
                            .keys("cluster-key")
                            .body("cluster-body")
                            .build();

            SendResult result = producer.syncSend(message);

            // SEND_OK 是 0.1.2 定稿语义："已受理（普通发送 = Redis 已确认 XADD）"——单 key 路径在 Cluster 可用
            assertThat(result.isSuccess()).as("普通发送在 Cluster 上必须成功").isTrue();
            assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_OK);

            // 读回：messageId 必须是 Stream 中的真实 Entry ID（而非占位 ID），否则"成功"只是客户端自说自话
            RStream<String, String> stream =
                    redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
            Map<StreamMessageId, Map<String, String>> entries =
                    stream.range(
                            StreamRangeArgs.startId(StreamMessageId.MIN)
                                    .endId(StreamMessageId.MAX));
            assertThat(entries.keySet())
                    .as("SendResult.messageId 必须能从 Stream 读回：%s", result.getMessageId())
                    .anyMatch(
                            entryId ->
                                    entryId.toString()
                                            .equals(result.getMessageId().getStreamEntryId()));
        } finally {
            producer.close();
        }
    }

    @Test
    @DisplayName("运行期守卫：配置级判定为 Cluster（硬拒绝依据）；单机配置指向 Cluster 节点时探针仍能识别")
    void clusterTopologyDetectedByGuard() {
        // 硬拒绝的唯一依据：用户显式把客户端配成 Cluster
        assertThat(RedisClusterCompatibility.isClusterConfigured(redisson))
                .as("useClusterServers() 配置必须被判定为 Cluster（requireCrossKeyAtomicity 的依据）")
                .isTrue();
        assertThat(RedisClusterCompatibility.isClusterMode(redisson))
                .as("Cluster 配置必须被判定为 Cluster（生产者/容器启动告警的依据）")
                .isTrue();

        // 探针级命中：单机配置指向一个 Cluster 节点——isClusterConfig() 为 false，且硬拒绝不生效
        // （单机客户端不是 cluster-aware，多 key 请求只会被服务端确定性 CROSSSLOT 拒绝，不存在按节点拆分），
        // 但 CLUSTER KEYSLOT 探针必须识别出来，否则"配置写错"的部署连 WARN 都没有。
        Config misconfigured = new Config();
        misconfigured.useSingleServer().setAddress("redis://" + firstNode());
        misconfigured.setCodec(StringCodec.INSTANCE);
        RedissonClient singleConfigured = Redisson.create(misconfigured);
        try {
            assertThat(singleConfigured.getConfig().isClusterConfig()).isFalse();
            assertThat(RedisClusterCompatibility.isClusterConfigured(singleConfigured))
                    .as("单机配置 → 不触发硬拒绝（不误伤可用部署）")
                    .isFalse();
            assertThat(RedisClusterCompatibility.isClusterMode(singleConfigured))
                    .as("单机配置 + 指向 Cluster 节点：必须由探针兜底识别，不能只看配置")
                    .isTrue();
        } finally {
            singleConfigured.shutdown();
        }
    }

    @Test
    @DisplayName("生产路径拒绝：Cluster 上延时发送在写任何 key 之前被守卫拒绝（StreamMQException，零副作用）")
    void delayEnqueueOnCluster_refusedBeforeAnyWrite() {
        String topic = "cls-delay-refused";
        RedissonStreamProducer producer =
                new RedissonStreamProducer(redisson, namespace, GROUP, converter, 3000L, 0, 0, 0);
        try {
            // 固定等级延时与任意延时共用同一条登记原语（payload Hash + 调度 ZSet 跨 key 原子批）
            assertThatThrownBy(
                            () ->
                                    producer.syncSend(
                                            MessageBuilder.<String>withTopic(topic)
                                                    .body("delayed-body")
                                                    .delayLevel(DelayLevel.SECOND_1)
                                                    .build()))
                    .as("延时登记必须前置拒绝（而不是拆分提交后返回占位 ID 冒充'已受理'）")
                    .isInstanceOf(StreamMQException.class)
                    .hasMessageContaining("Delayed message enqueue")
                    .hasMessageContaining("requires cross-key atomicity");
            assertThatThrownBy(
                            () ->
                                    producer.syncSend(
                                            MessageBuilder.<String>withTopic(topic)
                                                    .body("custom-delayed-body")
                                                    .delayTimeMillis(5_000L)
                                                    .build()))
                    .as("任意延时（delayTimeMillis）走同一原语，必须同样被拒绝")
                    .isInstanceOf(StreamMQException.class)
                    .hasMessageContaining("Delayed message enqueue");

            // 零副作用：命名空间下不得出现任何延时 key（payload Hash / 调度 ZSet 条目），目标流也不得有写入
            assertThat(
                            redisson.getKeys()
                                    .getKeys(
                                            KeysScanOptions.defaults()
                                                    .pattern(
                                                            StreamMQKeys.prefix(namespace)
                                                                    + ":delay:*")))
                    .as("拒绝发生在第一次写入之前：不得残留 payload Hash 或调度条目")
                    .isEmpty();
            assertThat(redisson.getStream(StreamMQKeys.topicStream(namespace, topic)).size())
                    .as("目标流不得出现任何条目")
                    .isZero();
        } finally {
            producer.close();
        }
    }

    @Test
    @DisplayName("调度路径拒绝且 fail-safe：到期延时消息在 Cluster 上不被转投、也不被丢弃（调度状态原样保留）")
    void delayTransferOnCluster_refusedAndSchedulingStateIntact() {
        String topic = "cls-delay-transfer";
        String msgId = UUID.randomUUID().toString();
        String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
        String zsetKey = StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name());
        String streamKey = StreamMQKeys.topicStream(namespace, topic);

        // 预置由单 key 写入构造（Cluster 安全）：调度条目已到期（score=0）+ payload 完整，
        // 等价于"在集群上已登记成功的延时消息"（例如旧版本/旧配置升级后遗留的存量）
        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("body", "delay-transfer-body");
        payload.put(DelayMessageScheduler.FIELD_TARGET_TOPIC, topic);
        payload.put(DelayMessageScheduler.FIELD_DELIVER_AT, "0");
        RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey, StringCodec.INSTANCE);
        zset.add(0, msgId);
        redisson.getMap(payloadKey, StringCodec.INSTANCE).putAll(payload);

        DelayMessageScheduler scheduler = new DelayMessageScheduler(redisson, namespace, 200L, 16);
        try {
            scheduler.start();
            // 转投尝试的指纹：失败回写退避把 score 从 0 推后（DEFAULT_FAILURE_REQUEUE_BACKOFF_MS=5s）。
            // 这一步证明"调度器确实尝试过转投并被拒绝"，而不是"扫描还没轮到它"。
            await().atMost(10, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                Double score = zset.getScore(msgId);
                                return score != null && score > 0;
                            });
            // 转投被拒 = 零投递：目标流在整个观察窗内必须持续为空
            await().during(1, TimeUnit.SECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .until(() -> redisson.getStream(streamKey).size() == 0);

            assertThat(zset.contains(msgId)).as("调度条目必须原样保留（不丢弃、不半写）").isTrue();
            assertThat(redisson.getMap(payloadKey, StringCodec.INSTANCE).readAllMap())
                    .as("payload 必须完整保留，修复部署形态后可继续投递")
                    .isEqualTo(payload);
            assertThat(redisson.getStream(streamKey).size()).as("目标流不得有写入").isZero();
        } finally {
            scheduler.stop();
        }
    }

    @Test
    @DisplayName("事务路径拒绝：登记与提交均被守卫拒绝（零半消息、零状态残留），原生三 key 脚本确为 CROSSSLOT")
    void transactionPaths_refusedOnCluster() {
        String txGroup = "cls-tx-group";
        String targetTopic = "cls-tx-target";
        String txId = "tx-" + UUID.randomUUID();
        String stateHashKey = StreamMQKeys.transactionStateHash(namespace, txGroup);
        String checkZSetKey = StreamMQKeys.transactionCheckZSet(namespace, txGroup);
        String halfStreamKey = StreamMQKeys.halfStream(namespace, txGroup);
        String targetStreamKey = StreamMQKeys.topicStream(namespace, targetTopic);

        TransactionScanner scanner = new TransactionScanner(redisson, namespace, converter);

        // 1) 登记阶段：状态 Hash + 回查 ZSet 跨 key 原子批 → 在写半消息之前拒绝
        assertThatThrownBy(
                        () ->
                                scanner.registerHalfMessage(
                                        txId, txGroup, targetTopic, Map.of("body", "half-message")))
                .as("登记是状态 Hash + 回查 ZSet 的跨 key 操作，必须在写任何 key 之前拒绝")
                .isInstanceOf(StreamMQException.class)
                .hasMessageContaining("Transaction prepare metadata");
        assertThat(redisson.getStream(halfStreamKey).size()).as("拒绝发生在 XADD 之前：不得留下半消息").isZero();
        assertThat(redisson.getKeys().countExists(stateHashKey, checkZSetKey))
                .as("状态 Hash 与回查 ZSet 都不得被创建")
                .isZero();

        // 2) 提交阶段：半消息流 + 目标流 + 状态 Hash 三 key 脚本 → 前置拒绝且零副作用
        RStream<String, String> halfStream = redisson.getStream(halfStreamKey);
        StreamMessageId halfId =
                halfStream.add(StreamAddArgs.entries(Map.of("body", "half-message")));
        redisson.<String, String>getMap(stateHashKey, StringCodec.INSTANCE).put(txId, "UNKNOWN");
        TransactionCommitExecutor executor = new TransactionCommitExecutor(redisson, namespace);
        assertThatThrownBy(
                        () ->
                                executor.publishHalfAndMarkCommit(
                                        txGroup, halfId.toString(), targetTopic, txId))
                .as("提交是三个 key 家族的原子脚本，Cluster 配置下必须被守卫拒绝")
                .isInstanceOf(StreamMQException.class)
                .hasMessageContaining("Transaction commit");
        assertThat(
                        halfStream.range(
                                StreamRangeArgs.startId(StreamMessageId.MIN)
                                        .endId(StreamMessageId.MAX)))
                .as("提交被拒时半消息必须仍可回查（未被 XDEL），可走回滚/人工补偿")
                .containsKey(halfId);
        assertThat(redisson.getStream(targetStreamKey).size()).as("目标流不得有写入").isZero();
        assertThat(redisson.<String, String>getMap(stateHashKey, StringCodec.INSTANCE).get(txId))
                .as("状态位不得被改写")
                .isEqualTo("UNKNOWN");

        // 3) 结构性证据：原生三 key 脚本（与 LUA_COMMIT_AND_MARK 同 KEYS 形态）被真实服务端 CROSSSLOT
        //    拒绝——守卫在客户端提前拒绝的，正是这个"服务端必然拒绝"的操作。
        //    前置条件：三 key 实测跨 slot；半消息与 COMMITTING 状态齐备（失败原因只能是 key 落点）。
        assertThat(slotOf(halfStreamKey))
                .as("前置条件：三 key 必须跨 slot，否则脚本会执行成功")
                .isNotEqualTo(slotOf(targetStreamKey));
        assertThat(slotOf(stateHashKey)).isNotEqualTo(slotOf(targetStreamKey));
        String rawTxId = "tx-raw-" + UUID.randomUUID();
        redisson.<String, String>getMap(stateHashKey, StringCodec.INSTANCE)
                .put(rawTxId, "COMMITTING");
        String txShapeScript =
                "local current = redis.call('HGET', KEYS[3], ARGV[2]);if current ~= 'COMMITTING'"
                        + " then return 'ABORTED:' .. tostring(current); end;local entries ="
                        + " redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1]);if #entries == 0 then"
                        + " return 'HALF_MISSING'; end;redis.call('XADD', KEYS[2], '*',"
                        + " unpack(entries[1][2]));redis.call('XDEL', KEYS[1],"
                        + " ARGV[1]);redis.call('HSET', KEYS[3], ARGV[2], 'COMMIT');return"
                        + " 'PUBLISHED';";
        assertThatThrownBy(
                        () ->
                                redisson.getScript(StringCodec.INSTANCE)
                                        .eval(
                                                RScript.Mode.READ_WRITE,
                                                txShapeScript,
                                                RScript.ReturnType.STATUS,
                                                Arrays.asList(
                                                        halfStreamKey,
                                                        targetStreamKey,
                                                        stateHashKey),
                                                halfId.toString(),
                                                rawTxId))
                .as("三 key 原生脚本必须被真实服务端以 CROSSSLOT 拒绝")
                .hasStackTraceContaining("CROSSSLOT");
        assertThat(
                        halfStream.range(
                                StreamRangeArgs.startId(StreamMessageId.MIN)
                                        .endId(StreamMessageId.MAX)))
                .as("脚本被拒 = 零副作用：半消息仍在")
                .containsKey(halfId);
        assertThat(redisson.getStream(targetStreamKey).size()).as("目标流仍不得有写入").isZero();
        assertThat(redisson.<String, String>getMap(stateHashKey, StringCodec.INSTANCE).get(rawTxId))
                .as("状态位不得被改写（脚本未执行）")
                .isEqualTo("COMMITTING");
    }

    @Test
    @DisplayName("PEL 认领脚本形态：同流重投（KEYS[1]==KEYS[2]）Cluster 安全，跨流认领被 CROSSSLOT 拒绝")
    void claimScriptShape_sameStreamVsCrossStream() {
        String streamKey = StreamMQKeys.topicStream(namespace, TOPIC);
        RStream<String, String> stream = redisson.getStream(streamKey);
        StreamMessageId id = stream.add(StreamAddArgs.entries(Map.of("body", "claim-body")));
        stream.createGroup(
                StreamCreateGroupArgs.name(GROUP).makeStream().id(new StreamMessageId(0, 0)));
        stream.readGroup(GROUP, CONSUMER, StreamReadGroupArgs.neverDelivered().count(10));

        // PelClaimScheduler#xaddAndAck 的脚本形态：XACK 源流旧条目（ARGV[1]=group, ARGV[2]=id）+
        // XADD 副本到目标流。常规重投目标与源相同（KEYS[1]==KEYS[2]）→ 单 distinct key → Cluster 安全。
        String claimScript =
                "if redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) == 1 then"
                        + "  return redis.call('XADD', KEYS[2], '*', 'body', ARGV[3])"
                        + " end return 0";
        Object sameStream =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                claimScript,
                                RScript.ReturnType.STATUS,
                                List.of(streamKey, streamKey),
                                GROUP,
                                id.toString(),
                                "claim-body");
        assertThat(sameStream)
                .as("同流认领重投：XACK + XADD 命中同一 key，Cluster 上必须成功（返回新 entry id）")
                .isNotNull();
        assertThat(
                        stream.range(
                                StreamRangeArgs.startId(StreamMessageId.MIN)
                                        .endId(StreamMessageId.MAX)))
                .hasSize(2);

        // 跨流认领（源流 vs 重试/死信流，如转投重试流）：两 key 分属不同 slot → 被服务端拒绝
        String retryStreamKey = StreamMQKeys.retryStream(namespace, TOPIC, GROUP);
        assertThat(slotOf(streamKey)).isNotEqualTo(slotOf(retryStreamKey));
        assertThatThrownBy(
                        () ->
                                redisson.getScript(StringCodec.INSTANCE)
                                        .eval(
                                                RScript.Mode.READ_WRITE,
                                                claimScript,
                                                RScript.ReturnType.STATUS,
                                                List.of(streamKey, retryStreamKey),
                                                id.toString(),
                                                "claim-body"))
                .as("跨流认领（源流与目标流不在同一 slot）必须被 CROSSSLOT 拒绝")
                .hasStackTraceContaining("CROSSSLOT");
    }

    @Test
    @DisplayName("绕过守卫的对照测量 A：同 master 跨 slot 的原子批在 EXEC 时被 CROSSSLOT 拒绝（零半写）")
    void atomicBatchBypassingGuard_sameNodeIsRejectedWithCrossSlot() {
        String[] pair = pickKeyPairOnSameNode();
        assertThat(slotOwners().get(slotOf(pair[0])))
                .as("前置条件：两 key 必须由同一 master 负责（%s vs %s）", pair[0], pair[1])
                .isEqualTo(slotOwners().get(slotOf(pair[1])));
        assertThat(slotOf(pair[0])).isNotEqualTo(slotOf(pair[1]));

        System.out.println(
                "[StreamMQ IT] REDIS_WRITE_ATOMIC 同 master 跨 slot 多 key 批："
                        + slotOwners().get(slotOf(pair[0]))
                        + "（slot "
                        + slotOf(pair[0])
                        + " / "
                        + slotOf(pair[1])
                        + "）");
        // 刻意绕过守卫直呼批原语：这正是守卫替用户拒绝的形态（守住之前它就是这样被静默执行的）
        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getStream(pair[0], StringCodec.INSTANCE)
                .addAsync(StreamAddArgs.entries(Map.of("body", "atomic-same-node")));
        batch.<String>getScoredSortedSet(pair[1], StringCodec.INSTANCE)
                .addAsync(0, "atomic-same-node");

        assertThatThrownBy(batch::execute)
                .as("同一 master 上的多 key 原子批被编组为单个 MULTI/EXEC，服务端在 EXEC 时以 CROSSSLOT 拒绝")
                .hasStackTraceContaining("CROSSSLOT");
        assertThat(redisson.getKeys().countExists(pair[0], pair[1]))
                .as("EXEC 被拒 = 零半写：两个 key 都不允许出现")
                .isZero();
    }

    @Test
    @DisplayName("绕过守卫的对照测量 B：跨 master 的原子批不报错但被静默拆分（原子性事实丢失）")
    void atomicBatchBypassingGuard_differentNodesIsSilentlySplit() {
        String[] pair = pickKeyPairOnDifferentNodes();
        assertThat(slotOwners().get(slotOf(pair[0])))
                .as("前置条件：两 key 必须由不同 master 负责（%s vs %s）", pair[0], pair[1])
                .isNotEqualTo(slotOwners().get(slotOf(pair[1])));

        RBatch batch =
                redisson.createBatch(
                        BatchOptions.defaults()
                                .executionMode(BatchOptions.ExecutionMode.REDIS_WRITE_ATOMIC));
        batch.<String, String>getStream(pair[0], StringCodec.INSTANCE)
                .addAsync(StreamAddArgs.entries(Map.of("body", "atomic-cross-node")));
        batch.<String>getScoredSortedSet(pair[1], StringCodec.INSTANCE)
                .addAsync(0, "atomic-cross-node");

        // 不抛异常——"静默"的含义：调用方看到的是一次成功的原子批，实际是两个独立事务
        batch.execute();
        System.out.println(
                "[StreamMQ IT] REDIS_WRITE_ATOMIC 跨 master 多 key 批：实测不报错、两 key 均已写入（"
                        + slotOwners().get(slotOf(pair[0]))
                        + " + "
                        + slotOwners().get(slotOf(pair[1]))
                        + "）——单条 MULTI/EXEC 不可能跨 master，故这不是原子提交");
        assertThat(redisson.getKeys().countExists(pair[0], pair[1]))
                .as("批未抛异常且两个写入均落库：Redisson 按节点拆分为两个独立事务，跨 key 原子性已被静默丢弃")
                .isEqualTo(2L);
    }

    /**
     * 选取一个 topic：其业务流与重试 ZSet 实测落在不同 slot。
     *
     * <p>同 slot 的概率约 1/16384，但测量不靠运气——循环换 topic 名直到实测确认， 保证 {@link
     * #multiKeyScript_failsWithCrossSlot_andLeavesMessagePending} 的前置条件成立。
     */
    private String pickTopicSpanningSlots() {
        for (int attempt = 0; attempt < 32; attempt++) {
            String topic = "cls-xslot-" + attempt;
            int streamSlot = slotOf(StreamMQKeys.topicStream(namespace, topic));
            int retrySlot = slotOf(StreamMQKeys.retryZSet(namespace, topic, GROUP));
            if (streamSlot != retrySlot) {
                return topic;
            }
        }
        throw new IllegalStateException(
                "no topic spans 2 slots after 32 attempts; namespace=" + namespace);
    }

    /**
     * 寻找一对"同一 master、不同 slot"的候选 key。
     *
     * <p>同 master 跨 slot 是原子批被服务端 {@code CROSSSLOT} 拒绝的确切条件；判定依据是服务端 {@code CLUSTER SLOTS} 的
     * slot→节点映射，而不是"3 主按 5461 均分"这类布局假设。
     */
    private String[] pickKeyPairOnSameNode() {
        Map<String, String> firstKeyByNode = new HashMap<>();
        for (int attempt = 0; attempt < 64; attempt++) {
            String key = StreamMQKeys.topicStream(namespace, "cls-place-" + attempt);
            String node = slotOwners().get(slotOf(key));
            String previous = firstKeyByNode.putIfAbsent(node, key);
            if (previous != null && slotOf(previous) != slotOf(key)) {
                return new String[] {previous, key};
            }
        }
        throw new IllegalStateException(
                "no same-node key pair with different slots found; namespace=" + namespace);
    }

    /** 寻找一对"不同 master"的候选 key（跨 master ⇒ 跨 slot ⇒ 必经按节点拆分路径）。 */
    private String[] pickKeyPairOnDifferentNodes() {
        Map<String, String> firstKeyByNode = new LinkedHashMap<>();
        for (int attempt = 0; attempt < 64 && firstKeyByNode.size() < 2; attempt++) {
            String key = StreamMQKeys.topicStream(namespace, "cls-place-" + attempt);
            firstKeyByNode.putIfAbsent(slotOwners().get(slotOf(key)), key);
        }
        if (firstKeyByNode.size() < 2) {
            throw new IllegalStateException(
                    "expected at least 2 masters handling candidate keys; namespace=" + namespace);
        }
        Iterator<String> keys = firstKeyByNode.values().iterator();
        return new String[] {keys.next(), keys.next()};
    }

    /**
     * slot → 负责节点（host:port）映射，来自真实服务端 {@code CLUSTER SLOTS}。
     *
     * <p>刻意不假设"3 主按 5461 均分"：只有服务端自报的映射才能支撑"同节点 / 跨节点"这两条确定性前置条件。
     */
    private Map<Integer, String> slotOwners() {
        if (cachedSlotOwners != null) {
            return cachedSlotOwners;
        }
        List<Object> ranges =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                LUA_CLUSTER_SLOTS,
                                RScript.ReturnType.MULTI,
                                List.of());
        Map<Integer, String> owners = new HashMap<>();
        for (Object rangeObj : new ArrayList<>(ranges)) {
            List<?> range = (List<?>) rangeObj;
            int start = ((Number) range.get(0)).intValue();
            int end = ((Number) range.get(1)).intValue();
            List<?> master = (List<?>) range.get(2);
            String node = master.get(0) + ":" + master.get(1);
            for (int slot = start; slot <= end; slot++) {
                owners.put(slot, node);
            }
        }
        assertThat(owners).as("CLUSTER SLOTS 必须覆盖全部 16384 个 slot（服务端权威映射）").hasSize(16384);
        cachedSlotOwners = owners;
        return cachedSlotOwners;
    }

    /**
     * 由真实服务端计算 key 的 slot：{@code EVAL "return redis.call('CLUSTER','KEYSLOT',KEYS[1])" 1 key}。
     *
     * <p>刻意用 {@link RScript.Mode#READ_WRITE} 强制路由到 master：KEYSLOT 本身只读，但 READ_ONLY 模式下 Redisson 会按
     * readMode 选副本，在没有副本的 3 主集群上只会引入额外的不确定性。
     */
    private int slotOf(String key) {
        Long slot =
                redisson.getScript(StringCodec.INSTANCE)
                        .eval(
                                RScript.Mode.READ_WRITE,
                                "return redis.call('CLUSTER','KEYSLOT',KEYS[1])",
                                RScript.ReturnType.INTEGER,
                                List.of(key));
        assertThat(slot).as("CLUSTER KEYSLOT 必须返回真实 slot：key=%s", key).isNotNull();
        return slot.intValue();
    }

    /** 读取集群节点列表：系统属性优先，缺省 {@link #DEFAULT_CLUSTER_NODES}。 */
    private static List<String> configuredNodes() {
        String configured = System.getProperty(CLUSTER_NODES_PROPERTY, DEFAULT_CLUSTER_NODES);
        return Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(node -> !node.isEmpty())
                .toList();
    }

    /** 首个配置节点（host:port）——用于"单机配置误指向 Cluster 节点"的探针兜底用例。 */
    private static String firstNode() {
        return configuredNodes().get(0);
    }

    /**
     * 单节点可用性探测：TCP PING → 期望 {@code +PONG}。
     *
     * <p>与 {@code AbstractRedisIT#isRedisAvailable} 同一手法，此处按节点独立实现（基类方法为 private，
     * 且本类不继承单实例客户端装配）。格式非法的节点按"不可达"处理，走 skip 提示而非崩溃。
     */
    private static boolean isNodeAvailable(String node) {
        int separator = node.lastIndexOf(':');
        if (separator <= 0 || separator == node.length() - 1) {
            return false;
        }
        String host = node.substring(0, separator);
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(host, Integer.parseInt(node.substring(separator + 1))),
                    500);
            socket.setSoTimeout(300);
            OutputStream out = socket.getOutputStream();
            out.write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return readsPong(socket.getInputStream());
        } catch (IOException | NumberFormatException ex) {
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
