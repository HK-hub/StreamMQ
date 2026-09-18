/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.DlqReason;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;

/**
 * PelClaim 认领补偿的关键回归（B-13）：目的键类型冲突（WRONGTYPE）不得造成消息丢失。
 *
 * <p>场景：业务流 PEL 中滞留一条已超限消息，本该「XACK 旧条目 + XADD 到 DLQ」——但 DLQ 目的键被字符串占用。 Redis Lua 无回滚语义：若脚本先 XACK 再
 * XADD 失败，条目会从 PEL 消失且副本未写入（静默丢失）。
 *
 * <p>本 IT 锁定两条不可退让的行为：
 *
 * <ol>
 *   <li><b>认领不发生</b>：目的键类型自检失败时提前放弃整轮认领——条目仍留在 PEL，消息不丢；
 *   <li><b>恢复后自动继续</b>：目的键恢复为 stream 后条目被正常认领并转入 DLQ，无需人工干预。
 * </ol>
 *
 * <p>这是"失败即红"的回归：任何移除类型自检、或让认领先于写入的改动都会让本用例失败。
 */
@DisplayName("PelClaim 目的键 WRONGTYPE 认领补偿")
class PelClaimCompensationIT extends AbstractRedisIT {

    private static final String TOPIC = "comp-topic";
    private static final String GROUP = "comp-group";

    /** 消费者名遵循 {group}-{instanceId} 约定：实例崩溃后其 PEL 遗留由认领调度器恢复。 */
    private static final String DEAD_CONSUMER = BroadcastGroupNaming.consumerName(GROUP, "dead-1");

    @Test
    @DisplayName("DLQ 目的键为字符串：认领不发生且条目留存 PEL；恢复键类型后条目转入 DLQ")
    void wrongTypeDestination_defersClaimThenRecovers() throws InterruptedException {
        String streamKey = StreamMQKeys.topicStream(namespace, TOPIC);
        String dlqKey = StreamMQKeys.dlqStream(namespace, GROUP);
        RStream<String, String> stream = redisson.getStream(streamKey);
        RStream<String, String> dlqStream = redisson.getStream(dlqKey);

        // 业务流：1 条消息 + 消费者组 + 由"已死消费者"读取制造 PEL 滞留
        stream.add(StreamAddArgs.entries(Map.of("body", "payload", "retryTimes", "0")));
        stream.createGroup(
                StreamCreateGroupArgs.name(GROUP).makeStream().id(new StreamMessageId(0, 0)));
        stream.readGroup(GROUP, DEAD_CONSUMER, StreamReadGroupArgs.neverDelivered().count(10));

        // 制造 WRONGTYPE：DLQ 目的键被字符串占用（maxReconsumeTimes=0 ⇒ 条目本应转 DLQ）
        RBucket<String> occupyingBucket = redisson.getBucket(dlqKey);
        occupyingBucket.set("occupied-by-string");

        PelClaimScheduler scheduler = new PelClaimScheduler(redisson, namespace, 100, 32, 50);
        scheduler.registerTarget(namespace, TOPIC, GROUP, 0);
        scheduler.start();
        try {
            // ① 认领不发生：经历多轮扫描（每轮目的键类型自检失败 → 放弃本轮全部认领）。
            // minIdle=50ms / scan=100ms，等待 800ms 足以覆盖多轮扫描。
            Thread.sleep(800L);
            assertThat(stream.size()).as("不得复制重投（目的键不可写时放弃认领）").isEqualTo(1);
            assertThat(pendingCount(stream)).as("条目必须留在 PEL：消息不丢").isEqualTo(1);
            assertThat(occupyingBucket.get())
                    .as("字符串占用的 DLQ 键不得被认领逻辑改写（目的键仍为字符串即为未写入）")
                    .isEqualTo("occupied-by-string");

            // ② 恢复键类型（删除字符串占用 → 目的键回到"不存在"= 可写）后条目被正常认领转 DLQ
            occupyingBucket.delete();

            await().atMost(10, TimeUnit.SECONDS).until(() -> dlqStream.size() == 1);
            Map<StreamMessageId, Map<String, String>> dlqEntries =
                    dlqStream.range(StreamMessageId.MIN, StreamMessageId.MAX);
            assertThat(dlqEntries).hasSize(1);
            assertThat(dlqEntries.values().iterator().next())
                    .as("转 DLQ 的条目必须带 dlqReason 且保留原 body（认领后必须有下落）")
                    .containsEntry(
                            RetryScheduler.FIELD_DLQ_REASON, DlqReason.MAX_RETRY_ORDERLY.getCode())
                    .containsEntry("body", "payload");
            await().atMost(5, TimeUnit.SECONDS).until(() -> pendingCount(stream) == 0);
        } finally {
            scheduler.stop();
        }
    }

    /** PEL 滞留条目数（XPENDING 摘要）。 */
    private int pendingCount(RStream<String, String> stream) {
        List<?> pending = stream.listPending(GROUP, StreamMessageId.MIN, StreamMessageId.MAX, 100);
        return pending.size();
    }
}
