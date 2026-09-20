/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link OrderlyDeferredRetryQueue} 单元测试（红队第六轮 R1-1）。
 *
 * <p>覆盖：单飞（同一 messageId 只保留一个条目）、锁竞争退避（1s 起、翻倍、上限 30s）、消费端 defer 的显式 延迟、容量溢出（保留在 PEL + 计数，绝不静默）、同分片
 * FIFO（含重投后仍保持登记顺序）。
 */
@DisplayName("顺序消费延迟重投队列")
class OrderlyDeferredRetryQueueTest {

    @Test
    @DisplayName("单飞 + 退避：重复登记只更新到期时间，不产生重复条目；退避不足期不重投")
    void singleFlight_andBackoff() {
        OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
        ListenerRegistration<?> reg = reg("single-flight", 4);
        Message<?> message = message("1-1", "k1");

        assertThat(queue.deferShardBusy(reg, message)).isTrue();
        // 单飞：第二次登记（如再次竞争）不新增条目
        assertThat(queue.deferShardBusy(reg, message)).isTrue();
        assertThat(queue.size(reg)).isEqualTo(1);
        assertThat(queue.registeredCount()).isEqualTo(2);

        long now = System.currentTimeMillis();
        // 两次登记后 deferEpoch=1 → 退避 2s：1.1s 后仍未到期，2.1s 后到期
        assertThat(queue.dueCount(reg, now)).isZero();
        assertThat(queue.dueCount(reg, now + 1_100L)).isZero();
        assertThat(queue.dueCount(reg, now + 2_100L)).isEqualTo(1);

        // 退避曲线：deferEpoch=0 → 1s；1 → 2s；2 → 4s…上限 30s
        assertThat(OrderlyDeferredRetryQueue.backoffMillis(0)).isEqualTo(1_000L);
        assertThat(OrderlyDeferredRetryQueue.backoffMillis(1)).isEqualTo(2_000L);
        assertThat(OrderlyDeferredRetryQueue.backoffMillis(2)).isEqualTo(4_000L);
        assertThat(OrderlyDeferredRetryQueue.backoffMillis(5)).isEqualTo(30_000L);
        assertThat(OrderlyDeferredRetryQueue.backoffMillis(50)).isEqualTo(30_000L);
    }

    @Test
    @DisplayName("重投成功即移除条目；重投中再次竞争则保留并退避（不丢条目）")
    void drainDue_removesOnSuccess_keepsOnReDefer() {
        OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
        ListenerRegistration<?> reg = reg("drain", 4);
        Message<?> message = message("2-2", "k1");
        queue.deferShardBusy(reg, message);
        long firstDue = System.currentTimeMillis() + 1_100L;

        // 第一次重投：仍竞争 → dispatcher 再次登记（模拟分片锁仍被占用）
        List<String> attempts = new ArrayList<>();
        queue.drainDue(
                reg,
                firstDue,
                mock(io.github.streammq.core.listener.StreamMQListener.class),
                (entry, r, listener) -> {
                    attempts.add("busy:" + entry.messageId());
                    queue.deferShardBusy(reg, entry.message());
                });
        assertThat(attempts).containsExactly("busy:2-2");
        assertThat(queue.size(reg)).isEqualTo(1);
        long afterFirstRetry = System.currentTimeMillis();
        assertThat(queue.dueCount(reg, afterFirstRetry + 500L)).as("再次竞争后必须退避").isZero();
        assertThat(queue.dueCount(reg, afterFirstRetry + 2_100L)).as("第二次退避 2s 后到期").isEqualTo(1);

        // 第二次重投：成功（dispatcher 不再登记）→ 条目移除
        queue.drainDue(
                reg,
                afterFirstRetry + 2_100L,
                mock(io.github.streammq.core.listener.StreamMQListener.class),
                (entry, r, listener) -> attempts.add("ok:" + entry.messageId()));
        assertThat(attempts).containsExactly("busy:2-2", "ok:2-2");
        assertThat(queue.size(reg)).isZero();
        assertThat(queue.redeliveryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("消费端 defer(delay) 按显式延迟到期；DLQ 转投失败条目为 DLQ_ROUTE 种类")
    void explicitDeferAndDlqRouteKind() {
        OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
        ListenerRegistration<?> reg = reg("defer", 4);
        Message<?> deferred = message("3-3", "k1");

        queue.deferByAction(reg, deferred, 5_000L);
        long now = System.currentTimeMillis();
        assertThat(queue.dueCount(reg, now + 100L)).as("defer 延迟内不得提前重投").isZero();
        assertThat(queue.dueCount(reg, now + 5_100L)).isEqualTo(1);

        Message<?> dlqMessage = message("3-4", "k1");
        queue.deferDlqRouteFailure(reg, dlqMessage);
        List<String> kinds = new ArrayList<>();
        queue.drainDue(
                reg,
                now + 6_000L,
                mock(io.github.streammq.core.listener.StreamMQListener.class),
                (entry, r, listener) -> kinds.add(entry.kind().name()));
        assertThat(kinds).containsExactly("REPROCESS", "DLQ_ROUTE");
    }

    @Test
    @DisplayName("同分片严格 FIFO：按登记顺序重投，跨分片互不阻塞")
    void fifoPerShard() {
        OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
        ListenerRegistration<?> reg = reg("fifo", 4);
        Message<?> a = message("4-1", "k1");
        Message<?> b = message("4-2", "k1");
        Message<?> c = message("4-3", "k1");
        Message<?> otherShard = message("4-4", "k2");
        assertThat(shardOf(reg, a)).isEqualTo(shardOf(reg, b)).isEqualTo(shardOf(reg, c));

        queue.deferShardBusy(reg, a);
        queue.deferShardBusy(reg, b);
        queue.deferShardBusy(reg, otherShard);
        queue.deferShardBusy(reg, c);

        List<String> order = new ArrayList<>();
        queue.drainDue(
                reg,
                System.currentTimeMillis() + 60_000L,
                mock(io.github.streammq.core.listener.StreamMQListener.class),
                (entry, r, listener) -> order.add(entry.messageId()));

        // 同分片按登记顺序（FIFO）；另一分片可插在中间但不得越过同分片前序消息
        assertThat(order).containsExactly("4-1", "4-2", "4-4", "4-3");
    }

    @Test
    @DisplayName("容量溢出：不登记（消息保留在 PEL）、计数 + 限频 ERROR，绝不静默丢弃")
    void capacityOverflow_isRejectedNotSilentlyDropped() {
        OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
        queue.setMaxEntriesPerRegistration(1);
        ListenerRegistration<?> reg = reg("overflow", 4);

        assertThat(queue.deferShardBusy(reg, message("5-1", "k1"))).isTrue();
        assertThat(queue.deferShardBusy(reg, message("5-2", "k1"))).isFalse();

        assertThat(queue.size(reg)).isEqualTo(1);
        assertThat(queue.rejectedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("无 messageId 的条目不入队（无法单飞去重，保持 PEL 兜底语义）")
    void messageWithoutMessageId_isNotQueued() {
        OrderlyDeferredRetryQueue queue = new OrderlyDeferredRetryQueue();
        ListenerRegistration<?> reg = reg("no-id", 4);
        Message<?> noId = MessageBuilder.<String>withTopic("t").body("b").build();

        assertThat(queue.deferShardBusy(reg, noId)).isFalse();
        assertThat(queue.size(reg)).isZero();
    }

    // ===================== 夹具 =====================

    private static ListenerRegistration<?> reg(String name, int shardCount) {
        ListenerRegistration<?> reg = mock(ListenerRegistration.class);
        when(reg.key()).thenReturn("t-" + name + ":g-" + name);
        when(reg.getTopic()).thenReturn("t-" + name);
        when(reg.getGroup()).thenReturn("g-" + name);
        when(reg.getShardCount()).thenReturn(shardCount);
        return reg;
    }

    private static Message<?> message(String messageId, String shardingKey) {
        return MessageBuilder.<String>withTopic("t")
                .body("b")
                .shardingKey(shardingKey)
                .messageId(MessageId.fromStreamEntry(messageId))
                .build();
    }

    private static int shardOf(ListenerRegistration<?> reg, Message<?> message) {
        String key = message.getShardingKey() == null ? "" : message.getShardingKey();
        return (key.hashCode() & 0x7fffffff) % reg.getShardCount();
    }
}
