/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListenerFactory;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.support.BroadcastGroupNaming;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.DlqReason;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;

/**
 * DLQ 实例心跳与 PEL 判活的集成回归（B-10）。
 *
 * <p>缺陷形态：DLQ 注册<b>不建组管理器</b>（无实例心跳行），而 {@code PelClaimScheduler} 的 DLQ 目标按 {@code
 * consumerGroupInstances(group)} 精确匹配属主实例判活——于是"活跃慢 DLQ 消费者"（handler 阻塞期间无法 心跳） 的 pending
 * 会被尾部复制重投，造成重复消费；DLQ-only 部署（无同组业务消费者）尤其必现。
 *
 * <p>本 IT 同时锁定两个方向，缺一不可：
 *
 * <ol>
 *   <li>DLQ 注册也必须写实例心跳，且心跳新鲜时 DLQ 目标<b>不</b>认领活跃消费者的 pending；
 *   <li>无心跳的遗留 pending（实例已死）仍被正常恢复重投——修复不得把 DLQ 恢复能力一并掐死。
 * </ol>
 */
@DisplayName("DLQ 实例心跳与活跃消费者保护")
class DlqInstanceHeartbeatIT extends AbstractRedisIT {

    private static final String GROUP = "dlq-live-group";
    private static final String GHOST_GROUP = "dlq-ghost-group";
    private static final String INSTANCE_TOKEN = "dlq-live-inst";

    /** 心跳间隔远小于判活窗口，避免"心跳恰好未续期"造成假阴性。 */
    private static final long HEARTBEAT_INTERVAL_MS = 100L;

    /** 判活窗口（同时是 PEL 空闲阈值）：2s。 */
    private static final long MIN_IDLE_MS = 2_000L;

    @Test
    @DisplayName("活跃 DLQ 消费者（心跳新鲜）不被复制重投；无心跳的遗留条目仍被恢复")
    void activeDlqConsumer_notCopied_ghostLegacyStillRecovered() throws Exception {
        String dlqKey = StreamMQKeys.dlqStream(namespace, GROUP);
        RStream<String, String> dlqStream = redisson.getStream(dlqKey);
        // 先建组（id=0-0：随后注入的死信可被 DLQ 消费者读取），再注入一条死信
        dlqStream.createGroup(
                StreamCreateGroupArgs.name(GROUP).makeStream().id(new StreamMessageId(0, 0)));
        dlqStream.add(StreamAddArgs.entries(dlqEntryFields("dlq-payload")));

        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch handlerRelease = new CountDownLatch(1);
        DefaultStreamMQListenerContainer container = newContainer();
        container.setInstanceToken(INSTANCE_TOKEN);
        container.setHeartbeatIntervalMs(HEARTBEAT_INTERVAL_MS);
        container.registerConsumer(
                dlqHandler(handlerEntered, handlerRelease), dlqModeAnnotationOf(GROUP));

        PelClaimScheduler scheduler =
                new PelClaimScheduler(redisson, namespace, 100, 32, MIN_IDLE_MS);
        try {
            container.start();

            // ① 修复点：DLQ 注册同样建组管理器 → instances Hash 出现该实例心跳行
            //    （修复前 DLQ 注册被整体跳过，此处永远等不到 => 判活恒 false）
            RMap<String, String> instances =
                    redisson.getMap(
                            StreamMQKeys.consumerGroupInstances(namespace, GROUP),
                            StringCodec.INSTANCE);
            await().atMost(5, TimeUnit.SECONDS).until(() -> instances.containsKey(INSTANCE_TOKEN));

            // ② DLQ 消费者读到死信并在 handler 中阻塞：活跃慢消费者（消息留在 PEL 未 ACK）
            assertThat(handlerEntered.await(10, TimeUnit.SECONDS))
                    .as("DLQ 消费者应开始处理死信（handler 阻塞中）")
                    .isTrue();
            assertThat(pendingCount(dlqStream, GROUP)).isEqualTo(1);

            // ③ 心跳新鲜 ⇒ 不认领：等待若干个判活窗口（>2×minIdle）后不得出现副本
            scheduler.registerDlqTarget(namespace, GROUP, GROUP);
            scheduler.start();
            Thread.sleep(MIN_IDLE_MS + 1_200L);
            assertThat(dlqStream.size()).as("活跃 DLQ 消费者（心跳新鲜）不得被复制重投").isEqualTo(1);
            assertThat(pendingCount(dlqStream, GROUP)).as("条目必须仍在 PEL，不得被认领后丢给二级路径").isEqualTo(1);

            // ④ 反向：无任何心跳的遗留组（模拟实例崩溃后再无心跳）仍被恢复重投
            String ghostKey = StreamMQKeys.dlqStream(namespace, GHOST_GROUP);
            RStream<String, String> ghostStream = redisson.getStream(ghostKey);
            ghostStream.createGroup(
                    StreamCreateGroupArgs.name(GHOST_GROUP)
                            .makeStream()
                            .id(new StreamMessageId(0, 0)));
            ghostStream.add(StreamAddArgs.entries(dlqEntryFields("ghost-payload")));
            ghostStream.readGroup(
                    GHOST_GROUP,
                    BroadcastGroupNaming.consumerName(GHOST_GROUP, "ghost-inst"),
                    StreamReadGroupArgs.neverDelivered().count(10));
            scheduler.registerDlqTarget(namespace, GHOST_GROUP, GHOST_GROUP);

            await().atMost(10, TimeUnit.SECONDS).until(() -> ghostStream.size() == 2);
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> pendingCount(ghostStream, GHOST_GROUP) == 0);
        } finally {
            scheduler.stop();
            // 先放行阻塞中的 handler，再停容器：避免停机窗口内把 handler 中断误当成消费失败
            handlerRelease.countDown();
            container.stop();
            redisson.getMap(StreamMQKeys.consumerGroupInstances(namespace, GROUP)).delete();
        }
    }

    /** 组装测试容器（StringCodec 客户端；DLQ 消费者为唯一注册项，模拟"DLQ 单独部署"）。 */
    private DefaultStreamMQListenerContainer newContainer() {
        return new DefaultStreamMQListenerContainer(
                redisson,
                new RedissonStreamListenerFactory(redisson, converter),
                converter,
                new DlqConsumerIT.NoRetryPolicy(),
                namespace);
    }

    /**
     * 构造一条"框架语义等同"的死信 Entry 字段：body 等字段由生产端 converter 生成， 保证 DLQ 消费者能正常解码 （手工 {@code Map.of("body",
     * ...)} 缺少 bodyType / props，消费者会在解码阶段失败而进不到 handler）。
     */
    private Map<String, String> dlqEntryFields(String body) {
        Map<String, String> fields =
                converter.toStreamFields(
                        MessageBuilder.<String>withTopic(GROUP).body(body).build());
        fields.put(RetryScheduler.FIELD_DLQ_REASON, DlqReason.MAX_RETRY.getCode());
        return fields;
    }

    /** DLQ handler：进入即计数，随后阻塞直到测试放行（模拟慢处理中的活跃消费者）。 */
    private static StreamMessageConcurrentlyConsumer<String> dlqHandler(
            CountDownLatch entered, CountDownLatch release) {
        return (message, context) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return ConsumeAction.SUCCESS;
        };
    }

    /**
     * 通过动态代理构造 {@code dlqMode=true} 的 {@link StreamMQConsumer} 注解（与 DlqConsumerIT 同路： 该注册路径已被既有 IT
     * 覆盖，避免新引入未验证的注解装配面）。
     */
    private static StreamMQConsumer dlqModeAnnotationOf(String group) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "topic" -> group;
                                    case "consumerGroup" -> group;
                                    case "consumeMode" -> ConsumeMode.CLUSTERING;
                                    case "messageModel" -> MessageModel.CONCURRENT;
                                    case "maxReconsumeTimes" -> 0;
                                    case "consumeThreadMin" -> 1;
                                    case "consumeThreadMax" -> 64;
                                    case "consumeTimeout" -> 30000L;
                                    case "selectorExpression" -> "*";
                                    case "serializer" -> MessageSerializer.class;
                                    case "namespace" -> "";
                                    case "enable" -> true;
                                    case "selectorType" -> SelectorType.TAG;
                                    case "pullBatchSize" -> 32;
                                    case "retryPolicy" -> RetryPolicy.class;
                                    case "enableMsgTrace" -> false;
                                    case "streamMaxLen" -> 0;
                                    case "messageConverter" -> MessageConverter.class;
                                    case "rebalanceStrategy" -> RebalanceStrategy.class;
                                    case "pullInterval" -> 0L;
                                    case "suspendCurrentQueueTimeMillis" -> 1000L;
                                    case "shardCount" -> 4;
                                    case "dlqMode" -> true;
                                    case "consumerName" -> "";
                                    case "annotationType" -> StreamMQConsumer.class;
                                    case "hashCode" -> group.hashCode();
                                    case "equals" ->
                                            args != null && args.length > 0 && proxy == args[0];
                                    case "toString" ->
                                            "@StreamMQConsumer(consumerGroup=" + group + ")";
                                    default -> defaultAnnotationValue(method.getReturnType());
                                });
    }

    /** 注解属性默认值（与注解声明默认值对齐；新增属性时测试代理不会崩）。 */
    private static Object defaultAnnotationValue(Class<?> returnType) {
        if (returnType == String.class) return "";
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == boolean.class) return false;
        if (returnType == Class.class) return null;
        return null;
    }

    /** 指定消费组的 PEL 滞留条目数（XPENDING 摘要）。 */
    private int pendingCount(RStream<String, String> stream, String group) {
        return stream.listPending(group, StreamMessageId.MIN, StreamMessageId.MAX, 100).size();
    }
}
