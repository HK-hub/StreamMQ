/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * 延时消息写入侧的时间基准回归测试（R2-4）。
 *
 * <p>历史缺陷：{@code deliverAt = System.currentTimeMillis() + delay} 用本机时钟，而调度器扫描侧改用 Redis 服务器
 * 时钟判定到期——跨主机 NTP 偏差（数十秒量级）会把延时时长整体平移（拨快的实例提前投递）。本测试注入可控的「服务器 时钟」，断言下发到 Redis 的延时 ZSet score（=
 * deliverAt）由服务器时钟推导，而不是本机时钟。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("延时消息写入侧使用 Redis 服务器时钟（R2-4）")
class RedissonStreamProducerDelayClockTest {

    private static final String NAMESPACE = "delay-clock-ns";
    private static final String TOPIC = "delay-clock-topic";

    /** 可辨识的假服务器时钟（与本机时钟相差约 30 年，绝无混淆可能） */
    private static final long FAKE_SERVER_NOW = 1_800_000_000_000L;

    private static final long DELAY_MILLIS = 5_000L;

    private RedissonClient redisson;
    private RScript script;
    private RBatch batch;
    private RScoredSortedSetAsync<String> zsetAsync;
    private RedissonStreamProducer producer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        script = mock(RScript.class);
        batch = mock(RBatch.class);
        zsetAsync = mock(RScoredSortedSetAsync.class);
        doReturn(script).when(redisson).getScript(StringCodec.INSTANCE);
        doReturn(batch).when(redisson).createBatch(any(BatchOptions.class));
        doReturn(mock(RMapAsync.class))
                .when(batch)
                .<String, String>getMap(anyString(), eq(StringCodec.INSTANCE));
        doReturn(zsetAsync)
                .when(batch)
                .<String>getScoredSortedSet(anyString(), eq(StringCodec.INSTANCE));
        MessageConverter converter = new DefaultMessageConverter(new JacksonJsonSerializer<>());
        producer = new RedissonStreamProducer(redisson, NAMESPACE, "g", converter, 3000L, 0, 0, 0);
    }

    @Test
    @DisplayName("R2-4：延时 ZSet score = 服务器时钟 + 延时时长（不是本机时钟）")
    void sendDelayMessage_deliverAtDerivedFromServerClock() {
        RedissonStreamProducer spy = spy(producer);
        doReturn(FAKE_SERVER_NOW).when(spy).scheduleClockMillis();
        Message<String> message =
                MessageBuilder.<String>withTopic(TOPIC)
                        .body("delayed")
                        .delayTimeMillis(DELAY_MILLIS)
                        .build();

        spy.syncSend(message, 3000L);

        ArgumentCaptor<Double> scoreCaptor = ArgumentCaptor.forClass(Double.class);
        verify(zsetAsync).addAsync(scoreCaptor.capture(), anyString());
        assertThat(scoreCaptor.getValue())
                .as("deliverAt 必须由 Redis 服务器时钟推导，否则跨主机时钟偏差会平移延时时长")
                .isEqualTo((double) (FAKE_SERVER_NOW + DELAY_MILLIS));
        verify(batch).execute();
    }

    @Test
    @DisplayName("R2-4：服务器时钟不可用时回退本机时钟（发送不失败）")
    void scheduleClockMillis_fallsBackToLocalClock() {
        // RScript mock 默认返回 null → RedisServerClock 判定 UNKNOWN
        long before = System.currentTimeMillis();

        long result = producer.scheduleClockMillis();

        assertThat(result).isBetween(before, System.currentTimeMillis());
    }

    @Test
    @DisplayName("R2-4：服务器时钟可读时优先取服务器值")
    void scheduleClockMillis_prefersServerClock() {
        doReturn(FAKE_SERVER_NOW)
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        eq(RScript.ReturnType.INTEGER),
                        anyList());

        assertThat(producer.scheduleClockMillis()).isEqualTo(FAKE_SERVER_NOW);
    }

    @Test
    @DisplayName("R2-4：延时 ZSet key 命名空间不受时钟改造影响（回归）")
    void sendDelayMessage_usesCustomDelayZSet() {
        RedissonStreamProducer spy = spy(producer);
        doReturn(FAKE_SERVER_NOW).when(spy).scheduleClockMillis();
        Message<String> message =
                MessageBuilder.<String>withTopic(TOPIC)
                        .body("delayed")
                        .delayTimeMillis(DELAY_MILLIS)
                        .build();

        spy.syncSend(message, 3000L);

        verify(batch)
                .getScoredSortedSet(
                        eq(StreamMQKeys.delayCustomZSet(NAMESPACE)), eq(StringCodec.INSTANCE));
        // payload Hash 写入：putAll + expire 两次取同一 key（同生同死于原子批）
        verify(batch, atLeastOnce()).getMap(anyString(), eq(StringCodec.INSTANCE));
    }
}
