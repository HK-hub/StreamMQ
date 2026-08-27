/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.producer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMessageProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

/**
 * {@link RedissonStreamProducerFactory} 缓存键回归测试。
 *
 * <p>历史缺陷：仅按 {@code group} 缓存——同组第二次以不同 namespace/超时/压缩阈值等配置调用 {@code createProducer}
 * 会静默复用旧配置实例。修复后缓存键为全部影响行为字段的组合（归一化后比较）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("RedissonStreamProducerFactory 组合缓存键测试")
class RedissonStreamProducerFactoryTest {

    private final RedissonClient redisson = org.mockito.Mockito.mock(RedissonClient.class);
    private final MessageConverter converter =
            new DefaultMessageConverter(
                    new io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer<>());

    private RedissonStreamProducerFactory newFactory() {
        return new RedissonStreamProducerFactory(redisson, converter);
    }

    @Test
    @DisplayName("相同配置：返回同一实例")
    void identicalConfigReused() {
        RedissonStreamProducerFactory factory = newFactory();
        ProducerConfig config =
                ProducerConfig.builder()
                        .group("g")
                        .namespace("ns")
                        .sendMessageTimeout(1000)
                        .build();

        StreamMessageProducer first = factory.createProducer(config);
        StreamMessageProducer second = factory.createProducer(config);

        assertThat(second).isSameAs(first);
        first.close();
    }

    @Test
    @DisplayName("不同 namespace/超时/长度/压缩阈值/消息上限：各自独立实例")
    void differentConfigsGetDistinctInstances() {
        RedissonStreamProducerFactory factory = newFactory();
        ProducerConfig base = ProducerConfig.builder().group("g").namespace("ns").build();

        StreamMessageProducer p0 = factory.createProducer(base);
        StreamMessageProducer p1 =
                factory.createProducer(
                        ProducerConfig.builder().group("g").namespace("other-ns").build());
        StreamMessageProducer p2 =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g")
                                .namespace("ns")
                                .sendMessageTimeout(9999)
                                .build());
        StreamMessageProducer p3 =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g")
                                .namespace("ns")
                                .streamMaxLen(100)
                                .build());
        StreamMessageProducer p4 =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g")
                                .namespace("ns")
                                .compressThreshold(64)
                                .build());
        StreamMessageProducer p5 =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g")
                                .namespace("ns")
                                .maxMessageSize(4096L)
                                .build());

        assertThat(p1).isNotSameAs(p0);
        assertThat(p2).isNotSameAs(p0);
        assertThat(p3).isNotSameAs(p0);
        assertThat(p4).isNotSameAs(p0);
        assertThat(p5).isNotSameAs(p0);
    }

    @Test
    @DisplayName("等效归一化配置命中同一实例：timeout=0 等价默认值，负 maxLen 等价 0")
    void normalizedEquivalentConfigsShareInstance() {
        RedissonStreamProducerFactory factory = newFactory();
        StreamMessageProducer withZeroTimeout =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g")
                                .namespace("ns")
                                .sendMessageTimeout(0)
                                .build());
        StreamMessageProducer withDefaultTimeout =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g")
                                .namespace("ns")
                                .sendMessageTimeout(
                                        RedissonStreamProducerFactory.DEFAULT_SEND_TIMEOUT_MILLIS)
                                .build());
        assertThat(withDefaultTimeout).isSameAs(withZeroTimeout);

        StreamMessageProducer negativeMaxLen =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g2")
                                .namespace("ns")
                                .streamMaxLen(-5)
                                .build());
        StreamMessageProducer zeroMaxLen =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g2")
                                .namespace("ns")
                                .streamMaxLen(0)
                                .build());
        assertThat(zeroMaxLen).isSameAs(negativeMaxLen);

        StreamMessageProducer negativeThreshold =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g3")
                                .namespace("ns")
                                .compressThreshold(-1)
                                .build());
        StreamMessageProducer zeroThreshold =
                factory.createProducer(
                        ProducerConfig.builder()
                                .group("g3")
                                .namespace("ns")
                                .compressThreshold(0)
                                .build());
        assertThat(zeroThreshold).isSameAs(negativeThreshold);

        withZeroTimeout.close();
        negativeMaxLen.close();
        negativeThreshold.close();
    }
}
