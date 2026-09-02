/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.adapter.redisson.serializer.FurySerializer;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.core.serializer.MessageSerializer;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link StreamMQCoreAutoConfiguration#streamMQMessageSerializer} 装配行为单元测试（无需 Spring 上下文与 Redis）。
 *
 * <p>验证 {@code streammq.producer.fury-require-class-registration} 开关可正确传导到默认装配的 {@link
 * FurySerializer} 实例：默认 {@code false}=宽松模式（任意 POJO 开箱即用），{@code true}=强制类注册白名单模式。
 */
@DisplayName("StreamMQ 序列化器自动装配单元测试")
class StreamMQSerializerAutoConfigurationTest {

    @AfterEach
    void clearUnrestrictedConfirmation() {
        // 清除显式确认属性，避免不同用例间的状态污染
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
    }

    @Test
    @DisplayName("默认装配 FurySerializer 为宽松模式（开关默认 false）")
    void defaultFurySerializerIsUnrestricted() {
        StreamMQProperties properties = new StreamMQProperties();
        MessageSerializer<?> serializer =
                new StreamMQCoreAutoConfiguration(properties).streamMQMessageSerializer(properties);
        assertThat(serializer).isInstanceOf(FurySerializer.class);
        assertThat(((FurySerializer<?>) serializer).isRequireClassRegistration()).isFalse();
    }

    @Test
    @DisplayName("fury-require-class-registration=true 时装配为强制类注册白名单模式")
    void configuredFurySerializerRequiresRegistration() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getProducer().setFuryRequireClassRegistration(true);
        MessageSerializer<?> serializer =
                new StreamMQCoreAutoConfiguration(properties).streamMQMessageSerializer(properties);
        assertThat(serializer).isInstanceOf(FurySerializer.class);
        assertThat(((FurySerializer<?>) serializer).isRequireClassRegistration()).isTrue();
    }

    @Test
    @DisplayName("切换为 Jackson 时开关不生效（其它序列化器按无参构造实例化）")
    void nonFurySerializerIgnoresSwitch() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getProducer().setSerializer(JacksonJsonSerializer.class);
        properties.getProducer().setFuryRequireClassRegistration(true);
        MessageSerializer<?> serializer =
                new StreamMQCoreAutoConfiguration(properties).streamMQMessageSerializer(properties);
        assertThat(serializer).isInstanceOf(JacksonJsonSerializer.class);
    }
}
