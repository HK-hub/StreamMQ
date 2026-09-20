/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * <p>验证序列化器装配行为：<b>默认装配 {@link JacksonJsonSerializer}（严格类型、无 gadget RCE 面、Redis 中人类可读）</b>； 显式
 * opt-in 到 {@link FurySerializer} 时，{@code streammq.producer.fury-require-class-registration}
 * 开关可正确传导——{@code false}=宽松模式（任意 POJO 开箱即用，但共享 Redis 上有反序列化 RCE 面）， {@code true}=强制类注册白名单模式。
 *
 * <p>0.1.2 起安全默认值由 Fury 回退为 Jackson：SDK 的默认反序列化器不应把 RCE 面传播给下游应用。
 */
@DisplayName("StreamMQ 序列化器自动装配单元测试")
class StreamMQSerializerAutoConfigurationTest {

    @AfterEach
    void clearUnrestrictedConfirmation() {
        // 清除显式确认属性，避免不同用例间的状态污染
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
    }

    @Test
    @DisplayName("默认装配 JacksonJsonSerializer（安全默认：严格类型、无 gadget RCE 面）")
    void defaultSerializerIsJackson() {
        StreamMQProperties properties = new StreamMQProperties();
        MessageSerializer<?> serializer =
                new StreamMQCoreAutoConfiguration(properties).streamMQMessageSerializer(properties);
        assertThat(serializer).isInstanceOf(JacksonJsonSerializer.class);
        assertThat(serializer.name()).isEqualTo("jackson-json");
    }

    @Test
    @DisplayName("显式选择 Fury 且 fury-require-class-registration=true 时装配为强制类注册白名单模式")
    void configuredFurySerializerRequiresRegistration() {
        StreamMQProperties properties = new StreamMQProperties();
        properties.getProducer().setSerializer(FurySerializer.class);
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

    @Test
    @DisplayName("宽松模式门禁未开启：报出安全门禁提示，不得误报为『缺 fory-core 依赖』（R6-S4）")
    void unrestrictedFuryWithoutGate_reportsSecurityGateInsteadOfMissingDependency() {
        // 前提：显式清除门禁属性（@AfterEach 亦会清理），构造宽松模式 FurySerializer
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
        StreamMQProperties properties = new StreamMQProperties();
        properties.getProducer().setSerializer(FurySerializer.class);
        properties.getProducer().setFuryRequireClassRegistration(false);

        assertThatThrownBy(
                        () ->
                                new StreamMQCoreAutoConfiguration(properties)
                                        .streamMQMessageSerializer(properties))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("streammq.security.allowUnrestrictedSerializer")
                .hasMessageContaining("fury-require-class-registration")
                .hasMessageNotContaining("is not on the classpath")
                .hasMessageNotContaining("Add the dependency");
    }

    @Test
    @DisplayName("宽松模式门禁已开启：FurySerializer 正常装配（门禁通过路径不受影响）")
    void unrestrictedFuryWithGate_stillInstantiates() {
        System.setProperty("streammq.security.allowUnrestrictedSerializer", "true");
        try {
            StreamMQProperties properties = new StreamMQProperties();
            properties.getProducer().setSerializer(FurySerializer.class);
            properties.getProducer().setFuryRequireClassRegistration(false);

            MessageSerializer<?> serializer =
                    new StreamMQCoreAutoConfiguration(properties)
                            .streamMQMessageSerializer(properties);

            assertThat(serializer).isInstanceOf(FurySerializer.class);
        } finally {
            System.clearProperty("streammq.security.allowUnrestrictedSerializer");
        }
    }
}
