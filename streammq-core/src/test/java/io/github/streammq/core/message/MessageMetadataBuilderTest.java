/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link MessageMetadataBuilder} 单元测试，覆盖属性写入的空值防御与快照不可变性。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("MessageMetadataBuilder 元数据构造器测试")
class MessageMetadataBuilderTest {

    @Test
    @DisplayName("property value 为 null 时立即抛 NPE（F-08 回归：不延迟到 Map.copyOf）")
    void propertyNullValueFailsFast() {
        MessageMetadataBuilder metadata = MessageMetadataBuilder.create();
        assertThatThrownBy(() -> metadata.property("traceId", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("property value");
    }

    @Test
    @DisplayName("property key 为 null 时立即抛 NPE")
    void propertyNullKeyFailsFast() {
        MessageMetadataBuilder metadata = MessageMetadataBuilder.create();
        assertThatThrownBy(() -> metadata.property(null, "v"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("property key");
    }

    @Test
    @DisplayName("userProperty key/value 为 null 时立即抛 NPE")
    void userPropertyNullFailsFast() {
        MessageMetadataBuilder metadata = MessageMetadataBuilder.create();
        assertThatThrownBy(() -> metadata.userProperty("k", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("userProperty value");
        assertThatThrownBy(() -> metadata.userProperty(null, "v"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("userProperty key");
    }

    @Test
    @DisplayName("合法 property 写入后 getProperties 返回不可修改快照")
    void propertySnapshotUnmodifiable() {
        MessageMetadataBuilder metadata =
                MessageMetadataBuilder.create().property("traceId", "t-1");
        assertThat(metadata.getProperties()).containsEntry("traceId", "t-1");
        assertThatThrownBy(() -> metadata.getProperties().put("k2", "v2"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("元数据应用到 Builder 后可构造消息（applyTo 冒烟）")
    void applyToSmoke() {
        MessageMetadataBuilder metadata = MessageMetadataBuilder.create().tag("t1").keys("k1");
        Message<String> message = MessageBuilder.<String>withTopic("topic-a").body("b").build();
        metadata.applyTo(MessageBuilder.from(message));
        assertThat(metadata.getTag()).isEqualTo("t1");
        assertThat(metadata.getKeys()).isEqualTo("k1");
    }
}
