/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link Message} 属性插入顺序保持的回归守卫。
 *
 * <p>锁定历史缺陷：{@code MessageBuilder} 与 {@code MessageMetadataBuilder} 都承诺"保留插入顺序"， 但 {@code Message}
 * 构造器对<b>系统属性</b>用 {@code HashMap} 拷贝，顺序在构造时被静默丢弃 （只有用户属性保留了顺序）。{@code addProperty} 也无理由地改用 {@code
 * HashMap}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("Message 属性插入顺序")
class MessagePropertyOrderTest {

    private static List<String> keysOf(Map<String, String> map) {
        return new ArrayList<>(map.keySet());
    }

    @Test
    @DisplayName("构造器保持系统属性插入顺序")
    void constructorPreservesSystemPropertyOrder() {
        Map<String, String> properties = new LinkedHashMap<>();
        for (int i = 0; i < 12; i++) {
            properties.put("k" + i, "v" + i);
        }

        Message<String> message =
                MessageBuilder.<String>withTopic("t").body("b").properties(properties).build();

        assertThat(keysOf(message.getProperties())).containsExactlyElementsOf(keysOf(properties));
    }

    @Test
    @DisplayName("构造器保持用户属性插入顺序")
    void constructorPreservesUserPropertyOrder() {
        Map<String, String> userProperties = new LinkedHashMap<>();
        userProperties.put("traceId", "t-1");
        userProperties.put("shopId", "s-9");
        userProperties.put("region", "cn");

        Message<String> message =
                MessageBuilder.<String>withTopic("t")
                        .body("b")
                        .userProperties(userProperties)
                        .build();

        assertThat(keysOf(message.getUserProperties()))
                .containsExactlyElementsOf(keysOf(userProperties));
    }

    @Test
    @DisplayName("addProperty 追加到末尾且不破坏既有顺序")
    void addPropertyAppendsAndKeepsOrder() {
        Message<String> message =
                MessageBuilder.<String>withTopic("t").body("b").withProperty("a", "1").build();

        Message<String> extended = message.addProperty("zzz", "2");

        assertThat(keysOf(extended.getProperties())).containsExactly("a", "zzz");
    }

    @Test
    @DisplayName("withProperties / withUserProperties 同样保持顺序")
    void withersPreserveOrder() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("first", "1");
        properties.put("second", "2");
        properties.put("third", "3");

        Message<String> message = MessageBuilder.<String>withTopic("t").body("b").build();

        assertThat(keysOf(message.withProperties(properties).getProperties()))
                .containsExactly("first", "second", "third");
        assertThat(keysOf(message.withUserProperties(properties).getUserProperties()))
                .containsExactly("first", "second", "third");
    }
}
