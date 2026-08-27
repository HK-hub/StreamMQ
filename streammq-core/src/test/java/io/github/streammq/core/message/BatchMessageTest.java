/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** {@link BatchMessage} 单元测试，覆盖 Builder add/addAll、空列表校验、Topic 一致性、不可修改视图与 size/isEmpty。 */
@DisplayName("BatchMessage 批量消息测试")
class BatchMessageTest {

    private static Message<String> newMessage(String topic, String body) {
        return MessageBuilder.<String>withTopic(topic).body(body).build();
    }

    @Nested
    @DisplayName("Builder add")
    class BuilderAdd {

        @Test
        @DisplayName("add 单条消息后 build 返回包含该消息的批量消息")
        void addSingle() {
            Message<String> msg = newMessage("topic", "body");
            BatchMessage<String> batch = BatchMessage.<String>withTopic("topic").add(msg).build();
            assertThat(batch.getMessages()).containsExactly(msg);
            assertThat(batch.getTopic()).isEqualTo("topic");
        }

        @Test
        @DisplayName("add 多条消息")
        void addMultiple() {
            Message<String> m1 = newMessage("topic", "1");
            Message<String> m2 = newMessage("topic", "2");
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("topic").add(m1).add(m2).build();
            assertThat(batch.getMessages()).containsExactly(m1, m2);
        }

        @Test
        @DisplayName("add null 消息抛 NPE")
        void addNull() {
            BatchMessage.Builder<String> builder = BatchMessage.withTopic("topic");
            assertThatThrownBy(() -> builder.add(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("message");
        }

        @Test
        @DisplayName("add 时 topic 不一致抛 IllegalArgumentException")
        void addTopicMismatch() {
            Message<String> msg = newMessage("other-topic", "body");
            assertThatThrownBy(() -> BatchMessage.<String>withTopic("topic").add(msg))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not match batch topic");
        }
    }

    @Nested
    @DisplayName("Builder addAll")
    class BuilderAddAll {

        @Test
        @DisplayName("addAll 批量添加消息")
        void addAllBatch() {
            Message<String> m1 = newMessage("topic", "1");
            Message<String> m2 = newMessage("topic", "2");
            Message<String> m3 = newMessage("topic", "3");
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("topic")
                            .addAll(Arrays.asList(m1, m2, m3))
                            .build();
            assertThat(batch.getMessages()).containsExactly(m1, m2, m3);
        }

        @Test
        @DisplayName("addAll 传入 null 抛 IllegalArgumentException（与 add null 校验对称）")
        void addAllNull() {
            BatchMessage.Builder<String> builder = BatchMessage.withTopic("topic");
            assertThatThrownBy(() -> builder.addAll(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("messages list must not be null");
        }

        @Test
        @DisplayName("addAll 中存在 topic 不一致抛 IllegalArgumentException")
        void addAllTopicMismatch() {
            Message<String> m1 = newMessage("topic", "1");
            Message<String> m2 = newMessage("wrong", "2");
            List<Message<String>> list = Arrays.asList(m1, m2);
            assertThatThrownBy(() -> BatchMessage.<String>withTopic("topic").addAll(list))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Builder.build 空列表校验")
    class BuildEmpty {

        @Test
        @DisplayName("build 空列表抛 IllegalStateException")
        void buildEmptyThrows() {
            assertThatThrownBy(() -> BatchMessage.<String>withTopic("topic").build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("batch messages is empty");
        }
    }

    @Nested
    @DisplayName("Builder Topic 校验")
    class BuilderTopicValidation {

        @Test
        @DisplayName("withTopic 传 null 抛 NPE")
        void nullTopic() {
            assertThatThrownBy(() -> BatchMessage.withTopic(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("withTopic 传空字符串抛 IllegalArgumentException")
        void emptyTopic() {
            assertThatThrownBy(() -> BatchMessage.withTopic(""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("topic must not be empty");
        }

        @Test
        @DisplayName("withTopic 自动 trim")
        void topicTrims() {
            Message<String> msg = newMessage("topic", "b");
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("  topic  ").add(msg).build();
            assertThat(batch.getTopic()).isEqualTo("topic");
        }
    }

    @Nested
    @DisplayName("getMessages 不可修改视图")
    class Unmodifiable {

        @Test
        @DisplayName("getMessages 返回不可修改列表")
        void getMessagesUnmodifiable() {
            Message<String> msg = newMessage("topic", "b");
            BatchMessage<String> batch = BatchMessage.<String>withTopic("topic").add(msg).build();
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> batch.getMessages().add(msg));
        }
    }

    @Nested
    @DisplayName("size / isEmpty")
    class SizeAndEmpty {

        @Test
        @DisplayName("size 返回消息数量")
        void size() {
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("topic")
                            .add(newMessage("topic", "1"))
                            .add(newMessage("topic", "2"))
                            .build();
            assertThat(batch.size()).isEqualTo(2);
        }

        @Test
        @DisplayName("isEmpty 返回 false（build 后必然非空）")
        void isEmpty() {
            BatchMessage<String> batch =
                    BatchMessage.<String>withTopic("topic").add(newMessage("topic", "1")).build();
            assertThat(batch.isEmpty()).isFalse();
        }
    }
}
