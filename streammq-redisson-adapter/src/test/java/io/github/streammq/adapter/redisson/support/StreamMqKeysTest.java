package io.github.streammq.adapter.redisson.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link StreamMqKeys} 单元测试，覆盖全部 Key 生成方法、命名空间省略规则与参数校验。
 *
 * <p>验证所有 Key 遵循 {@code streammq:{ns}:{type}:{...}} 规范，namespace 为 null 或空字符串时
 * 行为等价（省略 namespace 段）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("StreamMqKeys Key 命名工具测试")
class StreamMqKeysTest {

    @Test
    @DisplayName("prefix: namespace 为 null 返回纯前缀")
    void prefixNull() {
        assertThat(StreamMqKeys.prefix(null)).isEqualTo("streammq");
    }

    @Test
    @DisplayName("prefix: namespace 为空字符串返回纯前缀")
    void prefixEmpty() {
        assertThat(StreamMqKeys.prefix("")).isEqualTo("streammq");
    }

    @Test
    @DisplayName("prefix: namespace 非空返回带命名空间前缀")
    void prefixNonEmpty() {
        assertThat(StreamMqKeys.prefix("ns")).isEqualTo("streammq:ns");
    }

    @Test
    @DisplayName("topicStream: 带命名空间")
    void topicStreamWithNamespace() {
        assertThat(StreamMqKeys.topicStream("ns", "topic")).isEqualTo("streammq:ns:msg:topic");
    }

    @Test
    @DisplayName("topicStream: 空命名空间省略 ns 段")
    void topicStreamEmptyNamespace() {
        assertThat(StreamMqKeys.topicStream("", "topic")).isEqualTo("streammq:msg:topic");
    }

    @Test
    @DisplayName("shardStream: 拼接分片后缀")
    void shardStream() {
        assertThat(StreamMqKeys.shardStream("ns", "topic", 0))
            .isEqualTo("streammq:ns:msg:topic:shard0");
    }

    @Test
    @DisplayName("consumerGroupInstances: 消费组实例列表 Key")
    void consumerGroupInstances() {
        assertThat(StreamMqKeys.consumerGroupInstances("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:instances");
    }

    @Test
    @DisplayName("consumerGroupSemaphore: 消费组信号量 Key")
    void consumerGroupSemaphore() {
        assertThat(StreamMqKeys.consumerGroupSemaphore("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:semaphore");
    }

    @Test
    @DisplayName("consumerGroupAssignment: 消费组分片分配 Key")
    void consumerGroupAssignment() {
        assertThat(StreamMqKeys.consumerGroupAssignment("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:assignment");
    }

    @Test
    @DisplayName("consumerGroupNotify: 消费组通知频道 Key")
    void consumerGroupNotify() {
        assertThat(StreamMqKeys.consumerGroupNotify("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:notify");
    }

    @Test
    @DisplayName("retryZSet: 重试队列 Key")
    void retryZSet() {
        assertThat(StreamMqKeys.retryZSet("ns", "topic", "group"))
            .isEqualTo("streammq:ns:retry:topic:group");
    }

    @Test
    @DisplayName("dlqStream: 死信队列 Key")
    void dlqStream() {
        assertThat(StreamMqKeys.dlqStream("ns", "topic", "group"))
            .isEqualTo("streammq:ns:dlq:topic:group");
    }

    @Test
    @DisplayName("retryTransferLock: 重试转移降级锁 Key")
    void retryTransferLock() {
        assertThat(StreamMqKeys.retryTransferLock("ns", "topic", "group"))
            .isEqualTo("streammq:ns:retry:topic:group:transfer:lock");
    }

    @Test
    @DisplayName("delayZSet: 延时级别 Key")
    void delayZSet() {
        assertThat(StreamMqKeys.delayZSet("ns", "SEC_1"))
            .isEqualTo("streammq:ns:delay:SEC_1");
    }

    @Test
    @DisplayName("delayPayloadHash: 延时消息 payload Key")
    void delayPayloadHash() {
        assertThat(StreamMqKeys.delayPayloadHash("ns", "msgId"))
            .isEqualTo("streammq:ns:delay:payload:msgId");
    }

    @Test
    @DisplayName("delayDeliveredCounter: 延时已投递计数 Key")
    void delayDeliveredCounter() {
        assertThat(StreamMqKeys.delayDeliveredCounter("ns"))
            .isEqualTo("streammq:ns:delay:meta:delivered");
    }

    @Test
    @DisplayName("halfStream: 半消息暂存 Key")
    void halfStream() {
        assertThat(StreamMqKeys.halfStream("ns", "txGroup"))
            .isEqualTo("streammq:ns:half:txGroup");
    }

    @Test
    @DisplayName("transactionStateHash: 事务状态 Key")
    void transactionStateHash() {
        assertThat(StreamMqKeys.transactionStateHash("ns", "txGroup"))
            .isEqualTo("streammq:ns:txstate:txGroup");
    }

    @Test
    @DisplayName("transactionCheckZSet: 事务回查 Key")
    void transactionCheckZSet() {
        assertThat(StreamMqKeys.transactionCheckZSet("ns", "txGroup"))
            .isEqualTo("streammq:ns:txcheck:txGroup");
    }

    @Test
    @DisplayName("transactionCheckCounter: 事务回查计数 Key")
    void transactionCheckCounter() {
        assertThat(StreamMqKeys.transactionCheckCounter("ns", "txGroup"))
            .isEqualTo("streammq:ns:txcheck:txGroup:counter");
    }

    @Test
    @DisplayName("shardLock: 顺序消费分片锁 Key")
    void shardLock() {
        assertThat(StreamMqKeys.shardLock("ns", "topic", "group", 0))
            .isEqualTo("streammq:ns:shardlock:topic:group:0");
    }

    @Test
    @DisplayName("metaOffset: 消费位点 Key")
    void metaOffset() {
        assertThat(StreamMqKeys.metaOffset("ns", "group", "topic"))
            .isEqualTo("streammq:ns:meta:offset:group:topic");
    }

    @Test
    @DisplayName("metaCounter: 消费计数 Key")
    void metaCounter() {
        assertThat(StreamMqKeys.metaCounter("ns", "group", "topic"))
            .isEqualTo("streammq:ns:meta:counter:group:topic");
    }

    @Test
    @DisplayName("metaStats: 运行时统计 Key")
    void metaStats() {
        assertThat(StreamMqKeys.metaStats("ns", "group", "topic"))
            .isEqualTo("streammq:ns:meta:stats:group:topic");
    }

    @Nested
    @DisplayName("参数校验 requireNonEmpty")
    class RequireNonEmptyTests {

        @Test
        @DisplayName("topic 为 null 抛出 NullPointerException")
        void topicNull() {
            assertThatThrownBy(() -> StreamMqKeys.topicStream("ns", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("topic 为空字符串抛出 IllegalArgumentException")
        void topicEmpty() {
            assertThatThrownBy(() -> StreamMqKeys.topicStream("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("group 为 null 抛出 NullPointerException")
        void groupNull() {
            assertThatThrownBy(() -> StreamMqKeys.retryZSet("ns", "topic", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("group");
        }

        @Test
        @DisplayName("level 为空字符串抛出 IllegalArgumentException")
        void levelEmpty() {
            assertThatThrownBy(() -> StreamMqKeys.delayZSet("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("level");
        }

        @Test
        @DisplayName("msgId 为 null 抛出 NullPointerException")
        void msgIdNull() {
            assertThatThrownBy(() -> StreamMqKeys.delayPayloadHash("ns", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("msgId");
        }

        @Test
        @DisplayName("txGroup 为空字符串抛出 IllegalArgumentException")
        void txGroupEmpty() {
            assertThatThrownBy(() -> StreamMqKeys.halfStream("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("txGroup");
        }
    }

    @Nested
    @DisplayName("namespace 为 null 与空字符串等价")
    class NamespaceEquivalenceTests {

        @Test
        @DisplayName("prefix: null 与空字符串结果一致")
        void prefixEquivalence() {
            assertThat(StreamMqKeys.prefix(null)).isEqualTo(StreamMqKeys.prefix(""));
        }

        @Test
        @DisplayName("topicStream: null 与空字符串结果一致")
        void topicStreamEquivalence() {
            assertThat(StreamMqKeys.topicStream(null, "topic"))
                .isEqualTo(StreamMqKeys.topicStream("", "topic"));
        }

        @Test
        @DisplayName("shardStream: null 与空字符串结果一致")
        void shardStreamEquivalence() {
            assertThat(StreamMqKeys.shardStream(null, "topic", 0))
                .isEqualTo(StreamMqKeys.shardStream("", "topic", 0));
        }

        @Test
        @DisplayName("consumerGroupInstances: null 与空字符串结果一致")
        void consumerGroupInstancesEquivalence() {
            assertThat(StreamMqKeys.consumerGroupInstances(null, "group"))
                .isEqualTo(StreamMqKeys.consumerGroupInstances("", "group"));
        }

        @Test
        @DisplayName("consumerGroupSemaphore: null 与空字符串结果一致")
        void consumerGroupSemaphoreEquivalence() {
            assertThat(StreamMqKeys.consumerGroupSemaphore(null, "group"))
                .isEqualTo(StreamMqKeys.consumerGroupSemaphore("", "group"));
        }

        @Test
        @DisplayName("consumerGroupAssignment: null 与空字符串结果一致")
        void consumerGroupAssignmentEquivalence() {
            assertThat(StreamMqKeys.consumerGroupAssignment(null, "group"))
                .isEqualTo(StreamMqKeys.consumerGroupAssignment("", "group"));
        }

        @Test
        @DisplayName("consumerGroupNotify: null 与空字符串结果一致")
        void consumerGroupNotifyEquivalence() {
            assertThat(StreamMqKeys.consumerGroupNotify(null, "group"))
                .isEqualTo(StreamMqKeys.consumerGroupNotify("", "group"));
        }

        @Test
        @DisplayName("retryZSet: null 与空字符串结果一致")
        void retryZSetEquivalence() {
            assertThat(StreamMqKeys.retryZSet(null, "topic", "group"))
                .isEqualTo(StreamMqKeys.retryZSet("", "topic", "group"));
        }

        @Test
        @DisplayName("dlqStream: null 与空字符串结果一致")
        void dlqStreamEquivalence() {
            assertThat(StreamMqKeys.dlqStream(null, "topic", "group"))
                .isEqualTo(StreamMqKeys.dlqStream("", "topic", "group"));
        }

        @Test
        @DisplayName("retryTransferLock: null 与空字符串结果一致")
        void retryTransferLockEquivalence() {
            assertThat(StreamMqKeys.retryTransferLock(null, "topic", "group"))
                .isEqualTo(StreamMqKeys.retryTransferLock("", "topic", "group"));
        }

        @Test
        @DisplayName("delayZSet: null 与空字符串结果一致")
        void delayZSetEquivalence() {
            assertThat(StreamMqKeys.delayZSet(null, "SEC_1"))
                .isEqualTo(StreamMqKeys.delayZSet("", "SEC_1"));
        }

        @Test
        @DisplayName("delayPayloadHash: null 与空字符串结果一致")
        void delayPayloadHashEquivalence() {
            assertThat(StreamMqKeys.delayPayloadHash(null, "msgId"))
                .isEqualTo(StreamMqKeys.delayPayloadHash("", "msgId"));
        }

        @Test
        @DisplayName("delayDeliveredCounter: null 与空字符串结果一致")
        void delayDeliveredCounterEquivalence() {
            assertThat(StreamMqKeys.delayDeliveredCounter(null))
                .isEqualTo(StreamMqKeys.delayDeliveredCounter(""));
        }

        @Test
        @DisplayName("halfStream: null 与空字符串结果一致")
        void halfStreamEquivalence() {
            assertThat(StreamMqKeys.halfStream(null, "txGroup"))
                .isEqualTo(StreamMqKeys.halfStream("", "txGroup"));
        }

        @Test
        @DisplayName("transactionStateHash: null 与空字符串结果一致")
        void transactionStateHashEquivalence() {
            assertThat(StreamMqKeys.transactionStateHash(null, "txGroup"))
                .isEqualTo(StreamMqKeys.transactionStateHash("", "txGroup"));
        }

        @Test
        @DisplayName("transactionCheckZSet: null 与空字符串结果一致")
        void transactionCheckZSetEquivalence() {
            assertThat(StreamMqKeys.transactionCheckZSet(null, "txGroup"))
                .isEqualTo(StreamMqKeys.transactionCheckZSet("", "txGroup"));
        }

        @Test
        @DisplayName("transactionCheckCounter: null 与空字符串结果一致")
        void transactionCheckCounterEquivalence() {
            assertThat(StreamMqKeys.transactionCheckCounter(null, "txGroup"))
                .isEqualTo(StreamMqKeys.transactionCheckCounter("", "txGroup"));
        }

        @Test
        @DisplayName("shardLock: null 与空字符串结果一致")
        void shardLockEquivalence() {
            assertThat(StreamMqKeys.shardLock(null, "topic", "group", 0))
                .isEqualTo(StreamMqKeys.shardLock("", "topic", "group", 0));
        }

        @Test
        @DisplayName("metaOffset: null 与空字符串结果一致")
        void metaOffsetEquivalence() {
            assertThat(StreamMqKeys.metaOffset(null, "group", "topic"))
                .isEqualTo(StreamMqKeys.metaOffset("", "group", "topic"));
        }

        @Test
        @DisplayName("metaCounter: null 与空字符串结果一致")
        void metaCounterEquivalence() {
            assertThat(StreamMqKeys.metaCounter(null, "group", "topic"))
                .isEqualTo(StreamMqKeys.metaCounter("", "group", "topic"));
        }

        @Test
        @DisplayName("metaStats: null 与空字符串结果一致")
        void metaStatsEquivalence() {
            assertThat(StreamMqKeys.metaStats(null, "group", "topic"))
                .isEqualTo(StreamMqKeys.metaStats("", "group", "topic"));
        }
    }
}
