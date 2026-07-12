package io.github.streammq.adapter.redisson.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link StreamMQKeys} 单元测试，覆盖全部 Key 生成方法、命名空间省略规则与参数校验。
 *
 * <p>验证所有 Key 遵循 {@code streammq:{ns}:{type}:{...}} 规范，namespace 为 null 或空字符串时
 * 行为等价（省略 namespace 段）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("StreamMQKeys Key 命名工具测试")
class StreamMQKeysTest {

    @Test
    @DisplayName("prefix: namespace 为 null 返回纯前缀")
    void prefixNull() {
        assertThat(StreamMQKeys.prefix(null)).isEqualTo("streammq");
    }

    @Test
    @DisplayName("prefix: namespace 为空字符串返回纯前缀")
    void prefixEmpty() {
        assertThat(StreamMQKeys.prefix("")).isEqualTo("streammq");
    }

    @Test
    @DisplayName("prefix: namespace 非空返回带命名空间前缀")
    void prefixNonEmpty() {
        assertThat(StreamMQKeys.prefix("ns")).isEqualTo("streammq:ns");
    }

    @Test
    @DisplayName("topicStream: 带命名空间")
    void topicStreamWithNamespace() {
        assertThat(StreamMQKeys.topicStream("ns", "topic")).isEqualTo("streammq:ns:msg:topic");
    }

    @Test
    @DisplayName("topicStream: 空命名空间省略 ns 段")
    void topicStreamEmptyNamespace() {
        assertThat(StreamMQKeys.topicStream("", "topic")).isEqualTo("streammq:msg:topic");
    }

    @Test
    @DisplayName("shardStream: 拼接分片后缀")
    void shardStream() {
        assertThat(StreamMQKeys.shardStream("ns", "topic", 0))
            .isEqualTo("streammq:ns:msg:topic:shard0");
    }

    @Test
    @DisplayName("consumerGroupInstances: 消费组实例列表 Key")
    void consumerGroupInstances() {
        assertThat(StreamMQKeys.consumerGroupInstances("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:instances");
    }

    @Test
    @DisplayName("consumerGroupSemaphore: 消费组信号量 Key")
    void consumerGroupSemaphore() {
        assertThat(StreamMQKeys.consumerGroupSemaphore("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:semaphore");
    }

    @Test
    @DisplayName("consumerGroupAssignment: 消费组分片分配 Key")
    void consumerGroupAssignment() {
        assertThat(StreamMQKeys.consumerGroupAssignment("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:assignment");
    }

    @Test
    @DisplayName("consumerGroupNotify: 消费组通知频道 Key")
    void consumerGroupNotify() {
        assertThat(StreamMQKeys.consumerGroupNotify("ns", "group"))
            .isEqualTo("streammq:ns:cg:group:notify");
    }

    @Test
    @DisplayName("retryZSet: 重试队列 Key")
    void retryZSet() {
        assertThat(StreamMQKeys.retryZSet("ns", "topic", "group"))
            .isEqualTo("streammq:ns:retry:topic:group");
    }

    @Test
    @DisplayName("dlqStream: 死信队列 Key")
    void dlqStream() {
        assertThat(StreamMQKeys.dlqStream("ns", "group"))
            .isEqualTo("streammq:ns:dlq:group");
    }

    @Test
    @DisplayName("retryStream: 重试消息 Stream Key")
    void retryStream() {
        assertThat(StreamMQKeys.retryStream("ns", "topic", "group"))
            .isEqualTo("streammq:ns:retry:msg:topic:group");
    }

    @Test
    @DisplayName("retryStream: 空命名空间省略 ns 段")
    void retryStreamEmptyNamespace() {
        assertThat(StreamMQKeys.retryStream("", "topic", "group"))
            .isEqualTo("streammq:retry:msg:topic:group");
    }

    @Test
    @DisplayName("retryTransferLock: 重试转移降级锁 Key")
    void retryTransferLock() {
        assertThat(StreamMQKeys.retryTransferLock("ns", "topic", "group"))
            .isEqualTo("streammq:ns:retry:topic:group:transfer:lock");
    }

    @Test
    @DisplayName("delayZSet: 延时级别 Key")
    void delayZSet() {
        assertThat(StreamMQKeys.delayZSet("ns", "SEC_1"))
            .isEqualTo("streammq:ns:delay:SEC_1");
    }

    @Test
    @DisplayName("delayPayloadHash: 延时消息 payload Key")
    void delayPayloadHash() {
        assertThat(StreamMQKeys.delayPayloadHash("ns", "msgId"))
            .isEqualTo("streammq:ns:delay:payload:msgId");
    }

    @Test
    @DisplayName("delayDeliveredCounter: 延时已投递计数 Key")
    void delayDeliveredCounter() {
        assertThat(StreamMQKeys.delayDeliveredCounter("ns"))
            .isEqualTo("streammq:ns:delay:meta:delivered");
    }

    @Test
    @DisplayName("halfStream: 半消息暂存 Key")
    void halfStream() {
        assertThat(StreamMQKeys.halfStream("ns", "txGroup"))
            .isEqualTo("streammq:ns:half:txGroup");
    }

    @Test
    @DisplayName("transactionStateHash: 事务状态 Key")
    void transactionStateHash() {
        assertThat(StreamMQKeys.transactionStateHash("ns", "txGroup"))
            .isEqualTo("streammq:ns:txstate:txGroup");
    }

    @Test
    @DisplayName("transactionCheckZSet: 事务回查 Key")
    void transactionCheckZSet() {
        assertThat(StreamMQKeys.transactionCheckZSet("ns", "txGroup"))
            .isEqualTo("streammq:ns:txcheck:txGroup");
    }

    @Test
    @DisplayName("transactionCheckCounter: 事务回查计数 Key")
    void transactionCheckCounter() {
        assertThat(StreamMQKeys.transactionCheckCounter("ns", "txGroup"))
            .isEqualTo("streammq:ns:txcheck:txGroup:counter");
    }

    @Test
    @DisplayName("shardLock: 顺序消费分片锁 Key")
    void shardLock() {
        assertThat(StreamMQKeys.shardLock("ns", "topic", "group", 0))
            .isEqualTo("streammq:ns:shardlock:topic:group:0");
    }

    @Test
    @DisplayName("metaOffset: 消费位点 Key")
    void metaOffset() {
        assertThat(StreamMQKeys.metaOffset("ns", "group", "topic"))
            .isEqualTo("streammq:ns:meta:offset:group:topic");
    }

    @Test
    @DisplayName("metaCounter: 消费计数 Key")
    void metaCounter() {
        assertThat(StreamMQKeys.metaCounter("ns", "group", "topic"))
            .isEqualTo("streammq:ns:meta:counter:group:topic");
    }

    @Test
    @DisplayName("metaStats: 运行时统计 Key")
    void metaStats() {
        assertThat(StreamMQKeys.metaStats("ns", "group", "topic"))
            .isEqualTo("streammq:ns:meta:stats:group:topic");
    }

    @Test
    @DisplayName("metaConfig: 消费组配置 Hash Key")
    void metaConfig() {
        assertThat(StreamMQKeys.metaConfig("ns", "group"))
            .isEqualTo("streammq:ns:meta:config:group");
    }

    @Test
    @DisplayName("metaConfig: 空命名空间省略 ns 段")
    void metaConfigEmptyNamespace() {
        assertThat(StreamMQKeys.metaConfig("", "group"))
            .isEqualTo("streammq:meta:config:group");
    }

    @Test
    @DisplayName("traceStream: 追踪数据 Stream Key")
    void traceStream() {
        assertThat(StreamMQKeys.traceStream("ns", "20260710"))
            .isEqualTo("streammq:ns:trace:20260710");
    }

    @Test
    @DisplayName("traceStream: 空命名空间省略 ns 段")
    void traceStreamEmptyNamespace() {
        assertThat(StreamMQKeys.traceStream("", "20260710"))
            .isEqualTo("streammq:trace:20260710");
    }

    @Nested
    @DisplayName("参数校验 requireNonEmpty")
    class RequireNonEmptyTests {

        @Test
        @DisplayName("topic 为 null 抛出 NullPointerException")
        void topicNull() {
            assertThatThrownBy(() -> StreamMQKeys.topicStream("ns", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("topic 为空字符串抛出 IllegalArgumentException")
        void topicEmpty() {
            assertThatThrownBy(() -> StreamMQKeys.topicStream("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
        }

        @Test
        @DisplayName("group 为 null 抛出 NullPointerException")
        void groupNull() {
            assertThatThrownBy(() -> StreamMQKeys.retryZSet("ns", "topic", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("group");
        }

        @Test
        @DisplayName("level 为空字符串抛出 IllegalArgumentException")
        void levelEmpty() {
            assertThatThrownBy(() -> StreamMQKeys.delayZSet("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("level");
        }

        @Test
        @DisplayName("msgId 为 null 抛出 NullPointerException")
        void msgIdNull() {
            assertThatThrownBy(() -> StreamMQKeys.delayPayloadHash("ns", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("msgId");
        }

        @Test
        @DisplayName("txGroup 为空字符串抛出 IllegalArgumentException")
        void txGroupEmpty() {
            assertThatThrownBy(() -> StreamMQKeys.halfStream("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("txGroup");
        }

        @Test
        @DisplayName("metaConfig: group 为 null 抛出 NullPointerException")
        void metaConfigGroupNull() {
            assertThatThrownBy(() -> StreamMQKeys.metaConfig("ns", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("group");
        }

        @Test
        @DisplayName("metaConfig: group 为空字符串抛出 IllegalArgumentException")
        void metaConfigGroupEmpty() {
            assertThatThrownBy(() -> StreamMQKeys.metaConfig("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("group");
        }

        @Test
        @DisplayName("traceStream: date 为 null 抛出 NullPointerException")
        void traceStreamDateNull() {
            assertThatThrownBy(() -> StreamMQKeys.traceStream("ns", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("date");
        }

        @Test
        @DisplayName("traceStream: date 为空字符串抛出 IllegalArgumentException")
        void traceStreamDateEmpty() {
            assertThatThrownBy(() -> StreamMQKeys.traceStream("ns", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("date");
        }
    }

    @Nested
    @DisplayName("namespace 为 null 与空字符串等价")
    class NamespaceEquivalenceTests {

        @Test
        @DisplayName("prefix: null 与空字符串结果一致")
        void prefixEquivalence() {
            assertThat(StreamMQKeys.prefix(null)).isEqualTo(StreamMQKeys.prefix(""));
        }

        @Test
        @DisplayName("topicStream: null 与空字符串结果一致")
        void topicStreamEquivalence() {
            assertThat(StreamMQKeys.topicStream(null, "topic"))
                .isEqualTo(StreamMQKeys.topicStream("", "topic"));
        }

        @Test
        @DisplayName("shardStream: null 与空字符串结果一致")
        void shardStreamEquivalence() {
            assertThat(StreamMQKeys.shardStream(null, "topic", 0))
                .isEqualTo(StreamMQKeys.shardStream("", "topic", 0));
        }

        @Test
        @DisplayName("consumerGroupInstances: null 与空字符串结果一致")
        void consumerGroupInstancesEquivalence() {
            assertThat(StreamMQKeys.consumerGroupInstances(null, "group"))
                .isEqualTo(StreamMQKeys.consumerGroupInstances("", "group"));
        }

        @Test
        @DisplayName("consumerGroupSemaphore: null 与空字符串结果一致")
        void consumerGroupSemaphoreEquivalence() {
            assertThat(StreamMQKeys.consumerGroupSemaphore(null, "group"))
                .isEqualTo(StreamMQKeys.consumerGroupSemaphore("", "group"));
        }

        @Test
        @DisplayName("consumerGroupAssignment: null 与空字符串结果一致")
        void consumerGroupAssignmentEquivalence() {
            assertThat(StreamMQKeys.consumerGroupAssignment(null, "group"))
                .isEqualTo(StreamMQKeys.consumerGroupAssignment("", "group"));
        }

        @Test
        @DisplayName("consumerGroupNotify: null 与空字符串结果一致")
        void consumerGroupNotifyEquivalence() {
            assertThat(StreamMQKeys.consumerGroupNotify(null, "group"))
                .isEqualTo(StreamMQKeys.consumerGroupNotify("", "group"));
        }

        @Test
        @DisplayName("retryZSet: null 与空字符串结果一致")
        void retryZSetEquivalence() {
            assertThat(StreamMQKeys.retryZSet(null, "topic", "group"))
                .isEqualTo(StreamMQKeys.retryZSet("", "topic", "group"));
        }

        @Test
        @DisplayName("dlqStream: null 与空字符串结果一致")
        void dlqStreamEquivalence() {
            assertThat(StreamMQKeys.dlqStream(null, "group"))
                .isEqualTo(StreamMQKeys.dlqStream("", "group"));
        }

        @Test
        @DisplayName("retryTransferLock: null 与空字符串结果一致")
        void retryTransferLockEquivalence() {
            assertThat(StreamMQKeys.retryTransferLock(null, "topic", "group"))
                .isEqualTo(StreamMQKeys.retryTransferLock("", "topic", "group"));
        }

        @Test
        @DisplayName("delayZSet: null 与空字符串结果一致")
        void delayZSetEquivalence() {
            assertThat(StreamMQKeys.delayZSet(null, "SEC_1"))
                .isEqualTo(StreamMQKeys.delayZSet("", "SEC_1"));
        }

        @Test
        @DisplayName("delayPayloadHash: null 与空字符串结果一致")
        void delayPayloadHashEquivalence() {
            assertThat(StreamMQKeys.delayPayloadHash(null, "msgId"))
                .isEqualTo(StreamMQKeys.delayPayloadHash("", "msgId"));
        }

        @Test
        @DisplayName("delayDeliveredCounter: null 与空字符串结果一致")
        void delayDeliveredCounterEquivalence() {
            assertThat(StreamMQKeys.delayDeliveredCounter(null))
                .isEqualTo(StreamMQKeys.delayDeliveredCounter(""));
        }

        @Test
        @DisplayName("halfStream: null 与空字符串结果一致")
        void halfStreamEquivalence() {
            assertThat(StreamMQKeys.halfStream(null, "txGroup"))
                .isEqualTo(StreamMQKeys.halfStream("", "txGroup"));
        }

        @Test
        @DisplayName("transactionStateHash: null 与空字符串结果一致")
        void transactionStateHashEquivalence() {
            assertThat(StreamMQKeys.transactionStateHash(null, "txGroup"))
                .isEqualTo(StreamMQKeys.transactionStateHash("", "txGroup"));
        }

        @Test
        @DisplayName("transactionCheckZSet: null 与空字符串结果一致")
        void transactionCheckZSetEquivalence() {
            assertThat(StreamMQKeys.transactionCheckZSet(null, "txGroup"))
                .isEqualTo(StreamMQKeys.transactionCheckZSet("", "txGroup"));
        }

        @Test
        @DisplayName("transactionCheckCounter: null 与空字符串结果一致")
        void transactionCheckCounterEquivalence() {
            assertThat(StreamMQKeys.transactionCheckCounter(null, "txGroup"))
                .isEqualTo(StreamMQKeys.transactionCheckCounter("", "txGroup"));
        }

        @Test
        @DisplayName("shardLock: null 与空字符串结果一致")
        void shardLockEquivalence() {
            assertThat(StreamMQKeys.shardLock(null, "topic", "group", 0))
                .isEqualTo(StreamMQKeys.shardLock("", "topic", "group", 0));
        }

        @Test
        @DisplayName("metaOffset: null 与空字符串结果一致")
        void metaOffsetEquivalence() {
            assertThat(StreamMQKeys.metaOffset(null, "group", "topic"))
                .isEqualTo(StreamMQKeys.metaOffset("", "group", "topic"));
        }

        @Test
        @DisplayName("metaCounter: null 与空字符串结果一致")
        void metaCounterEquivalence() {
            assertThat(StreamMQKeys.metaCounter(null, "group", "topic"))
                .isEqualTo(StreamMQKeys.metaCounter("", "group", "topic"));
        }

        @Test
        @DisplayName("metaStats: null 与空字符串结果一致")
        void metaStatsEquivalence() {
            assertThat(StreamMQKeys.metaStats(null, "group", "topic"))
                .isEqualTo(StreamMQKeys.metaStats("", "group", "topic"));
        }

        @Test
        @DisplayName("metaConfig: null 与空字符串结果一致")
        void metaConfigEquivalence() {
            assertThat(StreamMQKeys.metaConfig(null, "group"))
                .isEqualTo(StreamMQKeys.metaConfig("", "group"));
        }

        @Test
        @DisplayName("traceStream: null 与空字符串结果一致")
        void traceStreamEquivalence() {
            assertThat(StreamMQKeys.traceStream(null, "20260710"))
                .isEqualTo(StreamMQKeys.traceStream("", "20260710"));
        }
    }
}
