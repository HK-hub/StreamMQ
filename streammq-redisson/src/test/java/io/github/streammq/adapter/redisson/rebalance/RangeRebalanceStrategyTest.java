package io.github.streammq.adapter.redisson.rebalance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RangeRebalanceStrategy} 单元测试，覆盖范围分配、不可整除、
 * 实例数大于分片数、空集合与参数校验场景。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("RangeRebalanceStrategy 范围分配策略测试")
class RangeRebalanceStrategyTest {

    private final RangeRebalanceStrategy strategy = new RangeRebalanceStrategy();

    @Test
    @DisplayName("分片可被消费者整除时连续分配（6 分片 / 3 消费者，各 2 个）")
    void rangeDivisible() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(0, 1, 2, 3, 4, 5), List.of("A", "B", "C"), "g1");

        assertThat(assignment).hasSize(6);
        // 前 2 个分片给 A，中间 2 个给 B，最后 2 个给 C（连续范围）
        assertThat(assignment.get(0)).isEqualTo("A");
        assertThat(assignment.get(1)).isEqualTo("A");
        assertThat(assignment.get(2)).isEqualTo("B");
        assertThat(assignment.get(3)).isEqualTo("B");
        assertThat(assignment.get(4)).isEqualTo("C");
        assertThat(assignment.get(5)).isEqualTo("C");
    }

    @Test
    @DisplayName("分片不可整除时前 remainder 个消费者多分一个（7 分片 / 3 消费者 → 3/2/2）")
    void rangeWithRemainder() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(0, 1, 2, 3, 4, 5, 6), List.of("A", "B", "C"), "g1");

        assertThat(assignment).hasSize(7);
        // A=3, B=2, C=2，分片连续
        assertThat(assignment.get(0)).isEqualTo("A");
        assertThat(assignment.get(1)).isEqualTo("A");
        assertThat(assignment.get(2)).isEqualTo("A");
        assertThat(assignment.get(3)).isEqualTo("B");
        assertThat(assignment.get(4)).isEqualTo("B");
        assertThat(assignment.get(5)).isEqualTo("C");
        assertThat(assignment.get(6)).isEqualTo("C");
    }

    @Test
    @DisplayName("实例数大于分片数时前 N 个消费者各分一个（2 分片 / 5 消费者）")
    void moreConsumersThanShards() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(0, 1), List.of("A", "B", "C", "D", "E"), "g1");

        assertThat(assignment).hasSize(2);
        assertThat(assignment.get(0)).isEqualTo("A");
        assertThat(assignment.get(1)).isEqualTo("B");
    }

    @Test
    @DisplayName("单分片单消费者")
    void singleShardSingleConsumer() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(0), List.of("A"), "g1");
        assertThat(assignment).containsEntry(0, "A");
    }

    @Test
    @DisplayName("空分片列表返回空 Map")
    void emptyShards() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(), List.of("A", "B"), "g1");
        assertThat(assignment).isEmpty();
    }

    @Test
    @DisplayName("空实例列表返回空 Map")
    void emptyConsumers() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(0, 1, 2), List.of(), "g1");
        assertThat(assignment).isEmpty();
    }

    @Test
    @DisplayName("shards 为 null 抛出 NullPointerException")
    void nullShards() {
        assertThatThrownBy(() -> strategy.assign(null, List.of("A"), "g1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("shards");
    }

    @Test
    @DisplayName("consumers 为 null 抛出 NullPointerException")
    void nullConsumers() {
        assertThatThrownBy(() -> strategy.assign(List.of(0), null, "g1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("consumers");
    }

    @Test
    @DisplayName("consumerGroup 为 null 抛出 NullPointerException")
    void nullConsumerGroup() {
        assertThatThrownBy(() -> strategy.assign(List.of(0), List.of("A"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("consumerGroup");
    }

    @Test
    @DisplayName("分片 ID 非连续时仍按列表顺序连续分配给消费者")
    void nonContiguousShardIds() {
        Map<Integer, String> assignment =
                strategy.assign(List.of(10, 20, 30, 40), List.of("A", "B"), "g1");

        assertThat(assignment).hasSize(4);
        assertThat(assignment.get(10)).isEqualTo("A");
        assertThat(assignment.get(20)).isEqualTo("A");
        assertThat(assignment.get(30)).isEqualTo("B");
        assertThat(assignment.get(40)).isEqualTo("B");
    }

    @Test
    @DisplayName("name 返回 range")
    void name() {
        assertThat(strategy.name()).isEqualTo("range");
    }
}
