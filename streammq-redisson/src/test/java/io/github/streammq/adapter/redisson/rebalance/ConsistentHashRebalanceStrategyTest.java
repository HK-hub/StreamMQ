package io.github.streammq.adapter.redisson.rebalance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link ConsistentHashRebalanceStrategy} 单元测试，覆盖一致性哈希分配、 结果稳定性与实例增减时分片迁移最小化。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("ConsistentHashRebalanceStrategy 一致性哈希策略测试")
class ConsistentHashRebalanceStrategyTest {

  private final ConsistentHashRebalanceStrategy strategy = new ConsistentHashRebalanceStrategy();

  @Test
  @DisplayName("所有分片都被分配到某个消费者（无 null 归属）")
  void allShardsAssigned() {
    List<Integer> shards = IntStream.range(0, 8).boxed().toList();
    List<String> consumers = List.of("A", "B", "C");

    Map<Integer, String> assignment = strategy.assign(shards, consumers, "g1");

    assertThat(assignment).hasSize(8);
    assignment.values().forEach(owner -> assertThat(owner).isNotNull());
  }

  @Test
  @DisplayName("相同实例集合多次调用结果一致")
  void stableAssignment() {
    List<Integer> shards = IntStream.range(0, 16).boxed().toList();
    List<String> consumers = List.of("A", "B", "C");

    Map<Integer, String> first = strategy.assign(shards, consumers, "g1");
    Map<Integer, String> second = strategy.assign(shards, consumers, "g1");

    assertThat(second).isEqualTo(first);
  }

  @Test
  @DisplayName("增加一个消费者时分片迁移量最小化（多数分片归属不变）")
  void minimalMigrationOnAdd() {
    List<Integer> shards = IntStream.range(0, 64).boxed().toList();
    List<String> before = List.of("A", "B", "C");
    List<String> after = List.of("A", "B", "C", "D");

    Map<Integer, String> beforeAssignment = strategy.assign(shards, before, "g1");
    Map<Integer, String> afterAssignment = strategy.assign(shards, after, "g1");

    long unchanged =
        shards.stream().filter(s -> beforeAssignment.get(s).equals(afterAssignment.get(s))).count();

    assertThat(unchanged).isGreaterThan(shards.size() / 2L);
  }

  @Test
  @DisplayName("减少一个消费者时分片迁移量最小化（多数分片归属不变）")
  void minimalMigrationOnRemove() {
    List<Integer> shards = IntStream.range(0, 64).boxed().toList();
    List<String> before = List.of("A", "B", "C", "D");
    List<String> after = List.of("A", "B", "C");

    Map<Integer, String> beforeAssignment = strategy.assign(shards, before, "g1");
    Map<Integer, String> afterAssignment = strategy.assign(shards, after, "g1");

    long unchanged =
        shards.stream().filter(s -> beforeAssignment.get(s).equals(afterAssignment.get(s))).count();

    assertThat(unchanged).isGreaterThan(shards.size() / 2L);
  }

  @Test
  @DisplayName("新增消费者会接管部分分片")
  void newConsumerTakesShards() {
    List<Integer> shards = IntStream.range(0, 64).boxed().toList();
    Map<Integer, String> before = strategy.assign(shards, List.of("A", "B", "C"), "g1");
    Map<Integer, String> after = strategy.assign(shards, List.of("A", "B", "C", "D"), "g1");

    long takenByD =
        shards.stream().filter(s -> "D".equals(after.get(s)) && !"D".equals(before.get(s))).count();

    assertThat(takenByD).isPositive();
  }

  @Test
  @DisplayName("空分片列表返回空 Map")
  void emptyShards() {
    Map<Integer, String> assignment = strategy.assign(List.of(), List.of("A", "B"), "g1");
    assertThat(assignment).isEmpty();
  }

  @Test
  @DisplayName("空实例列表返回空 Map")
  void emptyConsumers() {
    Map<Integer, String> assignment = strategy.assign(List.of(0, 1, 2), List.of(), "g1");
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
  @DisplayName("virtualNodes <= 0 抛出 IllegalArgumentException")
  void invalidVirtualNodes() {
    assertThatThrownBy(() -> new ConsistentHashRebalanceStrategy(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("virtualNodes");
    assertThatThrownBy(() -> new ConsistentHashRebalanceStrategy(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("自定义虚拟节点数构造可用")
  void customVirtualNodes() {
    ConsistentHashRebalanceStrategy custom = new ConsistentHashRebalanceStrategy(32);
    Map<Integer, String> assignment = custom.assign(List.of(0, 1, 2), List.of("A", "B"), "g1");
    assertThat(assignment).hasSize(3);
  }

  @Test
  @DisplayName("name 返回 consistent-hash")
  void name() {
    assertThat(strategy.name()).isEqualTo("consistent-hash");
  }
}
