package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * {@link HpaMetricsProvider} 单元测试，验证消费延迟、消费速率与聚合指标查询。
 *
 * <p>默认实现使用内存数据结构，通过 record 方法写入指标值，通过 getter 方法读取。
 */
@DisplayName("HPA 指标提供者测试")
class HpaMetricsProviderTest {

  private HpaMetricsProvider provider;

  @BeforeEach
  void setUp() {
    provider = new HpaMetricsProvider();
  }

  @Nested
  @DisplayName("getConsumerLag - 消费延迟查询")
  class GetConsumerLag {

    @Test
    @DisplayName("未记录时返回 0")
    void returnsZeroWhenNotRecorded() {
      assertThat(provider.getConsumerLag("topic", "group")).isZero();
    }

    @Test
    @DisplayName("记录后返回最新值")
    void returnsLatestValue() {
      provider.recordLag("order-topic", "order-group", 100L);
      assertThat(provider.getConsumerLag("order-topic", "order-group")).isEqualTo(100L);
    }

    @Test
    @DisplayName("多次记录返回最后一次值")
    void returnsLastValueAfterMultipleRecords() {
      provider.recordLag("topic", "group", 50L);
      provider.recordLag("topic", "group", 200L);
      assertThat(provider.getConsumerLag("topic", "group")).isEqualTo(200L);
    }
  }

  @Nested
  @DisplayName("getConsumeRate - 消费速率查询")
  class GetConsumeRate {

    @Test
    @DisplayName("未记录时返回 0.0")
    void returnsZeroWhenNotRecorded() {
      assertThat(provider.getConsumeRate("topic", "group")).isZero();
    }

    @Test
    @DisplayName("记录后返回最新值")
    void returnsLatestValue() {
      provider.recordConsumeRate("order-topic", "order-group", 12.5);
      assertThat(provider.getConsumeRate("order-topic", "order-group")).isEqualTo(12.5);
    }
  }

  @Nested
  @DisplayName("getConsumerMetrics - 聚合指标查询")
  class GetConsumerMetrics {

    @Test
    @DisplayName("无数据时返回空 Map")
    void returnsEmptyMapWhenNoData() {
      Map<String, Double> metrics = provider.getConsumerMetrics();
      assertThat(metrics).isEmpty();
    }

    @Test
    @DisplayName("包含已记录的延迟与速率指标")
    void containsAllRecordedMetrics() {
      provider.recordLag("topic-a", "group-a", 10L);
      provider.recordConsumeRate("topic-a", "group-a", 5.0);
      Map<String, Double> metrics = provider.getConsumerMetrics();
      assertThat(metrics).hasSize(2);
      assertThat(metrics).containsEntry("consumer.lag.topic-a:group-a", 10.0);
      assertThat(metrics).containsEntry("consume.rate.topic-a:group-a", 5.0);
    }
  }
}
